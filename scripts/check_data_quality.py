"""
Data Quality Checker

Validates data quality across the pipeline
"""

import sys
from datetime import datetime, timedelta
from src.config.settings import config
from src.quality.data_contracts import MarketDataContract
from src.features.smartdb_contract import SmartDBContract
from src.utils.helpers import check_feature_freshness
from sqlalchemy import create_engine
import pandas as pd


class DataQualityChecker:
    """Check data quality across the pipeline"""
    
    def __init__(self):
        self.db_engine = create_engine(config.database.url)
        self.contract = SmartDBContract(self.db_engine)
        self.errors = []
        self.warnings = []
    
    def check_raw_data_quality(self):
        """Check raw market data quality"""
        print("\n" + "="*80)
        print("Checking Raw Data Quality")
        print("="*80)
        
        from sqlalchemy import text
        
        # Check for null values
        query = text("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(*) FILTER (WHERE symbol IS NULL) as null_symbols,
                COUNT(*) FILTER (WHERE price IS NULL OR price <= 0) as invalid_prices,
                COUNT(*) FILTER (WHERE volume IS NULL OR volume < 0) as invalid_volumes,
                COUNT(*) FILTER (WHERE time IS NULL) as null_timestamps
            FROM market_data_raw
            WHERE time > NOW() - INTERVAL '1 hour'
        """)
        
        with self.db_engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            
            print(f"Total rows (last hour): {row.total_rows}")
            print(f"Null symbols: {row.null_symbols}")
            print(f"Invalid prices: {row.invalid_prices}")
            print(f"Invalid volumes: {row.invalid_volumes}")
            print(f"Null timestamps: {row.null_timestamps}")
            
            if row.null_symbols > 0:
                self.errors.append(f"Found {row.null_symbols} rows with null symbols")
            if row.invalid_prices > 0:
                self.errors.append(f"Found {row.invalid_prices} rows with invalid prices")
            if row.invalid_volumes > 0:
                self.errors.append(f"Found {row.invalid_volumes} rows with invalid volumes")
    
    def check_feature_quality(self):
        """Check feature calculation quality"""
        print("\n" + "="*80)
        print("Checking Feature Quality")
        print("="*80)
        
        # Check OHLC aggregate
        from sqlalchemy import text
        
        query = text("""
            SELECT 
                COUNT(*) as total_buckets,
                COUNT(*) FILTER (WHERE open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL) as null_ohlc,
                COUNT(*) FILTER (WHERE high < low) as invalid_ohlc,
                COUNT(*) FILTER (WHERE volume < 0) as invalid_volume
            FROM ohlc_1m_agg
            WHERE minute > NOW() - INTERVAL '1 hour'
        """)
        
        with self.db_engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            
            print(f"Total OHLC buckets (last hour): {row.total_buckets}")
            print(f"Null OHLC values: {row.null_ohlc}")
            print(f"Invalid OHLC (high < low): {row.invalid_ohlc}")
            print(f"Invalid volumes: {row.invalid_volume}")
            
            if row.null_ohlc > 0:
                self.warnings.append(f"Found {row.null_ohlc} OHLC buckets with null values")
            if row.invalid_ohlc > 0:
                self.errors.append(f"Found {row.invalid_ohlc} OHLC buckets with invalid values (high < low)")
    
    def check_feature_freshness(self):
        """Check feature freshness against SLAs"""
        print("\n" + "="*80)
        print("Checking Feature Freshness")
        print("="*80)
        
        features_to_check = {
            'ohlc_1m': config.feature_sla.ohlc_sla_seconds,
            'sma_20': config.feature_sla.sma_20_sla_seconds,
            'vwap_5m': config.feature_sla.vwap_sla_seconds,
        }
        
        for feature_name, max_age in features_to_check.items():
            status = check_feature_freshness(self.db_engine, feature_name, max_age)
            
            if status.get('status') == 'fresh':
                print(f"✓ {feature_name}: {status.get('age_seconds', 0):.1f}s (SLA: {max_age}s)")
            elif status.get('status') == 'stale':
                age = status.get('age_seconds', 0)
                print(f"✗ {feature_name}: {age:.1f}s (SLA: {max_age}s) - VIOLATED")
                self.errors.append(f"Feature {feature_name} SLA violated: {age:.1f}s > {max_age}s")
            else:
                print(f"⚠ {feature_name}: {status.get('error', 'Unknown')}")
                self.warnings.append(f"Could not check freshness for {feature_name}")
    
    def check_data_consistency(self):
        """Check data consistency between raw and aggregates"""
        print("\n" + "="*80)
        print("Checking Data Consistency")
        print("="*80)
        
        from sqlalchemy import text
        
        # Check if raw data exists for recent aggregates
        query = text("""
            SELECT 
                COUNT(DISTINCT symbol) as symbols_in_raw,
                COUNT(DISTINCT symbol) FILTER (
                    WHERE symbol IN (SELECT DISTINCT symbol FROM ohlc_1m_agg WHERE minute > NOW() - INTERVAL '1 hour')
                ) as symbols_in_aggregates
            FROM market_data_raw
            WHERE time > NOW() - INTERVAL '1 hour'
        """)
        
        with self.db_engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            
            print(f"Symbols in raw data (last hour): {row.symbols_in_raw}")
            print(f"Symbols in aggregates (last hour): {row.symbols_in_aggregates}")
            
            if row.symbols_in_raw > row.symbols_in_aggregates:
                self.warnings.append(
                    f"Some symbols in raw data don't have aggregates: "
                    f"{row.symbols_in_raw} vs {row.symbols_in_aggregates}"
                )
    
    def check_duplicates(self):
        """Check for duplicate records"""
        print("\n" + "="*80)
        print("Checking for Duplicates")
        print("="*80)
        
        from sqlalchemy import text
        
        # Check for duplicate trades
        query = text("""
            SELECT 
                trade_id,
                COUNT(*) as count
            FROM market_data_raw
            WHERE time > NOW() - INTERVAL '1 hour'
            GROUP BY trade_id
            HAVING COUNT(*) > 1
            LIMIT 10
        """)
        
        with self.db_engine.connect() as conn:
            result = conn.execute(query)
            duplicates = result.fetchall()
            
            if duplicates:
                print(f"Found {len(duplicates)} duplicate trade_ids:")
                for dup in duplicates[:5]:
                    print(f"  - {dup.trade_id}: {dup.count} occurrences")
                self.errors.append(f"Found {len(duplicates)} duplicate trade_ids")
            else:
                print("✓ No duplicates found")
    
    def run_all_checks(self):
        """Run all quality checks"""
        print("\n" + "="*80)
        print("Data Quality Check - Financial Timeseries Pipeline")
        print("="*80)
        print(f"Started at: {datetime.now()}")
        
        self.check_raw_data_quality()
        self.check_feature_quality()
        self.check_feature_freshness()
        self.check_data_consistency()
        self.check_duplicates()
        
        # Print summary
        print("\n" + "="*80)
        print("Quality Check Summary")
        print("="*80)
        
        if self.errors:
            print(f"\n✗ Errors ({len(self.errors)}):")
            for error in self.errors:
                print(f"  - {error}")
        
        if self.warnings:
            print(f"\n⚠ Warnings ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  - {warning}")
        
        if not self.errors and not self.warnings:
            print("\n✓ All quality checks passed!")
            return True
        elif not self.errors:
            print("\n✓ Quality checks passed with warnings")
            return True
        else:
            print("\n✗ Quality checks failed")
            return False


def main():
    """Main entry point"""
    checker = DataQualityChecker()
    success = checker.run_all_checks()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
