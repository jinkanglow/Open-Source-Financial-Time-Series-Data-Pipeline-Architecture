"""
Great Expectations Integration with TimescaleDB

Actual implementation using Great Expectations library
Based on: https://github.com/great-expectations/great_expectations
"""

import pandas as pd
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text
from great_expectations.dataset import PandasDataset
from great_expectations.core import ExpectationSuite
from datetime import datetime
from src.config.settings import config


class GreatExpectationsValidator:
    """Great Expectations validator for TimescaleDB data"""
    
    def __init__(self, db_engine=None):
        """
        Initialize validator
        
        Args:
            db_engine: SQLAlchemy engine (optional)
        """
        self.db_engine = db_engine or create_engine(config.database.url)
    
    def validate_ohlc_1m(self, limit: int = 1000) -> Dict:
        """
        Validate OHLC_1m aggregate using Great Expectations
        
        Args:
            limit: Number of rows to validate
            
        Returns:
            Validation result dictionary
        """
        # Load data from TimescaleDB
        query = text(f"""
            SELECT minute, symbol, open, high, low, close, volume
            FROM ohlc_1m_agg
            WHERE minute > NOW() - INTERVAL '1 hour'
            ORDER BY minute DESC
            LIMIT {limit}
        """)
        
        df = pd.read_sql(query, self.db_engine)
        
        if len(df) == 0:
            return {
                "success": False,
                "error": "No data found"
            }
        
        # Convert to Great Expectations dataset
        ge_df = PandasDataset(df)
        
        # Define expectations
        results = {}
        
        # Expect open > 0
        try:
            result = ge_df.expect_column_values_to_be_between(
                column="open",
                min_value=0.01,
                max_value=999999
            )
            results["open_positive"] = result.success
        except Exception as e:
            results["open_positive"] = False
            results["open_positive_error"] = str(e)
        
        # Expect volume >= 0
        try:
            result = ge_df.expect_column_values_to_be_nonnegative(column="volume")
            results["volume_nonnegative"] = result.success
        except Exception as e:
            results["volume_nonnegative"] = False
            results["volume_nonnegative_error"] = str(e)
        
        # Expect high >= low
        try:
            result = ge_df.expect_column_pair_values_A_to_be_greater_than_B(
                column_A="high",
                column_B="low"
            )
            results["high_greater_than_low"] = result.success
        except Exception as e:
            results["high_greater_than_low"] = False
            results["high_greater_than_low_error"] = str(e)
        
        # Expect no nulls in close
        try:
            result = ge_df.expect_column_values_to_not_be_null(column="close")
            results["close_not_null"] = result.success
        except Exception as e:
            results["close_not_null"] = False
            results["close_not_null_error"] = str(e)
        
        # Expect timestamp ordered
        try:
            result = ge_df.expect_column_values_to_be_increasing(column="minute")
            results["timestamp_ordered"] = result.success
        except Exception as e:
            results["timestamp_ordered"] = False
            results["timestamp_ordered_error"] = str(e)
        
        success = all([
            results.get("open_positive", False),
            results.get("volume_nonnegative", False),
            results.get("high_greater_than_low", False),
            results.get("close_not_null", False),
            results.get("timestamp_ordered", False)
        ])
        
        return {
            "success": success,
            "results": results,
            "rows_validated": len(df),
            "timestamp": datetime.now().isoformat()
        }
    
    def validate_sma_20(self, limit: int = 1000) -> Dict:
        """
        Validate SMA_20 calculation
        
        Args:
            limit: Number of rows to validate
            
        Returns:
            Validation result dictionary
        """
        query = text(f"""
            SELECT minute, symbol, price, sma_20
            FROM sma_20_calc
            WHERE minute > NOW() - INTERVAL '1 hour'
            ORDER BY minute DESC
            LIMIT {limit}
        """)
        
        df = pd.read_sql(query, self.db_engine)
        
        if len(df) == 0:
            return {
                "success": False,
                "error": "No data found"
            }
        
        ge_df = PandasDataset(df)
        results = {}
        
        # Expect SMA not null
        try:
            result = ge_df.expect_column_values_to_not_be_null(column="sma_20")
            results["sma_not_null"] = result.success
        except Exception as e:
            results["sma_not_null"] = False
            results["sma_not_null_error"] = str(e)
        
        # Expect SMA in reasonable range (80% to 120% of price)
        try:
            df_with_range = df.copy()
            df_with_range['sma_min'] = df_with_range['price'] * 0.8
            df_with_range['sma_max'] = df_with_range['price'] * 1.2
            
            ge_df_range = PandasDataset(df_with_range)
            result = ge_df_range.expect_column_values_to_be_between(
                column="sma_20",
                min_value=df_with_range['sma_min'].min(),
                max_value=df_with_range['sma_max'].max()
            )
            results["sma_in_range"] = result.success
        except Exception as e:
            results["sma_in_range"] = False
            results["sma_in_range_error"] = str(e)
        
        success = all([
            results.get("sma_not_null", False),
            results.get("sma_in_range", False)
        ])
        
        return {
            "success": success,
            "results": results,
            "rows_validated": len(df),
            "timestamp": datetime.now().isoformat()
        }
    
    def validate_volatility_1h(self, limit: int = 1000) -> Dict:
        """
        Validate volatility_1h calculation
        
        Args:
            limit: Number of rows to validate
            
        Returns:
            Validation result dictionary
        """
        query = text(f"""
            SELECT hour, symbol, volatility_1h
            FROM volatility_1h_agg
            WHERE hour > NOW() - INTERVAL '24 hours'
            ORDER BY hour DESC
            LIMIT {limit}
        """)
        
        df = pd.read_sql(query, self.db_engine)
        
        if len(df) == 0:
            return {
                "success": False,
                "error": "No data found"
            }
        
        ge_df = PandasDataset(df)
        results = {}
        
        # Expect volatility >= 0
        try:
            result = ge_df.expect_column_values_to_be_nonnegative(column="volatility_1h")
            results["volatility_nonnegative"] = result.success
        except Exception as e:
            results["volatility_nonnegative"] = False
            results["volatility_nonnegative_error"] = str(e)
        
        # Expect volatility < 5 (financial sanity check)
        try:
            result = ge_df.expect_column_values_to_be_between(
                column="volatility_1h",
                min_value=0,
                max_value=5.0
            )
            results["volatility_reasonable"] = result.success
        except Exception as e:
            results["volatility_reasonable"] = False
            results["volatility_reasonable_error"] = str(e)
        
        success = all([
            results.get("volatility_nonnegative", False),
            results.get("volatility_reasonable", False)
        ])
        
        return {
            "success": success,
            "results": results,
            "rows_validated": len(df),
            "timestamp": datetime.now().isoformat()
        }
    
    def validate_all_features(self) -> Dict:
        """
        Validate all features
        
        Returns:
            Combined validation results
        """
        results = {
            "ohlc_1m": self.validate_ohlc_1m(),
            "sma_20": self.validate_sma_20(),
            "volatility_1h": self.validate_volatility_1h(),
            "timestamp": datetime.now().isoformat()
        }
        
        all_success = all([r.get("success", False) for r in results.values() if isinstance(r, dict)])
        
        return {
            "success": all_success,
            "feature_results": results,
            "overall_success": all_success
        }


# Example usage
if __name__ == "__main__":
    validator = GreatExpectationsValidator()
    
    print("Validating OHLC_1m...")
    ohlc_result = validator.validate_ohlc_1m()
    print(f"OHLC Validation: {'✓' if ohlc_result['success'] else '✗'}")
    
    print("\nValidating SMA_20...")
    sma_result = validator.validate_sma_20()
    print(f"SMA Validation: {'✓' if sma_result['success'] else '✗'}")
    
    print("\nValidating Volatility_1h...")
    vol_result = validator.validate_volatility_1h()
    print(f"Volatility Validation: {'✓' if vol_result['success'] else '✗'}")
    
    print("\nValidating all features...")
    all_results = validator.validate_all_features()
    print(f"Overall Validation: {'✓' if all_results['overall_success'] else '✗'}")
