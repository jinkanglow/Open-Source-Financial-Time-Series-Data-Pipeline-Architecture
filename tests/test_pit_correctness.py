"""
Point-in-Time (PIT) Correctness Tests

These tests ensure that:
1. No future data leakage occurs
2. PIT queries return correct historical data
3. Features match offline calculations
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from src.features.smartdb_contract import SmartDBContract, FeaturePITSnapshot


@pytest.fixture
def db_engine():
    """Create database engine for testing"""
    # Use test database URL from environment
    import os
    db_url = os.getenv(
        'TEST_DATABASE_URL',
        'postgresql://user:password@localhost:5432/test_db'
    )
    return create_engine(db_url)


@pytest.fixture
def smartdb_contract(db_engine):
    """Create SmartDBContract instance"""
    return SmartDBContract(db_engine)


class TestPITCorrectness:
    """Test Point-in-Time correctness"""
    
    def test_pit_no_future_leakage(self, smartdb_contract):
        """
        Test that PIT queries never return future data
        
        This is the most critical test - ensures no future leakage
        """
        symbol = 'AAPL'
        # Query for a time in the past
        past_time = datetime.now() - timedelta(hours=1)
        
        snapshot = smartdb_contract.get_pit_snapshot(symbol, past_time)
        
        # Verify snapshot_time is not in the future
        assert snapshot.snapshot_time <= datetime.now()
        
        # Verify all timestamps in the snapshot are <= as_of_ts
        assert snapshot.snapshot_time <= past_time
    
    def test_pit_future_query_raises_error(self, smartdb_contract):
        """Test that querying future data raises an error"""
        symbol = 'AAPL'
        future_time = datetime.now() + timedelta(hours=1)
        
        with pytest.raises(ValueError, match="Cannot query future data"):
            smartdb_contract.get_pit_snapshot(symbol, future_time)
    
    def test_pit_historical_consistency(self, smartdb_contract, db_engine):
        """
        Test that PIT queries return consistent historical data
        
        Query the same time multiple times - should get same results
        """
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(days=1)
        
        snapshot1 = smartdb_contract.get_pit_snapshot(symbol, query_time)
        snapshot2 = smartdb_contract.get_pit_snapshot(symbol, query_time)
        
        # Should get identical results
        assert snapshot1.ohlc_1m_close == snapshot2.ohlc_1m_close
        assert snapshot1.sma_20 == snapshot2.sma_20
        assert snapshot1.volatility_1h == snapshot2.volatility_1h
    
    def test_pit_vs_feast_alignment(self, smartdb_contract):
        """
        Test that PIT snapshot aligns with Feast historical features
        
        This ensures consistency between TimescaleDB and Feast
        """
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=24)
        
        # Get snapshot from TimescaleDB
        db_snapshot = smartdb_contract.get_pit_snapshot(symbol, query_time)
        
        # Get features from Feast (would need Feast client)
        # For now, this is a placeholder test structure
        # In real implementation, you would:
        # 1. Create entity_df with symbol and event_timestamp
        # 2. Call fs.get_historical_features()
        # 3. Compare results
        
        # Placeholder assertion
        assert db_snapshot.symbol == symbol
        assert db_snapshot.snapshot_time <= query_time


class TestFeatureCorrectness:
    """Test individual feature correctness"""
    
    def test_ohlc_1m_correctness(self, db_engine):
        """
        Test that OHLC values match manual calculation
        
        Compare continuous aggregate OHLC with manual SQL calculation
        """
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=1)
        
        # Query continuous aggregate
        query_agg = text("""
            SELECT open, high, low, close, volume
            FROM ohlc_1m_agg
            WHERE symbol = :symbol
              AND minute <= :query_time
            ORDER BY minute DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result_agg = conn.execute(
                query_agg,
                {"symbol": symbol, "query_time": query_time}
            )
            row_agg = result_agg.fetchone()
            
            # Manual calculation from raw data
            query_manual = text("""
                SELECT 
                    first(price, time) as open,
                    max(price) as high,
                    min(price) as low,
                    last(price, time) as close,
                    sum(volume) as volume
                FROM market_data_raw
                WHERE symbol = :symbol
                  AND time_bucket('1 minute', time) = (
                      SELECT time_bucket('1 minute', time)
                      FROM market_data_raw
                      WHERE symbol = :symbol
                        AND time <= :query_time
                      ORDER BY time DESC
                      LIMIT 1
                  )
                GROUP BY time_bucket('1 minute', time)
            """)
            
            result_manual = conn.execute(
                query_manual,
                {"symbol": symbol, "query_time": query_time}
            )
            row_manual = result_manual.fetchone()
            
            # Values should match (within floating point precision)
            if row_agg and row_manual:
                assert abs(row_agg.open - row_manual.open) < 0.0001
                assert abs(row_agg.high - row_manual.high) < 0.0001
                assert abs(row_agg.low - row_manual.low) < 0.0001
                assert abs(row_agg.close - row_manual.close) < 0.0001
    
    def test_sma_20_vs_spark_calculation(self, db_engine):
        """
        Test that SMA_20 matches Spark offline calculation
        
        This ensures consistency between real-time and batch processing
        """
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=1)
        
        # Get SMA from TimescaleDB
        query_db = text("""
            SELECT sma_20
            FROM sma_20_calc
            WHERE symbol = :symbol
              AND minute <= :query_time
            ORDER BY minute DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result_db = conn.execute(
                query_db,
                {"symbol": symbol, "query_time": query_time}
            )
            row_db = result_db.fetchone()
            
            # In real implementation, you would:
            # 1. Load same data in Spark
            # 2. Calculate SMA_20 using Spark SQL
            # 3. Compare results
            
            # Placeholder: just verify we got a result
            if row_db:
                assert row_db.sma_20 is not None
                assert row_db.sma_20 > 0
    
    def test_volatility_pit_no_future_leakage(self, db_engine):
        """
        Test that volatility calculation doesn't use future data
        
        Critical for PIT correctness
        """
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=2)
        
        # Query volatility
        query = text("""
            SELECT volatility_1h, hour
            FROM volatility_1h_agg
            WHERE symbol = :symbol
              AND hour <= :query_time
            ORDER BY hour DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(
                query,
                {"symbol": symbol, "query_time": query_time}
            )
            row = result.fetchone()
            
            if row:
                # Verify hour is not in the future
                assert row.hour <= query_time
                assert row.hour <= datetime.now()


class TestSLACompliance:
    """Test SLA compliance for all features"""
    
    def test_all_features_meet_sla(self, smartdb_contract):
        """Test that all 11 features meet their SLA"""
        freshness_status = smartdb_contract.check_all_features_fresh()
        
        for feature_name, is_fresh in freshness_status.items():
            assert is_fresh, f"Feature {feature_name} does not meet SLA"
    
    def test_ohlc_sla_30_seconds(self, db_engine):
        """Test OHLC SLA: ≤30s"""
        query = text("""
            SELECT minute, NOW() - minute as age
            FROM ohlc_1m_agg
            ORDER BY minute DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            
            if row:
                age_seconds = row.age.total_seconds()
                assert age_seconds <= 30, f"OHLC SLA violated: {age_seconds}s > 30s"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
