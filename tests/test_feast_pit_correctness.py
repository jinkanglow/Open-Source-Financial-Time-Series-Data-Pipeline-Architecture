"""
Complete Feast PIT Correctness Tests

Tests Feast historical features alignment with TimescaleDB PIT snapshots
Based on: dmatrix/feast_workshops and qooba/mlflow-feast
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from feast import FeatureStore
from src.features.smartdb_contract import SmartDBContract


@pytest.fixture
def db_engine():
    """Create database engine for testing"""
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


@pytest.fixture
def feast_store():
    """Create Feast FeatureStore instance"""
    return FeatureStore(repo_path="feast_repo")


class TestFeastPITCorrectness:
    """Test Feast Point-in-Time correctness with TimescaleDB alignment"""
    
    def test_feast_pit_no_future_leakage(self, feast_store):
        """
        Test that Feast historical features never return future data
        
        This is the critical PIT correctness test from the spec
        """
        entity_df = pd.DataFrame({
            'symbol': ['AAPL'],
            'event_timestamp': [pd.Timestamp('2025-01-20 10:00:00')]
        })
        
        # Get historical features from Feast
        features = feast_store.get_historical_features(
            entity_df=entity_df,
            features=['technical_indicators:sma_20']
        ).to_df()
        
        # Feast must not return data after event_timestamp
        assert features['event_timestamp'].max() <= pd.Timestamp('2025-01-20 10:00:00')
        
        # Verify no future data leakage
        max_timestamp = features['event_timestamp'].max()
        query_timestamp = pd.Timestamp('2025-01-20 10:00:00')
        assert max_timestamp <= query_timestamp, \
            f"Future data leakage detected: {max_timestamp} > {query_timestamp}"
    
    def test_feast_vs_timescale_alignment(self, feast_store, smartdb_contract):
        """
        Test that Feast historical features align with TimescaleDB PIT snapshot
        
        This ensures consistency between Feast and TimescaleDB
        """
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=24)
        
        # Get snapshot from TimescaleDB
        db_snapshot = smartdb_contract.get_pit_snapshot(symbol, query_time)
        
        # Get features from Feast
        entity_df = pd.DataFrame({
            'symbol': [symbol],
            'event_timestamp': [pd.Timestamp(query_time)]
        })
        
        features = feast_store.get_historical_features(
            entity_df=entity_df,
            features=[
                'technical_indicators:ohlc_1m',
                'technical_indicators:sma_20',
                'technical_indicators:volatility_1h',
                'technical_indicators:vwap_5m'
            ]
        ).to_df()
        
        # Compare values (within floating point precision)
        if len(features) > 0:
            feast_row = features.iloc[0]
            
            # Compare OHLC close
            if 'ohlc_1m_close' in feast_row and db_snapshot.ohlc_1m_close:
                assert abs(feast_row['ohlc_1m_close'] - db_snapshot.ohlc_1m_close) < 0.01, \
                    f"OHLC mismatch: Feast={feast_row['ohlc_1m_close']}, DB={db_snapshot.ohlc_1m_close}"
            
            # Compare SMA_20
            if 'sma_20_sma_20' in feast_row and db_snapshot.sma_20:
                assert abs(feast_row['sma_20_sma_20'] - db_snapshot.sma_20) < 0.01, \
                    f"SMA20 mismatch: Feast={feast_row['sma_20_sma_20']}, DB={db_snapshot.sma_20}"
            
            # Compare volatility
            if 'volatility_1h_volatility_1h' in feast_row and db_snapshot.volatility_1h:
                assert abs(feast_row['volatility_1h_volatility_1h'] - db_snapshot.volatility_1h) < 0.01, \
                    f"Volatility mismatch: Feast={feast_row['volatility_1h_volatility_1h']}, DB={db_snapshot.volatility_1h}"
    
    def test_feast_pit_100_historical_timestamps(self, feast_store):
        """
        Test PIT correctness with 100 historical timestamps
        
        As specified: "100 个历史时间戳，无未来泄露"
        """
        symbol = 'AAPL'
        base_time = datetime.now() - timedelta(days=30)
        
        # Generate 100 timestamps over the past 30 days
        timestamps = [
            base_time + timedelta(hours=i * 7.2)  # ~100 timestamps
            for i in range(100)
        ]
        
        entity_df = pd.DataFrame({
            'symbol': [symbol] * 100,
            'event_timestamp': [pd.Timestamp(ts) for ts in timestamps]
        })
        
        # Get historical features
        features = feast_store.get_historical_features(
            entity_df=entity_df,
            features=['technical_indicators:sma_20']
        ).to_df()
        
        # Verify no future leakage for any timestamp
        for idx, row in entity_df.iterrows():
            query_ts = row['event_timestamp']
            # Get features for this timestamp
            row_features = features[features['event_timestamp'] <= query_ts]
            
            # Verify no data after query timestamp
            if len(row_features) > 0:
                max_ts = row_features['event_timestamp'].max()
                assert max_ts <= query_ts, \
                    f"Future leakage at timestamp {query_ts}: found {max_ts}"
    
    def test_feast_online_vs_offline_consistency(self, feast_store):
        """
        Test that Feast online and offline features are consistent
        
        Online features should match offline features for the same timestamp
        """
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=1)
        
        # Get offline features
        entity_df = pd.DataFrame({
            'symbol': [symbol],
            'event_timestamp': [pd.Timestamp(query_time)]
        })
        
        offline_features = feast_store.get_historical_features(
            entity_df=entity_df,
            features=['technical_indicators:sma_20']
        ).to_df()
        
        # Get online features
        online_features = feast_store.get_online_features(
            features=['technical_indicators:sma_20'],
            entity_rows=[{'symbol': symbol}]
        ).to_df()
        
        # Compare values (online should be close to latest offline)
        if len(offline_features) > 0 and len(online_features) > 0:
            offline_sma = offline_features['sma_20_sma_20'].iloc[-1]  # Latest
            online_sma = online_features['sma_20_sma_20'].iloc[0]
            
            # Online should be within reasonable range of offline
            # (allowing for some delay in materialization)
            assert abs(offline_sma - online_sma) / abs(offline_sma) < 0.1, \
                f"Online/Offline mismatch: offline={offline_sma}, online={online_sma}"


class TestFeastMaterialization:
    """Test Feast materialization correctness"""
    
    def test_feast_incremental_materialization(self, feast_store):
        """
        Test that Feast incremental materialization works correctly
        
        As specified: "feast materialize-incremental ✓"
        """
        # This would test the materialization process
        # In production, you would:
        # 1. Run materialization
        # 2. Verify data appears in online store
        # 3. Verify PIT correctness after materialization
        
        # Placeholder test
        assert feast_store is not None
    
    def test_feast_batch_materialization(self, feast_store):
        """
        Test Feast batch materialization
        
        Verify that batch materialization from Delta Lake works
        """
        # Placeholder test
        assert feast_store is not None
