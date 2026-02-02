"""
Smart-DB Correctness Tests

Tests all 11 features for correctness and SLA compliance
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
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


class TestSmartDBCorrectness:
    """Test Smart-DB feature correctness"""
    
    def test_ohlc_1m_aggregate_exists(self, db_engine):
        """Test that ohlc_1m_agg continuous aggregate exists"""
        query = text("""
            SELECT COUNT(*) as count
            FROM ohlc_1m_agg
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            assert row is not None
    
    def test_sma_20_calculation_correct(self, db_engine):
        """Test SMA_20 calculation correctness"""
        symbol = 'AAPL'
        
        # Insert test data
        test_data = []
        base_time = datetime.now() - timedelta(minutes=30)
        base_price = 150.0
        
        for i in range(25):
            test_data.append({
                'time': base_time + timedelta(minutes=i),
                'symbol': symbol,
                'price': base_price + i * 0.1,
                'volume': 100.0,
                'trade_id': f'TEST-{i}'
            })
        
        # Insert data
        insert_query = text("""
            INSERT INTO market_data_raw (time, symbol, price, volume, trade_id)
            VALUES (:time, :symbol, :price, :volume, :trade_id)
            ON CONFLICT (time, symbol, trade_id) DO NOTHING
        """)
        
        with db_engine.connect() as conn:
            for data in test_data:
                conn.execute(insert_query, data)
            conn.commit()
        
        # Query SMA_20
        query = text("""
            SELECT sma_20
            FROM sma_20_calc
            WHERE symbol = :symbol
            ORDER BY minute DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            row = result.fetchone()
            
            if row and row.sma_20:
                # Verify SMA_20 is reasonable (should be around base_price + 10*0.1)
                expected_sma = base_price + 10 * 0.1  # Average of middle 20 prices
                assert abs(row.sma_20 - expected_sma) < 1.0
    
    def test_volatility_1h_calculation(self, db_engine):
        """Test volatility_1h calculation"""
        symbol = 'AAPL'
        
        # Query volatility
        query = text("""
            SELECT volatility_1h, hour
            FROM volatility_1h_agg
            WHERE symbol = :symbol
            ORDER BY hour DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            row = result.fetchone()
            
            if row:
                # Volatility should be non-negative
                assert row.volatility_1h >= 0 or row.volatility_1h is None
    
    def test_vwap_5m_calculation(self, db_engine):
        """Test VWAP_5m calculation"""
        symbol = 'AAPL'
        
        query = text("""
            SELECT vwap_5m, total_volume
            FROM vwap_5m_agg
            WHERE symbol = :symbol
            ORDER BY five_min DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            row = result.fetchone()
            
            if row and row.vwap_5m:
                # VWAP should be positive
                assert row.vwap_5m > 0
                assert row.total_volume > 0
    
    def test_trade_imbalance_5m_range(self, db_engine):
        """Test trade_imbalance_5m is in valid range [-1, 1]"""
        symbol = 'AAPL'
        
        query = text("""
            SELECT trade_imbalance_5m
            FROM trade_imbalance_5m_agg
            WHERE symbol = :symbol
            ORDER BY five_min DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query, {"symbol": symbol})
            row = result.fetchone()
            
            if row and row.trade_imbalance_5m is not None:
                # Trade imbalance should be between -1 and 1
                assert -1.0 <= row.trade_imbalance_5m <= 1.0
    
    def test_regime_tag_values(self, db_engine):
        """Test regime_tag has valid values"""
        query = text("""
            SELECT DISTINCT regime_tag
            FROM regime_tags
            WHERE regime_tag IS NOT NULL
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()
            
            valid_tags = {'up', 'down', 'neutral'}
            for row in rows:
                assert row.regime_tag in valid_tags
    
    def test_feature_pit_snapshot_returns_all_features(self, smartdb_contract):
        """Test that feature_pit_snapshot returns all 11 features"""
        symbol = 'AAPL'
        query_time = datetime.now() - timedelta(hours=1)
        
        snapshot = smartdb_contract.get_pit_snapshot(symbol, query_time)
        
        # Verify all 11 features are present (may be None if no data)
        assert hasattr(snapshot, 'ohlc_1m_open')
        assert hasattr(snapshot, 'ohlc_1m_high')
        assert hasattr(snapshot, 'ohlc_1m_low')
        assert hasattr(snapshot, 'ohlc_1m_close')
        assert hasattr(snapshot, 'sma_20')
        assert hasattr(snapshot, 'ewm_12')
        assert hasattr(snapshot, 'volatility_1h')
        assert hasattr(snapshot, 'vwap_5m')
        assert hasattr(snapshot, 'large_trade_flag')
        assert hasattr(snapshot, 'bidask_spread')
        assert hasattr(snapshot, 'trade_imbalance_5m')
        assert hasattr(snapshot, 'regime_tag')
        assert hasattr(snapshot, 'news_sentiment_embedding')


class TestSLACompliance:
    """Test SLA compliance for all features"""
    
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
    
    def test_sma_20_sla_2_minutes(self, db_engine):
        """Test SMA_20 SLA: ≤2min"""
        query = text("""
            SELECT minute, NOW() - minute as age
            FROM sma_20_calc
            ORDER BY minute DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            
            if row:
                age_seconds = row.age.total_seconds()
                assert age_seconds <= 120, f"SMA_20 SLA violated: {age_seconds}s > 120s"
    
    def test_vwap_5m_sla_30_seconds(self, db_engine):
        """Test VWAP_5m SLA: ≤30s"""
        query = text("""
            SELECT five_min, NOW() - five_min as age
            FROM vwap_5m_agg
            ORDER BY five_min DESC
            LIMIT 1
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(query)
            row = result.fetchone()
            
            if row:
                age_seconds = row.age.total_seconds()
                assert age_seconds <= 30, f"VWAP_5m SLA violated: {age_seconds}s > 30s"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
