"""
Complete Integration Test

Tests the entire pipeline end-to-end
"""

import pytest
import time
from datetime import datetime, timedelta
from src.kafka.market_data_producer import MarketDataProducer
from src.features.smartdb_contract import SmartDBContract
from feast import FeatureStore
from sqlalchemy import create_engine
import pandas as pd


@pytest.fixture(scope="module")
def pipeline_setup():
    """Setup complete pipeline for integration testing"""
    # Start services (assumes docker-compose is running)
    # Initialize database
    db_engine = create_engine('postgresql://user:password@localhost:5432/financial_db')
    
    # Initialize Feast
    fs = FeatureStore(repo_path="feast_repo")
    
    return {
        "db_engine": db_engine,
        "feast_store": fs,
        "kafka_producer": MarketDataProducer()
    }


class TestEndToEndPipeline:
    """End-to-end integration tests"""
    
    def test_complete_data_flow(self, pipeline_setup):
        """
        Test complete data flow:
        Kafka → Flink → TimescaleDB → Feast → API
        """
        producer = pipeline_setup["kafka_producer"]
        db_engine = pipeline_setup["db_engine"]
        fs = pipeline_setup["feast_store"]
        
        symbol = 'INTEGRATION_TEST'
        num_trades = 100
        
        # Step 1: Produce data to Kafka
        print(f"Producing {num_trades} trades to Kafka...")
        for i in range(num_trades):
            producer.produce_trade(
                symbol=symbol,
                price=150.0 + i * 0.1,
                volume=100.0 + i * 10,
                trade_id=f'INT-{i:04d}',
                side='buy' if i % 2 == 0 else 'sell',
                bid=150.0 + i * 0.1 - 0.05,
                ask=150.0 + i * 0.1 + 0.05,
                source='integration_test'
            )
        producer.flush()
        
        # Step 2: Wait for Flink processing
        print("Waiting for Flink processing...")
        time.sleep(10)
        
        # Step 3: Verify data in TimescaleDB
        print("Verifying data in TimescaleDB...")
        from sqlalchemy import text
        
        count_query = text("""
            SELECT COUNT(*) as count
            FROM market_data_raw
            WHERE symbol = :symbol
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(count_query, {"symbol": symbol})
            count = result.fetchone().count
        
        assert count >= num_trades * 0.9, f"Expected at least {num_trades * 0.9} rows, got {count}"
        
        # Step 4: Verify continuous aggregates
        print("Verifying continuous aggregates...")
        ohlc_query = text("""
            SELECT COUNT(*) as count
            FROM ohlc_1m_agg
            WHERE symbol = :symbol
        """)
        
        with db_engine.connect() as conn:
            result = conn.execute(ohlc_query, {"symbol": symbol})
            ohlc_count = result.fetchone().count
        
        assert ohlc_count > 0, "OHLC aggregates should exist"
        
        # Step 5: Test PIT snapshot
        print("Testing PIT snapshot...")
        contract = SmartDBContract(db_engine)
        query_time = datetime.now() - timedelta(minutes=1)
        snapshot = contract.get_pit_snapshot(symbol, query_time)
        
        assert snapshot.symbol == symbol
        assert snapshot.snapshot_time <= query_time
        
        # Step 6: Test Feast integration
        print("Testing Feast integration...")
        entity_df = pd.DataFrame({
            'symbol': [symbol],
            'event_timestamp': [query_time]
        })
        
        features = fs.get_historical_features(
            entity_df=entity_df,
            features=['technical_indicators:sma_20']
        ).to_df()
        
        assert len(features) > 0, "Feast should return features"
        
        print("✅ End-to-end test passed!")
    
    def test_feature_sla_compliance(self, pipeline_setup):
        """Test that all features meet their SLA"""
        db_engine = pipeline_setup["db_engine"]
        contract = SmartDBContract(db_engine)
        
        freshness_status = contract.check_all_features_fresh()
        
        for feature_name, is_fresh in freshness_status.items():
            assert is_fresh, f"Feature {feature_name} does not meet SLA"
        
        print("✅ All features meet SLA")


if __name__ == "__main__":
    pytest.main([__file__, '-v', '-s'])
