"""
Complete Pipeline Demo Script

Demonstrates the complete pipeline workflow with all components
"""

import sys
import time
from datetime import datetime, timedelta
from src.config.settings import config
from src.kafka.market_data_producer import MarketDataProducer
from src.features.smartdb_contract import SmartDBContract
from src.utils.helpers import check_feature_freshness, calculate_sharpe_ratio
from sqlalchemy import create_engine
import pandas as pd


def demo_kafka_producer():
    """Demo: Produce market data to Kafka"""
    print("\n" + "="*80)
    print("Demo 1: Kafka Producer")
    print("="*80)
    
    producer = MarketDataProducer(
        bootstrap_servers=config.kafka.bootstrap_servers,
        schema_registry_url=config.kafka.schema_registry_url,
        topic=config.kafka.topic_market_data
    )
    
    print(f"Producing 50 trades to topic: {config.kafka.topic_market_data}")
    for i in range(50):
        producer.produce_trade(
            symbol='AAPL',
            price=150.0 + i * 0.1,
            volume=100.0 + i * 10,
            trade_id=f'DEMO-{i:04d}',
            side='buy' if i % 2 == 0 else 'sell',
            bid=150.0 + i * 0.1 - 0.05,
            ask=150.0 + i * 0.1 + 0.05,
            source='demo'
        )
    
    producer.flush()
    print("✓ Trades produced successfully")
    time.sleep(2)  # Wait for processing


def demo_smartdb_contract():
    """Demo: Query Smart-DB features"""
    print("\n" + "="*80)
    print("Demo 2: Smart-DB Contract")
    print("="*80)
    
    db_engine = create_engine(config.database.url)
    contract = SmartDBContract(db_engine)
    
    # Get PIT snapshot
    query_time = datetime.now() - timedelta(minutes=5)
    snapshot = contract.get_pit_snapshot('AAPL', query_time)
    
    print(f"PIT Snapshot for AAPL at {query_time}:")
    print(f"  OHLC Close: {snapshot.ohlc_1m_close}")
    print(f"  SMA_20: {snapshot.sma_20}")
    print(f"  EWM_12: {snapshot.ewm_12}")
    print(f"  Volatility_1h: {snapshot.volatility_1h}")
    print(f"  VWAP_5m: {snapshot.vwap_5m}")
    print(f"  Bid-Ask Spread: {snapshot.bidask_spread}")
    print(f"  Regime Tag: {snapshot.regime_tag}")
    
    # Check feature freshness
    print("\nFeature Freshness Check:")
    freshness_status = contract.check_all_features_fresh()
    for feature, is_fresh in freshness_status.items():
        status = "✓" if is_fresh else "✗"
        print(f"  {status} {feature}: {'Fresh' if is_fresh else 'Stale'}")


def demo_feast_integration():
    """Demo: Feast feature store"""
    print("\n" + "="*80)
    print("Demo 3: Feast Feature Store")
    print("="*80)
    
    try:
        from feast import FeatureStore
        
        fs = FeatureStore(repo_path="feast_repo")
        
        # Online features
        print("Fetching online features...")
        online_features = fs.get_online_features(
            features=['technical_indicators:sma_20'],
            entity_rows=[{'symbol': 'AAPL'}]
        ).to_df()
        print(f"✓ Online features: {online_features}")
        
        # Historical features
        print("\nFetching historical features...")
        entity_df = pd.DataFrame({
            'symbol': ['AAPL'],
            'event_timestamp': [datetime.now() - timedelta(hours=1)]
        })
        
        historical_features = fs.get_historical_features(
            entity_df=entity_df,
            features=['technical_indicators:sma_20']
        ).to_df()
        print(f"✓ Historical features retrieved: {len(historical_features)} rows")
        
    except Exception as e:
        print(f"⚠️  Feast demo skipped: {e}")


def demo_feature_freshness_monitoring():
    """Demo: Feature freshness monitoring"""
    print("\n" + "="*80)
    print("Demo 4: Feature Freshness Monitoring")
    print("="*80)
    
    db_engine = create_engine(config.database.url)
    
    features_to_check = {
        'ohlc_1m': config.feature_sla.ohlc_sla_seconds,
        'sma_20': config.feature_sla.sma_20_sla_seconds,
        'vwap_5m': config.feature_sla.vwap_sla_seconds,
    }
    
    print("Checking feature freshness against SLAs:")
    for feature_name, max_age in features_to_check.items():
        status = check_feature_freshness(db_engine, feature_name, max_age)
        if status.get('status') == 'fresh':
            print(f"  ✓ {feature_name}: {status.get('age_seconds', 0):.1f}s (SLA: {max_age}s)")
        elif status.get('status') == 'stale':
            print(f"  ✗ {feature_name}: {status.get('age_seconds', 0):.1f}s (SLA: {max_age}s) - VIOLATED")
        else:
            print(f"  ⚠ {feature_name}: {status.get('error', 'Unknown')}")


def demo_openlineage_tracking():
    """Demo: OpenLineage data lineage tracking"""
    print("\n" + "="*80)
    print("Demo 5: OpenLineage Data Lineage")
    print("="*80)
    
    try:
        from src.observability.openlineage_tracker import OpenLineageTracker
        
        tracker = OpenLineageTracker(marquez_url="http://localhost:5000")
        
        # Track Kafka source
        kafka_run_id = tracker.track_kafka_source(
            topic="market-data",
            schema={"symbol": "string", "price": "double", "volume": "double"}
        )
        print(f"✓ Tracked Kafka source: {kafka_run_id}")
        
        # Track Flink transformation
        flink_run_id = tracker.track_flink_transformation(
            job_name="anomaly_detection",
            input_datasets=["kafka_source_market-data"],
            output_datasets=["timescale_sink_bidask_spreads"]
        )
        print(f"✓ Tracked Flink transformation: {flink_run_id}")
        
        # Complete runs
        tracker.complete_run(kafka_run_id, "kafka_source_market-data")
        tracker.complete_run(flink_run_id, "flink_anomaly_detection")
        print("✓ Lineage tracking complete")
        
    except Exception as e:
        print(f"⚠️  OpenLineage demo skipped: {e}")


def demo_performance_metrics():
    """Demo: Performance metrics calculation"""
    print("\n" + "="*80)
    print("Demo 6: Performance Metrics")
    print("="*80)
    
    # Simulate returns for Sharpe ratio calculation
    import numpy as np
    returns = pd.Series(np.random.normal(0.001, 0.02, 100))  # Daily returns
    
    sharpe = calculate_sharpe_ratio(returns)
    print(f"Sharpe Ratio: {sharpe:.3f}")
    print(f"  (Mean return: {returns.mean():.4f}, Std: {returns.std():.4f})")
    
    if sharpe > 0.5:
        print("✓ Sharpe ratio meets threshold (>0.5)")
    else:
        print("⚠️  Sharpe ratio below threshold")


def main():
    """Run all demos"""
    print("\n" + "="*80)
    print("Financial Timeseries Pipeline - Complete Demo")
    print("="*80)
    print(f"Version: 2.1.0")
    print(f"Started at: {datetime.now()}")
    
    demos = [
        ("Kafka Producer", demo_kafka_producer),
        ("Smart-DB Contract", demo_smartdb_contract),
        ("Feast Integration", demo_feast_integration),
        ("Feature Freshness", demo_feature_freshness_monitoring),
        ("OpenLineage", demo_openlineage_tracking),
        ("Performance Metrics", demo_performance_metrics),
    ]
    
    for name, demo_func in demos:
        try:
            demo_func()
        except Exception as e:
            print(f"\n✗ {name} demo failed: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "="*80)
    print("Demo Complete!")
    print("="*80)
    print("\nNext steps:")
    print("1. View Grafana: http://localhost:3000")
    print("2. View Jaeger: http://localhost:16686")
    print("3. View MLflow: http://localhost:5000")
    print("4. Run tests: pytest tests/ -v")


if __name__ == "__main__":
    main()
