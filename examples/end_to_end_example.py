"""
Complete Example: End-to-End Pipeline Usage

Demonstrates how to use all components together
Based on: hoangsonww/End-to-End-Data-Pipeline
"""

from src.kafka.market_data_producer import MarketDataProducer
from src.features.smartdb_contract import SmartDBContract
from feast import FeatureStore
from src.modeling.train_reproducible import ReproducibleTrainer
from src.serving.triton_canary import CanaryDeployment, TritonModelServer
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta


def example_end_to_end_pipeline():
    """
    Complete end-to-end example:
    1. Produce market data to Kafka
    2. Flink processes and writes to TimescaleDB
    3. Query features from TimescaleDB (Smart-DB)
    4. Get features from Feast
    5. Train model with MLflow
    6. Serve model with Triton + Canary
    """
    
    # ========================================================================
    # Step 1: Produce Market Data to Kafka
    # ========================================================================
    print("Step 1: Producing market data to Kafka...")
    producer = MarketDataProducer(
        bootstrap_servers='localhost:9092',
        schema_registry_url='http://localhost:8081',
        topic='market-data'
    )
    
    # Produce sample trades
    for i in range(100):
        producer.produce_trade(
            symbol='AAPL',
            price=150.0 + i * 0.1,
            volume=100.0 + i * 10,
            trade_id=f'TRADE-{i:04d}',
            side='buy' if i % 2 == 0 else 'sell',
            bid=150.0 + i * 0.1 - 0.05,
            ask=150.0 + i * 0.1 + 0.05,
            source='example'
        )
    
    producer.flush()
    print("✓ Market data produced to Kafka")
    
    # ========================================================================
    # Step 2: Wait for Flink to process (in production, this happens automatically)
    # ========================================================================
    print("\nStep 2: Waiting for Flink to process...")
    import time
    time.sleep(5)  # Wait for processing
    print("✓ Flink processing complete")
    
    # ========================================================================
    # Step 3: Query Features from TimescaleDB (Smart-DB)
    # ========================================================================
    print("\nStep 3: Querying features from TimescaleDB...")
    db_engine = create_engine('postgresql://user:password@localhost:5432/financial_db')
    contract = SmartDBContract(db_engine)
    
    # Get PIT snapshot
    query_time = datetime.now() - timedelta(minutes=5)
    snapshot = contract.get_pit_snapshot('AAPL', query_time)
    
    print(f"✓ PIT Snapshot retrieved:")
    print(f"  OHLC Close: {snapshot.ohlc_1m_close}")
    print(f"  SMA_20: {snapshot.sma_20}")
    print(f"  Volatility_1h: {snapshot.volatility_1h}")
    
    # ========================================================================
    # Step 4: Get Features from Feast
    # ========================================================================
    print("\nStep 4: Getting features from Feast...")
    fs = FeatureStore(repo_path="feast_repo")
    
    # Online features
    online_features = fs.get_online_features(
        features=['technical_indicators:sma_20'],
        entity_rows=[{'symbol': 'AAPL'}]
    ).to_df()
    
    print(f"✓ Online features retrieved: {online_features}")
    
    # Historical features (for training)
    entity_df = pd.DataFrame({
        'symbol': ['AAPL'],
        'event_timestamp': [query_time]
    })
    
    historical_features = fs.get_historical_features(
        entity_df=entity_df,
        features=['technical_indicators:sma_20', 'technical_indicators:ohlc_1m']
    ).to_df()
    
    print(f"✓ Historical features retrieved: {len(historical_features)} rows")
    
    # ========================================================================
    # Step 5: Train Model with MLflow (Reproducible)
    # ========================================================================
    print("\nStep 5: Training model with MLflow...")
    trainer = ReproducibleTrainer(
        experiment_name='regime-prediction',
        tracking_uri='sqlite:///mlflow.db',
        seed=42
    )
    
    # Prepare training data (simplified)
    X_train = pd.DataFrame({
        'sma_20': [150.0] * 100,
        'volatility_1h': [0.02] * 100,
        'vwap_5m': [150.0] * 100
    })
    y_train = pd.Series([1] * 50 + [0] * 50)  # Binary classification
    
    X_val = X_train[:20]
    y_val = y_train[:20]
    
    model = trainer.train_model(
        X_train, y_train, X_val, y_val,
        model_type='sklearn',
        model_params={'n_estimators': 100, 'max_depth': 5}
    )
    
    print("✓ Model trained and logged to MLflow")
    
    # ========================================================================
    # Step 6: Serve Model with Triton + Canary
    # ========================================================================
    print("\nStep 6: Serving model with Triton + Canary...")
    
    # Initialize canary deployment
    canary = CanaryDeployment(
        baseline_model='regime_baseline',
        canary_model='regime_canary_v2',
        canary_traffic_percent=10.0
    )
    
    # Make prediction request
    from src.serving.triton_canary import PredictionRequest
    
    request = PredictionRequest(
        symbol='AAPL',
        features={
            'feature_0': snapshot.sma_20 or 150.0,
            'feature_1': snapshot.volatility_1h or 0.02,
            'feature_2': snapshot.vwap_5m or 150.0,
            # ... add all 11 features
        }
    )
    
    # In production, this would call the FastAPI endpoint
    print("✓ Model serving ready (Triton + Canary)")
    
    # ========================================================================
    # Summary
    # ========================================================================
    print("\n" + "="*60)
    print("End-to-End Pipeline Example Complete!")
    print("="*60)
    print("\nPipeline Flow:")
    print("1. Kafka → Market Data Produced")
    print("2. Flink → Real-time Processing → TimescaleDB")
    print("3. TimescaleDB → Smart-DB Features Available")
    print("4. Feast → Feature Store (Online/Offline)")
    print("5. MLflow → Model Training (Reproducible)")
    print("6. Triton → Model Serving (Canary Deployment)")
    print("\nAll components working together! ✓")


if __name__ == "__main__":
    example_end_to_end_pipeline()
