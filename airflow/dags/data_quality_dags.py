"""
Airflow DAGs for Data Quality and Great Expectations

Integrates Great Expectations with Airflow for automated data quality checks
Addresses: P2.2 Great Expectations + Data Contracts
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from src.quality.data_contracts import MarketDataContract, validate_data_contract, route_to_dlq
import pandas as pd
from sqlalchemy import create_engine


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def validate_market_data_quality(**context):
    """
    Validate market data using Great Expectations
    
    On failure: route to DLQ + send alert
    """
    import os
    from src.quality.data_contracts import MarketDataContract, validate_data_contract
    
    # Get data from TimescaleDB
    db_url = os.getenv('DATABASE_URL', 'postgresql://user:password@timescaledb:5432/financial_db')
    engine = create_engine(db_url)
    
    # Query recent market data
    query = """
        SELECT symbol, price, volume, trade_id, side, bid, ask
        FROM market_data_raw
        WHERE created_at >= NOW() - INTERVAL '1 hour'
        LIMIT 10000
    """
    
    df = pd.read_sql(query, engine)
    
    # Validate against contract
    contract = MarketDataContract()
    result = validate_data_contract(df, contract)
    
    if not result["success"]:
        # Route invalid data to DLQ
        dlq_path = "s3://dlq/invalid-market-data/"
        route_to_dlq(df, contract, dlq_path)
        
        # Send alert
        print(f"Data quality validation failed: {result['failed']} expectations failed")
        raise Exception(f"Data quality check failed: {result['failed']} failures")
    
    print(f"Data quality validation passed: {result['passed']} expectations passed")
    return result


def check_feature_sla(**context):
    """Check feature SLA compliance"""
    import os
    from sqlalchemy import create_engine, text
    
    db_url = os.getenv('DATABASE_URL', 'postgresql://user:password@timescaledb:5432/financial_db')
    engine = create_engine(db_url)
    
    # Check OHLC freshness
    query = text("""
        SELECT 
            'ohlc_1m' as feature,
            EXTRACT(EPOCH FROM (NOW() - MAX(minute))) as age_seconds
        FROM ohlc_1m_agg
        UNION ALL
        SELECT 
            'sma_20' as feature,
            EXTRACT(EPOCH FROM (NOW() - MAX(minute))) as age_seconds
        FROM sma_20_calc
        UNION ALL
        SELECT 
            'vwap_5m' as feature,
            EXTRACT(EPOCH FROM (NOW() - MAX(five_min))) as age_seconds
        FROM vwap_5m_agg
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
        
        sla_violations = []
        for row in rows:
            feature, age_seconds = row
            if feature == 'ohlc_1m' and age_seconds > 30:
                sla_violations.append(f"OHLC_1m SLA violated: {age_seconds}s > 30s")
            elif feature == 'sma_20' and age_seconds > 120:
                sla_violations.append(f"SMA_20 SLA violated: {age_seconds}s > 120s")
            elif feature == 'vwap_5m' and age_seconds > 30:
                sla_violations.append(f"VWAP_5m SLA violated: {age_seconds}s > 30s")
        
        if sla_violations:
            raise Exception(f"SLA violations detected: {', '.join(sla_violations)}")
        
        print("All feature SLAs are compliant")


# Define DAGs
with DAG(
    'data_quality_checks',
    default_args=default_args,
    description='Data quality checks using Great Expectations',
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['data-quality', 'great-expectations'],
) as data_quality_dag:
    
    validate_market_data = PythonOperator(
        task_id='validate_market_data',
        python_callable=validate_market_data_quality,
    )
    
    check_sla = PythonOperator(
        task_id='check_feature_sla',
        python_callable=check_feature_sla,
    )
    
    validate_market_data >> check_sla


with DAG(
    'feature_materialization',
    default_args=default_args,
    description='Materialize features to Feast online store',
    schedule_interval=timedelta(minutes=10),  # Run every 10 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['feast', 'materialization'],
) as materialization_dag:
    
    materialize_features = BashOperator(
        task_id='materialize_features',
        bash_command='feast materialize-incremental {{ ds }}',
        env={
            'FEAST_REPO_PATH': '/opt/feast_repo'
        }
    )
    
    materialize_features


with DAG(
    'spark_batch_features',
    default_args=default_args,
    description='Spark batch job for feature calculation',
    schedule_interval=timedelta(hours=1),  # Run hourly
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['spark', 'batch'],
) as spark_batch_dag:
    
    calculate_sma_20 = BashOperator(
        task_id='calculate_sma_20',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/spark/batch_feature_calculation.py',
    )
    
    calculate_sma_20
