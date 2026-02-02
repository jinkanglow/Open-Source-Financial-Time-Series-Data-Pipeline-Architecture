"""
Utility Functions for Pipeline Operations

Common utilities used across the pipeline
"""

import os
import json
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine


def get_db_connection_string() -> str:
    """Get database connection string from environment"""
    return os.getenv(
        'DATABASE_URL',
        'postgresql://user:password@localhost:5432/financial_db'
    )


def get_kafka_bootstrap_servers() -> str:
    """Get Kafka bootstrap servers from environment"""
    return os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')


def get_s3_bucket_name() -> str:
    """Get S3 bucket name from environment"""
    return os.getenv('S3_BUCKET_NAME', 'financial-timeseries-feature-store')


def validate_environment() -> Dict[str, bool]:
    """
    Validate that all required environment variables are set
    
    Returns:
        Dictionary with validation results
    """
    required_vars = {
        'DATABASE_URL': os.getenv('DATABASE_URL'),
        'KAFKA_BOOTSTRAP_SERVERS': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'S3_BUCKET_NAME': os.getenv('S3_BUCKET_NAME'),
    }
    
    results = {}
    for var_name, var_value in required_vars.items():
        results[var_name] = var_value is not None
    
    return results


def format_timestamp(ts: datetime) -> str:
    """Format timestamp for logging"""
    return ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    """
    Calculate Sharpe ratio
    
    Args:
        returns: Series of returns
        risk_free_rate: Risk-free rate (default 0)
        
    Returns:
        Sharpe ratio
    """
    if len(returns) == 0 or returns.std() == 0:
        return 0.0
    
    excess_returns = returns - risk_free_rate
    sharpe = excess_returns.mean() / excess_returns.std() * (252 ** 0.5)  # Annualized
    return sharpe


def check_feature_freshness(db_engine, feature_name: str, max_age_seconds: int) -> Dict[str, Any]:
    """
    Check feature freshness
    
    Args:
        db_engine: Database engine
        feature_name: Name of the feature
        max_age_seconds: Maximum age in seconds
        
    Returns:
        Freshness status dictionary
    """
    from sqlalchemy import text
    
    queries = {
        'ohlc_1m': "SELECT MAX(minute) as last_update FROM ohlc_1m_agg",
        'sma_20': "SELECT MAX(minute) as last_update FROM sma_20_calc",
        'vwap_5m': "SELECT MAX(five_min) as last_update FROM vwap_5m_agg",
    }
    
    query = queries.get(feature_name)
    if not query:
        return {"status": "unknown", "error": f"Unknown feature: {feature_name}"}
    
    with db_engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()
        
        if row and row.last_update:
            age_seconds = (datetime.now() - row.last_update).total_seconds()
            is_fresh = age_seconds <= max_age_seconds
            
            return {
                "status": "fresh" if is_fresh else "stale",
                "age_seconds": age_seconds,
                "max_age_seconds": max_age_seconds,
                "last_update": row.last_update.isoformat(),
                "violated": not is_fresh
            }
        else:
            return {
                "status": "no_data",
                "error": "No data found"
            }


def export_feature_snapshot_to_json(snapshot, output_path: str):
    """Export feature snapshot to JSON file"""
    snapshot_dict = {
        "symbol": snapshot.symbol,
        "snapshot_time": snapshot.snapshot_time.isoformat(),
        "features": {
            "ohlc_1m_open": snapshot.ohlc_1m_open,
            "ohlc_1m_high": snapshot.ohlc_1m_high,
            "ohlc_1m_low": snapshot.ohlc_1m_low,
            "ohlc_1m_close": snapshot.ohlc_1m_close,
            "sma_20": snapshot.sma_20,
            "ewm_12": snapshot.ewm_12,
            "volatility_1h": snapshot.volatility_1h,
            "vwap_5m": snapshot.vwap_5m,
            "large_trade_flag": snapshot.large_trade_flag,
            "bidask_spread": snapshot.bidask_spread,
            "trade_imbalance_5m": snapshot.trade_imbalance_5m,
            "regime_tag": snapshot.regime_tag,
        }
    }
    
    with open(output_path, 'w') as f:
        json.dump(snapshot_dict, f, indent=2, default=str)
    
    print(f"Feature snapshot exported to {output_path}")


if __name__ == "__main__":
    # Example usage
    print("Environment validation:")
    env_status = validate_environment()
    for var, is_set in env_status.items():
        status = "✓" if is_set else "✗"
        print(f"  {status} {var}")
