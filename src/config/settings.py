"""
Configuration Management

Loads configuration from environment variables with defaults
"""

import os
from typing import Optional
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database configuration"""
    url: str
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class KafkaConfig:
    """Kafka configuration"""
    bootstrap_servers: str
    schema_registry_url: str
    topic_market_data: str
    topic_dlq: str
    topic_anomalies: str


@dataclass
class RedisConfig:
    """Redis configuration"""
    host: str
    port: int
    password: Optional[str] = None


@dataclass
class S3Config:
    """S3 configuration"""
    access_key_id: str
    secret_access_key: str
    region: str
    bucket_name: str
    endpoint_url: Optional[str] = None


@dataclass
class MLflowConfig:
    """MLflow configuration"""
    tracking_uri: str
    backend_store_uri: str
    default_artifact_root: str


@dataclass
class FeatureSLAConfig:
    """Feature SLA configuration"""
    ohlc_sla_seconds: int = 30
    sma_20_sla_seconds: int = 120
    vwap_sla_seconds: int = 30
    volatility_sla_seconds: int = 120


@dataclass
class ModelConfig:
    """Model configuration"""
    sharpe_threshold: float = 0.5
    pnl_diff_threshold: float = 10.0
    latency_p95_threshold_ms: int = 150


@dataclass
class Config:
    """Main configuration class"""
    database: DatabaseConfig
    kafka: KafkaConfig
    redis: RedisConfig
    s3: S3Config
    mlflow: MLflowConfig
    feature_sla: FeatureSLAConfig
    model: ModelConfig
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables"""
        return cls(
            database=DatabaseConfig(
                url=os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/financial_db'),
                host=os.getenv('TIMESCALEDB_HOST', 'localhost'),
                port=int(os.getenv('TIMESCALEDB_PORT', '5432')),
                user=os.getenv('TIMESCALEDB_USER', 'user'),
                password=os.getenv('TIMESCALEDB_PASSWORD', 'password'),
                database=os.getenv('TIMESCALEDB_DB', 'financial_db')
            ),
            kafka=KafkaConfig(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                schema_registry_url=os.getenv('KAFKA_SCHEMA_REGISTRY_URL', 'http://localhost:8081'),
                topic_market_data=os.getenv('KAFKA_TOPIC_MARKET_DATA', 'market-data'),
                topic_dlq=os.getenv('KAFKA_TOPIC_DLQ', 'market-data-dlq'),
                topic_anomalies=os.getenv('KAFKA_TOPIC_ANOMALIES', 'anomalies')
            ),
            redis=RedisConfig(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', '6379')),
                password=os.getenv('REDIS_PASSWORD')
            ),
            s3=S3Config(
                access_key_id=os.getenv('AWS_ACCESS_KEY_ID', ''),
                secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
                region=os.getenv('AWS_REGION', 'us-east-1'),
                bucket_name=os.getenv('S3_BUCKET_NAME', 'financial-timeseries-feature-store'),
                endpoint_url=os.getenv('S3_ENDPOINT_URL')
            ),
            mlflow=MLflowConfig(
                tracking_uri=os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000'),
                backend_store_uri=os.getenv('MLFLOW_BACKEND_STORE_URI', 'sqlite:///mlflow.db'),
                default_artifact_root=os.getenv('MLFLOW_DEFAULT_ARTIFACT_ROOT', '/mlflow/artifacts')
            ),
            feature_sla=FeatureSLAConfig(
                ohlc_sla_seconds=int(os.getenv('FEATURE_OHLC_SLA_SECONDS', '30')),
                sma_20_sla_seconds=int(os.getenv('FEATURE_SMA_20_SLA_SECONDS', '120')),
                vwap_sla_seconds=int(os.getenv('FEATURE_VWAP_SLA_SECONDS', '30')),
                volatility_sla_seconds=int(os.getenv('FEATURE_VOLATILITY_SLA_SECONDS', '120'))
            ),
            model=ModelConfig(
                sharpe_threshold=float(os.getenv('MODEL_SHARPE_THRESHOLD', '0.5')),
                pnl_diff_threshold=float(os.getenv('MODEL_PNL_DIFF_THRESHOLD', '10.0')),
                latency_p95_threshold_ms=int(os.getenv('MODEL_LATENCY_P95_THRESHOLD_MS', '150'))
            )
        )


# Global configuration instance
config = Config.from_env()
