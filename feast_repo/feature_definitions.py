"""
Feast Feature Store Integration

Defines FeatureViews for all 11 core features with offline/online stores
"""

from datetime import timedelta
from feast import Entity, FeatureView, ValueType, Field
from feast.types import Float32, Float64, Int64, String, Bool, Array
from feast.data_source import KafkaSource, DeltaSource
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkOfflineStoreConfig
)
from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.repo_config import RepoConfig


# Define Entity
symbol_entity = Entity(
    name="symbol",
    value_type=ValueType.STRING,
    description="Stock symbol identifier",
)

# Define time entity for point-in-time queries
timestamp_entity = Entity(
    name="event_timestamp",
    value_type=ValueType.UNIX_TIMESTAMP,
    description="Event timestamp for point-in-time queries",
)


# 1. OHLC 1-minute FeatureView
ohlc_1m_feature_view = FeatureView(
    name="ohlc_1m",
    entities=[symbol_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="open", dtype=Float64),
        Field(name="high", dtype=Float64),
        Field(name="low", dtype=Float64),
        Field(name="close", dtype=Float64),
        Field(name="volume", dtype=Float64),
    ],
    source=DeltaSource(
        path="s3://feature-store/ohlc_1m",
        timestamp_field="minute",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "30s"},
)

# 2. SMA 20 FeatureView
sma_20_feature_view = FeatureView(
    name="sma_20",
    entities=[symbol_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="sma_20", dtype=Float64),
    ],
    source=DeltaSource(
        path="s3://feature-store/sma_20",
        timestamp_field="minute",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "2min"},
)

# 3. EWM 12 FeatureView
ewm_12_feature_view = FeatureView(
    name="ewm_12",
    entities=[symbol_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="ewm_12", dtype=Float64),
    ],
    source=DeltaSource(
        path="s3://feature-store/ewm_12",
        timestamp_field="minute",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "2min"},
)

# 4. Volatility 1-hour FeatureView
volatility_1h_feature_view = FeatureView(
    name="volatility_1h",
    entities=[symbol_entity],
    ttl=timedelta(days=7),
    schema=[
        Field(name="volatility_1h", dtype=Float64),
    ],
    source=DeltaSource(
        path="s3://feature-store/volatility_1h",
        timestamp_field="hour",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "2min"},
)

# 5. VWAP 5-minute FeatureView
vwap_5m_feature_view = FeatureView(
    name="vwap_5m",
    entities=[symbol_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="vwap_5m", dtype=Float64),
    ],
    source=DeltaSource(
        path="s3://feature-store/vwap_5m",
        timestamp_field="five_min",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "30s"},
)

# 6. Large Trade Flag FeatureView
large_trade_flag_feature_view = FeatureView(
    name="large_trade_flag",
    entities=[symbol_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="large_trade_flag", dtype=Bool),
    ],
    source=KafkaSource(
        kafka_bootstrap_servers="localhost:9092",
        topic="large-trades",
        message_format="avro",
        timestamp_field="time",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "30s"},
)

# 7. Bid-Ask Spread FeatureView
bidask_spread_feature_view = FeatureView(
    name="bidask_spread",
    entities=[symbol_entity],
    ttl=timedelta(hours=1),
    schema=[
        Field(name="bidask_spread", dtype=Float64),
    ],
    source=KafkaSource(
        kafka_bootstrap_servers="localhost:9092",
        topic="bidask-spreads",
        message_format="avro",
        timestamp_field="time",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "1s"},
)

# 8. Trade Imbalance 5-minute FeatureView
trade_imbalance_5m_feature_view = FeatureView(
    name="trade_imbalance_5m",
    entities=[symbol_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="trade_imbalance_5m", dtype=Float64),
    ],
    source=DeltaSource(
        path="s3://feature-store/trade_imbalance_5m",
        timestamp_field="five_min",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "30s"},
)

# 9. Regime Tag FeatureView
regime_tag_feature_view = FeatureView(
    name="regime_tag",
    entities=[symbol_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="regime_tag", dtype=String),
    ],
    source=DeltaSource(
        path="s3://feature-store/regime_tags",
        timestamp_field="time",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "1min"},
)

# 10. News Sentiment Embedding FeatureView
news_sentiment_embedding_feature_view = FeatureView(
    name="news_sentiment_embedding",
    entities=[symbol_entity],
    ttl=timedelta(days=7),
    schema=[
        Field(name="news_sentiment_embedding", dtype=Array(Float32)),
        Field(name="sentiment_score", dtype=Float64),
    ],
    source=DeltaSource(
        path="s3://feature-store/news_sentiment",
        timestamp_field="time",
        created_timestamp_column="created_at",
    ),
    online=True,
    tags={"team": "trading", "sla": "5min"},
)

# Feature Service for all technical indicators
technical_indicators_service = [
    ohlc_1m_feature_view,
    sma_20_feature_view,
    ewm_12_feature_view,
    volatility_1h_feature_view,
    vwap_5m_feature_view,
    large_trade_flag_feature_view,
    bidask_spread_feature_view,
    trade_imbalance_5m_feature_view,
    regime_tag_feature_view,
]

# Feature Service for ML features (includes sentiment)
ml_features_service = technical_indicators_service + [
    news_sentiment_embedding_feature_view,
]
