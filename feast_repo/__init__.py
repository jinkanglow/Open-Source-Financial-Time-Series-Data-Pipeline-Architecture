"""
Feast Repository Initialization

This file is used by Feast CLI to discover feature definitions
"""

from feast_repo.feature_definitions import (
    symbol_entity,
    ohlc_1m_feature_view,
    sma_20_feature_view,
    ewm_12_feature_view,
    volatility_1h_feature_view,
    vwap_5m_feature_view,
    large_trade_flag_feature_view,
    bidask_spread_feature_view,
    trade_imbalance_5m_feature_view,
    regime_tag_feature_view,
    news_sentiment_embedding_feature_view,
)

# Export for Feast CLI
entities = [symbol_entity]

feature_views = [
    ohlc_1m_feature_view,
    sma_20_feature_view,
    ewm_12_feature_view,
    volatility_1h_feature_view,
    vwap_5m_feature_view,
    large_trade_flag_feature_view,
    bidask_spread_feature_view,
    trade_imbalance_5m_feature_view,
    regime_tag_feature_view,
    news_sentiment_embedding_feature_view,
]
