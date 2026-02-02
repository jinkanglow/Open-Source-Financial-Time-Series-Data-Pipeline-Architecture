# Feast Repository README

This directory contains the Feast feature store configuration.

## Structure

```
feast_repo/
├── feature_store.yaml          # Feast repository configuration
├── feature_definitions.py       # Python feature definitions (11 FeatureViews)
└── .feast/                     # Feast metadata (auto-generated)
```

## Quick Start

### 1. Initialize Feast Repository

```bash
cd feast_repo
feast init
```

### 2. Apply Feature Definitions

```bash
feast apply
```

This will:
- Register all 11 FeatureViews
- Create entities
- Set up online store (Redis)
- Set up offline store (Delta Lake)

### 3. Materialize Features

```bash
# Materialize to online store
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

### 4. Use Features

```python
from feast import FeatureStore

fs = FeatureStore(repo_path="feast_repo")

# Online features
features = fs.get_online_features(
    features=['technical_indicators:sma_20'],
    entity_rows=[{'symbol': 'AAPL'}]
).to_df()

# Historical features (for training)
entity_df = pd.DataFrame({
    'symbol': ['AAPL'],
    'event_timestamp': [pd.Timestamp('2025-01-20 10:00:00')]
})

historical_features = fs.get_historical_features(
    entity_df=entity_df,
    features=['technical_indicators:sma_20']
).to_df()
```

## Feature Views

All 11 core features are defined:

1. `ohlc_1m` - 1-minute OHLC (SLA: ≤30s)
2. `sma_20` - 20-period SMA (SLA: ≤2min)
3. `ewm_12` - 12-period EWM (SLA: ≤2min)
4. `volatility_1h` - 1-hour volatility (SLA: ≤2min)
5. `vwap_5m` - 5-minute VWAP (SLA: ≤30s)
6. `large_trade_flag` - Large trade flag (SLA: ≤30s)
7. `bidask_spread` - Bid-ask spread (SLA: ≤1s)
8. `trade_imbalance_5m` - Trade imbalance (SLA: ≤30s)
9. `regime_tag` - Regime tag (SLA: ≤1min)
10. `news_sentiment_embedding` - News sentiment (SLA: 5min async)
11. `feature_pit_snapshot()` - PIT snapshot (SLA: instant)

## Integration with MLflow

See `src/modeling/train_reproducible.py` for MLflow integration examples.

## References

- [Feast Documentation](https://docs.feast.dev/)
- [dmatrix/feast_workshops](https://github.com/dmatrix/feast_workshops)
- [qooba/mlflow-feast](https://github.com/qooba/mlflow-feast)
