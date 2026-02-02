# Smart-DB Contract Specification

**Version**: 2.1 | **Last Updated**: Jan 23, 2026

## 概述

Smart-DB Contract 定义了 TimescaleDB 中必须维护的 11 个核心特征字段。这些字段是系统的"真实源"（Single Source of Truth），所有应用层必须从数据库读取，不得在应用层重新计算。

## 核心原则

1. **DB 是真实源**：所有特征计算在数据库层完成
2. **SLA 强制**：每个特征都有明确的新鲜度 SLA
3. **PIT 正确性**：Point-in-time 查询必须无未来数据泄露
4. **可测试性**：所有特征必须可通过自动化测试验证

## 11 个核心特征

### 1. ohlc_1m (1分钟 OHLC)

**类型**: 4×NUMERIC (open, high, low, close)  
**计算逻辑**: 1分钟时间窗口的 OHLC 聚合  
**新鲜度 SLA**: ≤30s  
**实现方式**: `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)`  
**PIT 测试方法**: 已知时间窗口的 OHLC 值与离线基准对齐

```sql
CREATE MATERIALIZED VIEW ohlc_1m_agg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 minute', time) as minute,
  symbol,
  first(price, time) as open,
  max(price) as high,
  min(price) as low,
  last(price, time) as close,
  sum(volume) as volume
FROM market_data_raw
GROUP BY minute, symbol;
```

### 2. sma_20 (20周期简单移动平均)

**类型**: NUMERIC  
**计算逻辑**: `avg(price) over 20 tick`  
**新鲜度 SLA**: ≤2min  
**实现方式**: Continuous aggregate + Flink trigger write  
**PIT 测试方法**: vs Spark 离线计算 ±ε

### 3. ewm_12 (12周期指数移动平均)

**类型**: NUMERIC  
**计算逻辑**: 指数移动平均 α=0.15  
**新鲜度 SLA**: ≤2min  
**实现方式**: 存储过程或 Flink aggregate  
**PIT 测试方法**: Deterministic seed + 边界条件

### 4. volatility_1h (1小时波动率)

**类型**: NUMERIC  
**计算逻辑**: `stddev(log_return)`  
**新鲜度 SLA**: ≤2min  
**实现方式**: Continuous aggregate  
**PIT 测试方法**: PIT：无未来数据泄露

### 5. vwap_5m (5分钟成交量加权平均价)

**类型**: NUMERIC  
**计算逻辑**: `sum(p×v)/sum(v)`  
**新鲜度 SLA**: ≤30s  
**实现方式**: 存储过程  
**PIT 测试方法**: FX 参考库对齐

### 6. large_trade_flag (大单标志)

**类型**: BOOLEAN  
**计算逻辑**: `volume > Q95`  
**新鲜度 SLA**: ≤30s  
**实现方式**: Flink CEP + 列写  
**PIT 测试方法**: 注入合成大单，验证触发

### 7. bidask_spread (买卖价差)

**类型**: NUMERIC  
**计算逻辑**: `ask - bid`  
**新鲜度 SLA**: ≤1s  
**实现方式**: Flink 直接 insert  
**PIT 测试方法**: 延迟测试：ts_ingestion - ts_trade < 1s

### 8. trade_imbalance_5m (5分钟交易不平衡)

**类型**: NUMERIC  
**计算逻辑**: `(buy-sell)/(buy+sell)`  
**新鲜度 SLA**: ≤30s  
**实现方式**: Continuous aggregate  
**PIT 测试方法**: 买卖标签注入测试

### 9. regime_tag (市场状态标签)

**类型**: VARCHAR  
**计算逻辑**: `IF(price > sma_20, 'up', 'down')`  
**新鲜度 SLA**: ≤1min  
**实现方式**: SQL 逻辑 + index  
**PIT 测试方法**: vs 离线模型标签，>95% 一致

### 10. news_sentiment_embedding (新闻情感向量)

**类型**: vector(1536)  
**计算逻辑**: pgvector + 外部 API  
**新鲜度 SLA**: async (5min)  
**实现方式**: 异步 job → upsert  
**PIT 测试方法**: Recall@k 测试

### 11. feature_pit_snapshot() (PIT 特征快照)

**类型**: RECORD  
**计算逻辑**: 复合存储过程  
**新鲜度 SLA**: instant  
**实现方式**: 核心 Feast 接口  
**PIT 测试方法**: 100 个历史时间戳，无未来泄露

```sql
CREATE OR REPLACE FUNCTION feature_pit_snapshot(
    p_symbol VARCHAR,
    p_as_of_ts TIMESTAMPTZ
)
RETURNS TABLE (
    ohlc_1m_open NUMERIC,
    ohlc_1m_high NUMERIC,
    ohlc_1m_low NUMERIC,
    ohlc_1m_close NUMERIC,
    sma_20 NUMERIC,
    ewm_12 NUMERIC,
    volatility_1h NUMERIC,
    vwap_5m NUMERIC,
    large_trade_flag BOOLEAN,
    bidask_spread NUMERIC,
    trade_imbalance_5m NUMERIC,
    regime_tag VARCHAR,
    news_sentiment_embedding vector(1536)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        o.open, o.high, o.low, o.close,
        s.sma_20,
        e.ewm_12,
        v.volatility_1h,
        vw.vwap_5m,
        l.large_trade_flag,
        b.bidask_spread,
        ti.trade_imbalance_5m,
        r.regime_tag,
        n.news_sentiment_embedding
    FROM ohlc_1m_agg o
    LEFT JOIN sma_20_agg s ON s.symbol = o.symbol AND s.time <= p_as_of_ts
    LEFT JOIN ewm_12_agg e ON e.symbol = o.symbol AND e.time <= p_as_of_ts
    LEFT JOIN volatility_1h_agg v ON v.symbol = o.symbol AND v.time <= p_as_of_ts
    LEFT JOIN vwap_5m_agg vw ON vw.symbol = o.symbol AND vw.time <= p_as_of_ts
    LEFT JOIN large_trade_flags l ON l.symbol = o.symbol AND l.time <= p_as_of_ts
    LEFT JOIN bidask_spreads b ON b.symbol = o.symbol AND b.time <= p_as_of_ts
    LEFT JOIN trade_imbalance_5m_agg ti ON ti.symbol = o.symbol AND ti.time <= p_as_of_ts
    LEFT JOIN regime_tags r ON r.symbol = o.symbol AND r.time <= p_as_of_ts
    LEFT JOIN news_sentiment n ON n.symbol = o.symbol AND n.time <= p_as_of_ts
    WHERE o.symbol = p_symbol
      AND o.minute <= p_as_of_ts
    ORDER BY o.minute DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;
```

## SLA 监控

所有特征必须满足其 SLA。监控通过以下方式实现：

1. **Prometheus 指标**: 每个特征的新鲜度指标
2. **Grafana 告警**: SLA 违反时触发告警
3. **CI 测试**: 每次 PR 自动运行 PIT 测试

## 验收标准

- ✅ 所有 11 个字段在 TimescaleDB 中实现
- ✅ PIT 测试全部通过
- ✅ SLA 监控就绪
- ✅ 文档完整
- ✅ 无应用层重复计算
