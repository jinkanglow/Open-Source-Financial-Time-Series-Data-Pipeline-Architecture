# Financial Time-Series Data Pipeline Architecture v2.1

## 架构评价

### ✅ 做得很好的地方

1. **清晰的延迟分层**
   - Flink <1s：异常检测、CEP
   - Spark ≥1min：特征计算、回溯
   - 无重叠，职责明确

2. **单一事件源 + 回放**
   - Kafka → Avro Schema Registry → Delta Lake
   - 支持时间旅行、合规审计

3. **Smart-DB 模式**
   - TimescaleDB 拥有：rolling aggregates、连续聚合、RLS
   - 减少往返延迟 60%+，降低系统复杂度

4. **MLOps 完整性**
   - Feast（offline/online）→ MLflow → Triton → FastAPI
   - Canary + Shadow 部署

5. **可观测性内建**
   - OpenTelemetry + OpenLineage
   - 数据血缘可追溯

### 🔴 需强化的地方

| 风险 | 当前状态 | 改进行动 |
|------|---------|---------|
| PIT 正确性 | 提及但未强制 | CI 自动化 PIT 测试，block 不合规 PR |
| Schema 演进 | 基础 Avro | 加入 backward/forward 兼容检查 + 迁移计划 |
| Smart-DB 合同 | 模糊 | 明确文档：哪 11 个字段必须在 DB 中 |
| 模型回滚 | Canary 提及 | 自动回滚触发器：PnL 降 >10% on shadow |
| 成本爆炸 | GPU/冷存储未限 | 成本 budget + auto-scaling 限制 |

## 流处理分工（Flink vs Spark）

| 责任 | 技术 | 延迟 | 交付件 |
|------|------|------|--------|
| 异常检测（3 笔大单据/5分钟） | Flink CEP | <1s | CEP rule + state mgmt |
| 滚动指标（bidask spread 每 tick） | Flink 有状态 | <1s | RocksDB state backend |
| 特征回溯（1 年 SMA） | Spark batch | ≥1min | Spark SQL job |
| 模型训练 | Spark + PyTorch | N/A | 日度定时任务 |

## Exactly-Once 语义

```
Kafka 生产者 (idempotent + acks=all)
    → Flink 两阶段提交 (checkpoint)
    → TimescaleDB 唯一约束 (symbol, timestamp, trade_id)
    → S3 + Delta Lake (ACID + 版本控制)
    → 失败回滚到 DLQ
```

## 模型服务：Triton + Canary + Shadow

### 部署流程

```
Shadow (24h)     → 100% baseline; 10% 复制到 canary
Canary (48h)     → 10% 实时流量到 canary
Ramp (24h)       → 50% → canary
Prod (持续)      → 100% canary（现在是新 baseline）
```

### Shadow 分析（关键）

```python
pnl_baseline = simulate_trades(baseline_regime, prices)
pnl_canary = simulate_trades(canary_regime, prices)
pnl_diff_pct = 100 * (pnl_canary - pnl_baseline) / abs(pnl_baseline)

if abs(pnl_diff_pct) > 10 or latency_p95 > baseline * 1.2:
    return {"status": "reject", "reason": "..."}
```

自动回滚：每 5 分钟检查 SLO，Sharpe 降 >20% → 立即回滚 + 告警

## 成本优化

### 存储分层

| 层 | 技术 | 保留期 | 月成本 | 查询延迟 |
|----|------|--------|--------|---------|
| Hot | TimescaleDB | 30d | ~$500 | <100ms |
| Warm | S3 Standard | 90d | ~$50 | <5s |
| Cold | S3 Glacier | 2y | ~$10 | >24h |

Action: S3 lifecycle policy: Standard (30d) → Intelligent-Tiering (60d) → Glacier (>90d)

### 计算成本

总基础月成本：~$3,850

节省机会：
- Confluent Cloud vs 自建 Kafka: -$900/month ✓
- Spot instances for GPU: -$150/month ✓
- Reserved instances: 30-40% 折扣

优化成本：~$2,500-2,800/month
