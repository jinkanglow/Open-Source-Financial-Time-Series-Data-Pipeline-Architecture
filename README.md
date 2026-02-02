# Financial Time-Series Data Pipeline Architecture v2.1

**Status**: Ready for Implementation | **Version**: 2.1 | **Last Updated**: Jan 23, 2026

## 🚀 快速开始（3 分钟理解核心）

### 架构概览

```
Data Sources (Kafka, APIs)
    ↓
[Kafka + Avro Schema Registry + DLQ]
    ↓
┌─ Real-time Path (Flink <1s) ─→ Timescale continuous aggregates → Redis → API
│
└─ Batch Path (Spark ≥1min) ─→ Delta Lake → Feast → MLflow → Triton

Smart-DB Layer (TimescaleDB):
  - 11 continuous aggregates (OHLC, SMA, volatility, etc.)
  - Point-in-time stored procedures
  - RLS for multi-tenancy

Observability: OpenTelemetry + Prometheus + Grafana + OpenLineage
```

## 📋 核心特征（Smart-DB Down-Sink）

| 特征                   | 计算位置                         | SLA     | GitHub 参考                         |
| ---------------------- | -------------------------------- | ------- | ----------------------------------- |
| ohlc_1m                | TimescaleDB continuous aggregate | ≤30s    | hoangsonww/End-to-End-Data-Pipeline |
| sma_20                 | Continuous aggregate + view      | ≤2min   | TimescaleDB docs                    |
| volatility_1h          | Stored procedure                 | ≤2min   | Paper: VLDB 2020                    |
| feature_pit_snapshot() | SQL stored proc (PIT interface)  | instant | Feast integration                   |
| regime_tag             | SQL logic + indexing             | ≤1min   | Custom                              |

## 🏗️ 项目结构

```
financial-timeseries-pipeline/
├── README.md                          # 本文档
├── ARCHITECTURE.md                    # 详细架构文档
├── docker-compose.yml                 # 本地开发环境
├── requirements.txt                   # Python 依赖
├── sql/
│   ├── smartdb_contract.md            # Smart-DB Contract 定义
│   ├── timescaledb_schema.sql         # TimescaleDB DDL
│   └── migrations/                    # Flyway 迁移脚本
├── src/
│   ├── features/
│   │   └── smartdb_contract.py        # Python 类强制 SLA
│   ├── observability/
│   │   └── otel_instrumentation.py    # OpenTelemetry 设置
│   ├── modeling/
│   │   └── train_reproducible.py      # 可重现训练
│   └── quality/
│       └── data_contracts.py         # 数据质量合约
├── flink-jobs/
│   └── market-realtime/               # Flink CEP 作业
├── feast_repo/
│   └── feature_definitions.py         # Feast FeatureView 定义
├── tests/
│   ├── test_pit_correctness.py        # PIT 正确性测试
│   └── test_smartdb_correctness.py    # Smart-DB 测试
├── .github/
│   └── workflows/
│       ├── pit_tests.yml              # CI PIT 测试
│       └── model_validation.yml       # 模型验证
└── k8s/                               # Kubernetes 部署配置
```

## 🔗 GitHub 参考项目

- **[hoangsonww/End-to-End-Data-Pipeline](https://github.com/hoangsonww/End-to-End-Data-Pipeline)**: Spark batch 和 Spark Streaming 的完整示例
- **[qooba/mlflow-feast](https://github.com/qooba/mlflow-feast)**: MLflow + Feast 集成示例
- **[dmatrix/feast_workshops](https://github.com/dmatrix/feast_workshops)**: Feast 完整教程

## 📚 论文参考

- "Real-time Event Joining in Practice With Kafka and Flink" (arXiv, 2024)
- "TimescaleDB Continuous Aggregates for High-Volume Time Series" (PostgresConf 2024)
- "MLOps: A Step Forward to Enterprise Machine Learning" (arXiv, 2023)

## 🛠️ 技术栈

- **流处理**: Apache Flink (CEP), Apache Kafka
- **批处理**: Apache Spark, Delta Lake
- **数据库**: TimescaleDB (Smart-DB), Redis
- **特征存储**: Feast (Online/Offline)
- **MLOps**: MLflow, Triton Inference Server
- **可观测性**: OpenTelemetry, Prometheus, Grafana, OpenLineage
- **CI/CD**: GitHub Actions, ArgoCD

## 📖 文档

- [Smart-DB Contract](./sql/smartdb_contract.md) - 11 个字段的完整规格
- [架构文档](./ARCHITECTURE.md) - 详细架构说明
- [部署指南](./docs/DEPLOYMENT.md) - 部署步骤

## 🚦 快速开始

### 前置要求

- Docker & Docker Compose
- Python 3.9+
- PostgreSQL 14+ (TimescaleDB extension)

### 本地启动

```bash
# 克隆仓库
git clone <repository-url>
cd financial-timeseries-pipeline

# 启动所有服务
docker-compose up -d

# 运行测试
pytest tests/

# 查看服务
# TimescaleDB: localhost:5432
# Grafana: http://localhost:3000
# Prometheus: http://localhost:9090
```

## 📅 12 周实现时间表

| 周  | 阶段 | 里程碑                                       | 任务        |
| --- | ---- | -------------------------------------------- | ----------- |
| 1-2 | 基础 | Kafka + Timescale 本地运行                   | P0.1 + P0.2 |
| 3   | 基础 | Flink job MVP                                | P0.3        |
| 4-5 | 特征 | Feast 集成                                   | P1.1        |
| 6-7 | ML   | MLflow + 可重现性                            | P1.2        |
| 8   | 服务 | Triton + canary + shadow                     | P1.3        |
| 9   | 运维 | OpenTelemetry + Great Expectations + Marquez | P2.1-P2.3   |
| 10  | 运维 | GitHub Actions + ArgoCD                      | P2.4        |
| 11  | 安全 | RLS + 加密 + 审计日志                        | Compliance  |
| 12  | 上线 | 压测 + DR 演练 + Go-live                     | Launch      |

## 📚 文档索引

完整的文档列表请查看 **[DOCS_INDEX.md](DOCS_INDEX.md)**

### 快速导航

- **[PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md)** - 完整项目概览
- **[QUICK_START.md](QUICK_START.md)** - 快速开始指南
- **[GETTING_STARTED.md](GETTING_STARTED.md)** - 详细入门指南
- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** - 部署指南
- **[RUNNING_EXAMPLES.md](RUNNING_EXAMPLES.md)** - 运行示例

## 📝 License

MIT License

## 🤝 Contributing

欢迎提交 Issue 和 Pull Request！
