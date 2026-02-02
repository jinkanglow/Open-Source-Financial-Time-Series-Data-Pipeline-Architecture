# Deployment Guide

## 前置要求

- Docker & Docker Compose
- Python 3.9+
- PostgreSQL 14+ (TimescaleDB extension)
- Kubernetes cluster (生产环境)
- AWS S3 bucket (用于 Delta Lake)

## 本地开发环境

### 1. 启动所有服务

```bash
# 克隆仓库
git clone <repository-url>
cd financial-timeseries-pipeline

# 启动 Docker Compose 服务
docker-compose up -d

# 验证服务状态
docker-compose ps
```

### 2. 初始化数据库

```bash
# 连接到 TimescaleDB
psql -h localhost -U user -d financial_db

# 运行 schema
\i sql/timescaledb_schema.sql

# 验证 continuous aggregates
SELECT * FROM ohlc_1m_agg LIMIT 10;
```

### 3. 运行测试

```bash
# 安装依赖
pip install -r requirements.txt

# 运行 PIT 测试
pytest tests/test_pit_correctness.py -v

# 运行所有测试
pytest tests/ -v --cov=src
```

### 4. 访问服务

- **TimescaleDB**: `localhost:5432`
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Jaeger**: http://localhost:16686
- **Kafka**: `localhost:9092`
- **Schema Registry**: http://localhost:8081

## 生产环境部署

### Kubernetes 部署

#### 1. 创建命名空间

```bash
kubectl create namespace financial-timeseries
```

#### 2. 创建 Secrets

```bash
# TimescaleDB credentials
kubectl create secret generic timescaledb-credentials \
  --from-literal=username=user \
  --from-literal=password=password \
  -n financial-timeseries

# AWS S3 credentials
kubectl create secret generic s3-credentials \
  --from-literal=access-key-id=<your-access-key> \
  --from-literal=secret-access-key=<your-secret-key> \
  -n financial-timeseries
```

#### 3. 部署 TimescaleDB

```bash
kubectl apply -f k8s/timescaledb-deployment.yaml
kubectl apply -f k8s/timescaledb-service.yaml
```

#### 4. 部署 Kafka

```bash
# 使用 Strimzi Operator
kubectl apply -f k8s/kafka-cluster.yaml
```

#### 5. 部署 Flink Job

```bash
# 构建 Flink job image
docker build -t financial-flink-job:latest -f flink-jobs/Dockerfile .

# 部署 Flink job
kubectl apply -f k8s/flink-job-deployment.yaml
```

#### 6. 部署 Spark Job

```bash
# 使用 Spark Operator
kubectl apply -f k8s/spark-job-deployment.yaml
```

#### 7. 部署 Feast

```bash
# 部署 Feast serving
kubectl apply -f k8s/feast-serving.yaml
```

#### 8. 部署 MLflow

```bash
# 部署 MLflow tracking server
kubectl apply -f k8s/mlflow-deployment.yaml
```

#### 9. 部署 Triton Inference Server

```bash
# 部署 Triton with canary deployment
kubectl apply -f k8s/triton-deployment.yaml
kubectl apply -f k8s/triton-canary.yaml
```

### ArgoCD GitOps 部署

```bash
# 安装 ArgoCD
kubectl create namespace argocd
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

# 创建 ArgoCD Application
kubectl apply -f k8s/argocd-application.yaml
```

## 监控和告警

### Prometheus 告警规则

创建 `monitoring/prometheus-alerts.yml`:

```yaml
groups:
  - name: financial_timeseries
    rules:
      - alert: OHLCSLAViolation
        expr: (now() - ohlc_1m_last_update) > 30
        for: 1m
        annotations:
          summary: "OHLC SLA violation"
      
      - alert: HighLatency
        expr: flink_job_latency_p95 > 1000
        for: 5m
        annotations:
          summary: "Flink job latency too high"
```

### Grafana Dashboard

导入预配置的 dashboard:
- Pipeline Health Dashboard
- Feature Freshness Dashboard
- Model Performance Dashboard

## 数据质量检查

### Great Expectations

```bash
# 运行数据质量检查
great_expectations checkpoint run market_data_checkpoint
```

### Data Contracts

```bash
# 验证数据合约
python src/quality/data_contracts.py validate
```

## 故障排查

### 检查服务状态

```bash
# Kubernetes
kubectl get pods -n financial-timeseries
kubectl logs -f <pod-name> -n financial-timeseries

# Docker Compose
docker-compose logs -f <service-name>
```

### 检查数据库连接

```bash
psql -h localhost -U user -d financial_db -c "SELECT * FROM ohlc_1m_agg LIMIT 1;"
```

### 检查 Kafka

```bash
# List topics
kafka-topics --bootstrap-server localhost:9092 --list

# Consume messages
kafka-console-consumer --bootstrap-server localhost:9092 --topic market-data --from-beginning
```

## 性能调优

### TimescaleDB

- 调整 `chunk_time_interval` 基于数据量
- 优化 continuous aggregate refresh policies
- 添加适当的索引

### Flink

- 调整 parallelism
- 优化 state backend (RocksDB)
- 配置 checkpoint interval

### Spark

- 调整 executor memory and cores
- 优化 shuffle partitions
- 使用 broadcast joins

## 备份和恢复

### TimescaleDB 备份

```bash
# 使用 pg_dump
pg_dump -h localhost -U user financial_db > backup.sql

# 使用 TimescaleDB backup
timescaledb-backup backup --db financial_db --output backup.tar
```

### S3 备份

```bash
# Delta Lake 自动版本控制
# 使用 S3 lifecycle policies 进行归档
```

## 安全

### 启用 TLS

- Kafka: 配置 SSL/TLS
- TimescaleDB: 启用 SSL 连接
- API: 使用 HTTPS

### 行级安全 (RLS)

已在 schema 中启用 RLS，需要配置 tenant 策略。

### 密钥管理

使用 Kubernetes Secrets 或外部密钥管理服务 (如 AWS Secrets Manager)。
