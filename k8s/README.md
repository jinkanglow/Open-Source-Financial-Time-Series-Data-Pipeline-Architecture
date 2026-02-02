# Complete Kubernetes Deployment Manifests

This directory contains all Kubernetes deployment configurations for the Financial Timeseries Pipeline.

## Directory Structure

```
k8s/
├── argocd-application.yaml      # ArgoCD GitOps configuration
├── autoscaling/
│   └── hpa-config.yaml          # Horizontal Pod Autoscaler configs
├── cost/
│   └── cost-budget-config.yaml  # Cost budget and monitoring
├── marquez/
│   └── marquez-deployment.yaml  # Marquez (OpenLineage) deployment
├── airflow/
│   └── airflow-deployment.yaml # Airflow deployment
├── timescaledb/
│   └── timescaledb-deployment.yaml  # TimescaleDB deployment
├── flink/
│   └── flink-deployment.yaml    # Flink job deployment
├── spark/
│   └── spark-deployment.yaml    # Spark job deployment
├── feast/
│   └── feast-deployment.yaml    # Feast serving deployment
└── triton/
    └── triton-deployment.yaml   # Triton inference server deployment
```

## Deployment Order

1. **Infrastructure** (TimescaleDB, Kafka, Redis)
2. **Observability** (Prometheus, Grafana, Jaeger, Marquez)
3. **Processing** (Flink, Spark)
4. **MLOps** (Feast, MLflow, Triton)
5. **Orchestration** (Airflow)
6. **GitOps** (ArgoCD)

## Quick Deploy

```bash
# Apply all configurations
kubectl apply -f k8s/ -R

# Or deploy in order
kubectl apply -f k8s/timescaledb/
kubectl apply -f k8s/marquez/
kubectl apply -f k8s/airflow/
kubectl apply -f k8s/autoscaling/
kubectl apply -f k8s/cost/
kubectl apply -f k8s/argocd-application.yaml
```

## Cost Optimization

- HPA limits prevent over-scaling
- Cost budget monitor enforces monthly limits
- S3 lifecycle policy reduces storage costs
- GPU instances use spot pricing

## Security

- RLS policies for multi-tenancy
- Encryption for sensitive data
- Audit logging for compliance
- Network policies for isolation
