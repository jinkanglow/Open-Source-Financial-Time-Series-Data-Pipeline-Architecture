# Configuration Files

This directory contains configuration templates and management utilities.

## Files

- `.env.example` - Environment variables template
- `src/config/settings.py` - Configuration management module

## Usage

### 1. Setup Environment Variables

Copy the example file and fill in your values:

```bash
cp .env.example .env
# Edit .env with your actual values
```

### 2. Use Configuration in Code

```python
from src.config.settings import config

# Access database config
db_url = config.database.url

# Access Kafka config
kafka_servers = config.kafka.bootstrap_servers

# Access feature SLA config
ohlc_sla = config.feature_sla.ohlc_sla_seconds
```

### 3. Validate Configuration

```bash
python scripts/validate_deployment.py
```

## Configuration Categories

### Database
- TimescaleDB connection settings
- Connection pooling options

### Kafka
- Bootstrap servers
- Schema Registry URL
- Topic names

### Redis
- Host and port
- Authentication

### S3 / Delta Lake
- AWS credentials
- Bucket name
- Region

### MLflow
- Tracking URI
- Backend store
- Artifact root

### Feature SLAs
- OHLC freshness: ≤30s
- SMA_20 freshness: ≤2min
- VWAP freshness: ≤30s
- Volatility freshness: ≤2min

### Model Configuration
- Sharpe ratio threshold: 0.5
- PnL difference threshold: 10%
- Latency P95 threshold: 150ms
