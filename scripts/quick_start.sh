#!/bin/bash
# Quick Start Script
# Starts the pipeline and runs a quick demo

set -e

echo "🚀 Quick Start - Financial Timeseries Pipeline"
echo "=============================================="

# Start services
echo "Starting services..."
docker-compose up -d

# Wait for services
echo "Waiting for services..."
sleep 15

# Generate test data
echo "Generating test data..."
python scripts/generate_test_data.py

# Wait for processing
echo "Waiting for data processing..."
sleep 10

# Query features
echo "Querying features from TimescaleDB..."
python -c "
from src.features.smartdb_contract import SmartDBContract
from sqlalchemy import create_engine
from datetime import datetime, timedelta

engine = create_engine('postgresql://user:password@localhost:5432/financial_db')
contract = SmartDBContract(engine)

snapshot = contract.get_pit_snapshot('AAPL', datetime.now() - timedelta(minutes=5))
print(f'PIT Snapshot:')
print(f'  OHLC Close: {snapshot.ohlc_1m_close}')
print(f'  SMA_20: {snapshot.sma_20}')
"

echo ""
echo "✅ Quick start complete!"
echo ""
echo "Access services:"
echo "  - Grafana: http://localhost:3000"
echo "  - Prometheus: http://localhost:9090"
echo "  - Jaeger: http://localhost:16686"
