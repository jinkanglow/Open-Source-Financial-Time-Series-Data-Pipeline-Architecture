#!/bin/bash
# Health Check Script
# Checks health of all pipeline components

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "🔍 Health Check for Financial Timeseries Pipeline"
echo "=================================================="
echo ""

# Check TimescaleDB
echo -n "TimescaleDB: "
if docker-compose exec -T timescaledb pg_isready -U user > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Kafka
echo -n "Kafka: "
if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Schema Registry
echo -n "Schema Registry: "
if curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Redis
echo -n "Redis: "
if docker-compose exec -T redis redis-cli ping > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Prometheus
echo -n "Prometheus: "
if curl -s http://localhost:9090/-/healthy > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Grafana
echo -n "Grafana: "
if curl -s http://localhost:3000/api/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check Jaeger
echo -n "Jaeger: "
if curl -s http://localhost:16686 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Healthy${NC}"
else
    echo -e "${RED}✗ Unhealthy${NC}"
fi

# Check database schema
echo -n "Database Schema: "
if docker-compose exec -T timescaledb psql -U user -d financial_db -c "SELECT COUNT(*) FROM ohlc_1m_agg;" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Initialized${NC}"
else
    echo -e "${YELLOW}⚠ Not initialized${NC}"
fi

echo ""
echo "Health check complete!"
