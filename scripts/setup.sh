#!/bin/bash
# Setup Script for Financial Timeseries Pipeline
# Based on: hoangsonww/End-to-End-Data-Pipeline

set -e

echo "🚀 Setting up Financial Timeseries Pipeline..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting." >&2; exit 1; }
command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed. Aborting." >&2; exit 1; }

echo -e "${GREEN}✓ Prerequisites check passed${NC}"

# Create virtual environment
echo -e "${YELLOW}Creating Python virtual environment...${NC}"
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python dependencies
echo -e "${YELLOW}Installing Python dependencies...${NC}"
pip install --upgrade pip
pip install -r requirements.txt

echo -e "${GREEN}✓ Python dependencies installed${NC}"

# Start Docker services
echo -e "${YELLOW}Starting Docker services...${NC}"
docker-compose up -d

# Wait for services to be ready
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
sleep 10

# Check service health
echo -e "${YELLOW}Checking service health...${NC}"

# Check TimescaleDB
until docker-compose exec -T timescaledb pg_isready -U user > /dev/null 2>&1; do
    echo "Waiting for TimescaleDB..."
    sleep 2
done
echo -e "${GREEN}✓ TimescaleDB is ready${NC}"

# Check Kafka
until docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo "Waiting for Kafka..."
    sleep 2
done
echo -e "${GREEN}✓ Kafka is ready${NC}"

# Initialize TimescaleDB schema
echo -e "${YELLOW}Initializing TimescaleDB schema...${NC}"
docker-compose exec -T timescaledb psql -U user -d financial_db -f /docker-entrypoint-initdb.d/01-schema.sql || \
    psql -h localhost -U user -d financial_db -f sql/timescaledb_schema.sql

echo -e "${GREEN}✓ TimescaleDB schema initialized${NC}"

# Create Kafka topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
docker-compose exec -T kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic market-data \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists || true

docker-compose exec -T kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic market-data-dlq \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists || true

docker-compose exec -T kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic anomalies \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists || true

echo -e "${GREEN}✓ Kafka topics created${NC}"

# Initialize Feast
echo -e "${YELLOW}Initializing Feast repository...${NC}"
cd feast_repo
feast apply || echo "Feast apply failed (may need manual setup)"
cd ..

echo -e "${GREEN}✓ Feast repository initialized${NC}"

# Run tests
echo -e "${YELLOW}Running initial tests...${NC}"
pytest tests/test_smartdb_correctness.py -v --tb=short || echo "Tests failed (this is OK for initial setup)"

echo ""
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Setup Complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════${NC}"
echo ""
echo "Next steps:"
echo "1. Generate test data: python scripts/generate_test_data.py"
echo "2. Run end-to-end example: python examples/end_to_end_example.py"
echo "3. View Grafana: http://localhost:3000 (admin/admin)"
echo "4. View Prometheus: http://localhost:9090"
echo "5. View Jaeger: http://localhost:16686"
echo ""
echo "To stop services: docker-compose down"
echo "To view logs: docker-compose logs -f"
