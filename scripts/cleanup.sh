#!/bin/bash
# Cleanup Script
# Stops all services and cleans up resources

set -e

echo "🧹 Cleaning up Financial Timeseries Pipeline..."

# Stop Docker services
echo "Stopping Docker services..."
docker-compose down -v

# Remove Python cache
echo "Removing Python cache..."
find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Remove test artifacts
echo "Removing test artifacts..."
rm -rf .pytest_cache/
rm -rf .coverage
rm -rf htmlcov/
rm -rf test-results/

# Remove MLflow artifacts
echo "Removing MLflow artifacts..."
rm -rf mlruns/
rm -f mlflow.db

# Remove Feast artifacts
echo "Removing Feast artifacts..."
rm -rf feast_repo/.feast/

echo "✅ Cleanup complete!"
