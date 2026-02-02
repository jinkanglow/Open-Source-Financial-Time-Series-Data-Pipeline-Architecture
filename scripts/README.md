# Scripts Directory

This directory contains utility scripts for the Financial Timeseries Pipeline.

## Available Scripts

### Setup and Initialization
- `setup.sh` / `setup.bat` - One-click setup script
- `health_check.sh` - Quick health check for all services
- `quick_start.sh` - Quick start demo

### Data Management
- `generate_test_data.py` - Generate synthetic market data to Kafka
- `check_data_quality.py` - Comprehensive data quality checker

### Monitoring and Validation
- `validate_deployment.py` - Deployment validation script
- `monitor_pipeline_health.py` - Pipeline health monitor
- `benchmark_performance.py` - Performance benchmarking

### Maintenance
- `cleanup.sh` - Clean up test artifacts and caches

## Usage

### Setup
```bash
# Linux/Mac
bash scripts/setup.sh

# Windows
scripts\setup.bat
```

### Health Check
```bash
bash scripts/health_check.sh
```

### Data Quality Check
```bash
python scripts/check_data_quality.py
```

### Pipeline Health Monitor
```bash
python scripts/monitor_pipeline_health.py
```

### Performance Benchmark
```bash
python scripts/benchmark_performance.py
```

### Generate Test Data
```bash
python scripts/generate_test_data.py
```

### Quick Start
```bash
bash scripts/quick_start.sh
```

### Cleanup
```bash
bash scripts/cleanup.sh
```

## Examples

See `examples/` directory for complete usage examples:
- `end_to_end_example.py` - End-to-end pipeline example
- `complete_demo.py` - Complete demo with all components

