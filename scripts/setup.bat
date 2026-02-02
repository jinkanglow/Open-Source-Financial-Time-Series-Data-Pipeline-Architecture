@echo off
REM Setup Script for Financial Timeseries Pipeline (Windows)
REM Based on: hoangsonww/End-to-End-Data-Pipeline

echo Setting up Financial Timeseries Pipeline...

REM Check prerequisites
where docker >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Docker is required but not installed. Aborting.
    exit /b 1
)

where python >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Python is required but not installed. Aborting.
    exit /b 1
)

REM Create virtual environment
echo Creating Python virtual environment...
python -m venv venv
call venv\Scripts\activate.bat

REM Install dependencies
echo Installing Python dependencies...
pip install --upgrade pip
pip install -r requirements.txt

REM Start Docker services
echo Starting Docker services...
docker-compose up -d

REM Wait for services
echo Waiting for services to be ready...
timeout /t 10 /nobreak >nul

REM Initialize database
echo Initializing TimescaleDB schema...
docker-compose exec -T timescaledb psql -U user -d financial_db -f /docker-entrypoint-initdb.d/01-schema.sql

echo Setup Complete!
echo Next steps:
echo 1. Generate test data: python scripts\generate_test_data.py
echo 2. View Grafana: http://localhost:3000
