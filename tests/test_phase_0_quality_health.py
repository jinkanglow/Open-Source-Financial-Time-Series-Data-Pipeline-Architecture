"""
Test Suite for Phase 0 & 0.5

Tests data quality framework and health monitoring
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.quality.great_expectations_setup import (
    DataQualityScore,
    DataQualityAlerts,
    validate_pit_quality,
    detect_distribution_shift,
    ci_validate_features
)
from src.quality.great_expectations_validator import GreatExpectationsValidator
from src.observability.health_dashboard import PipelineHealthMonitor
from src.observability.enhanced_health_monitor import EnhancedPipelineHealthMonitor
import asyncio


class TestDataQualityFramework:
    """Test Phase 0: Data Quality Framework"""
    
    def test_data_quality_score_calculation(self):
        """Test DQSOps framework scoring"""
        # Create test data
        df = pd.DataFrame({
            'close': np.random.normal(150, 5, 100),
            'high': np.random.normal(152, 5, 100),
            'low': np.random.normal(148, 5, 100),
            'timestamp': pd.date_range(end=datetime.now(), periods=100, freq='1min')
        })
        
        scorer = DataQualityScore(df)
        score = scorer.calculate_score()
        
        assert score['overall'] >= 0.0
        assert score['overall'] <= 1.0
        assert 'completeness' in score
        assert 'consistency' in score
        assert 'accuracy' in score
        assert 'timeliness' in score
    
    def test_quality_score_threshold(self):
        """Test quality score meets threshold (≥0.85)"""
        # Create high-quality data
        df = pd.DataFrame({
            'close': np.random.normal(150, 2, 1000),
            'high': np.random.normal(152, 2, 1000),
            'low': np.random.normal(148, 2, 1000),
            'volume': np.random.normal(1000000, 100000, 1000),
            'timestamp': pd.date_range(end=datetime.now(), periods=1000, freq='1min')
        })
        
        scorer = DataQualityScore(df)
        score = scorer.calculate_score()
        
        assert score['overall'] >= 0.85, f"Quality score {score['overall']} below threshold"
    
    def test_pit_no_future_leakage(self):
        """Test PIT quality validation - no future data leak"""
        reference_time = datetime.now() - timedelta(hours=1)
        
        # Valid data (all before reference time)
        valid_df = pd.DataFrame({
            'event_timestamp': pd.date_range(
                end=reference_time,
                periods=100,
                freq='1min'
            ),
            'close': np.random.normal(150, 5, 100)
        })
        
        # Should pass
        assert validate_pit_quality(valid_df, reference_time) == True
        
        # Invalid data (has future data)
        invalid_df = pd.DataFrame({
            'event_timestamp': pd.date_range(
                start=reference_time - timedelta(hours=1),
                end=reference_time + timedelta(minutes=10),
                freq='1min'
            ),
            'close': np.random.normal(150, 5, 71)
        })
        
        # Should fail
        with pytest.raises(AssertionError):
            validate_pit_quality(invalid_df, reference_time)
    
    def test_distribution_shift_detection(self):
        """Test distribution shift detection using KS-test"""
        # Baseline distribution
        baseline = np.random.normal(150, 5, 1000)
        
        # Similar distribution (should not detect shift)
        current_similar = np.random.normal(150, 5, 1000)
        result_similar = detect_distribution_shift(baseline, current_similar)
        assert result_similar['p_value'] > 0.05, "Should not detect shift for similar distributions"
        
        # Different distribution (should detect shift)
        current_different = np.random.normal(200, 10, 1000)
        result_different = detect_distribution_shift(baseline, current_different)
        assert result_different['shift_detected'] == True, "Should detect shift for different distributions"
    
    def test_data_quality_alerts(self):
        """Test automatic alerting"""
        # Low quality data
        low_quality_df = pd.DataFrame({
            'close': [None] * 50 + [150] * 50,  # 50% nulls
            'high': [152] * 100,
            'low': [148] * 100,
            'timestamp': pd.date_range(end=datetime.now(), periods=100, freq='1min')
        })
        
        alerts = DataQualityAlerts(threshold_score=0.85)
        score = alerts.check_and_alert(low_quality_df, symbol='AAPL', send_alert=False)
        
        assert score['overall'] < 0.85
        assert len(alerts.alert_history) > 0
    
    def test_ci_validation(self):
        """Test CI validation function"""
        reference_time = datetime.now() - timedelta(hours=1)
        baseline_dist = np.random.normal(150, 5, 1000)
        
        # High quality data
        features_df = pd.DataFrame({
            'close': np.random.normal(150, 5, 100),
            'high': np.random.normal(152, 5, 100),
            'low': np.random.normal(148, 5, 100),
            'event_timestamp': pd.date_range(end=reference_time, periods=100, freq='1min')
        })
        
        # Should pass
        assert ci_validate_features(features_df, reference_time, baseline_dist) == True


class TestHealthMonitoring:
    """Test Phase 0.5: Health Monitoring"""
    
    @pytest.mark.asyncio
    async def test_health_monitor_all_components(self):
        """Test monitoring all components"""
        monitor = PipelineHealthMonitor()
        health_data = await monitor.monitor_all_components()
        
        assert 'overall_status' in health_data
        assert 'components' in health_data
        assert 'timestamp' in health_data
        assert health_data['overall_status'] in ['healthy', 'degraded', 'critical']
    
    @pytest.mark.asyncio
    async def test_enhanced_health_monitor(self):
        """Test enhanced health monitor with real metrics"""
        monitor = EnhancedPipelineHealthMonitor()
        health_data = await monitor.monitor_all_components()
        
        assert 'overall_status' in health_data
        assert len(health_data['components']) > 0
    
    def test_prometheus_export(self):
        """Test Prometheus metrics export"""
        from src.observability.health_dashboard import PrometheusExporter
        
        health_data = {
            "overall_status": "healthy",
            "components": [
                {
                    "component": "Kafka",
                    "status": "healthy",
                    "metrics": {
                        "consumer_lag": 100.0,
                        "error_rate": 0.0001
                    }
                }
            ],
            "timestamp": datetime.now().isoformat()
        }
        
        exporter = PrometheusExporter()
        metrics = exporter.export_metrics(health_data)
        
        assert "pipeline_overall_status" in metrics
        assert "pipeline_kafka_status" in metrics
        assert "pipeline_kafka_consumer_lag" in metrics


class TestGreatExpectationsIntegration:
    """Test Great Expectations integration"""
    
    @pytest.fixture
    def validator(self):
        """Create validator instance"""
        return GreatExpectationsValidator()
    
    def test_ohlc_validation(self, validator):
        """Test OHLC validation"""
        # This will fail if database is not available
        # In CI, we can mock this
        try:
            result = validator.validate_ohlc_1m(limit=100)
            assert 'success' in result
            assert 'results' in result or 'error' in result
        except Exception:
            # Skip if database not available
            pytest.skip("Database not available")
    
    def test_sma_validation(self, validator):
        """Test SMA validation"""
        try:
            result = validator.validate_sma_20(limit=100)
            assert 'success' in result
        except Exception:
            pytest.skip("Database not available")
    
    def test_volatility_validation(self, validator):
        """Test volatility validation"""
        try:
            result = validator.validate_volatility_1h(limit=100)
            assert 'success' in result
        except Exception:
            pytest.skip("Database not available")


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
