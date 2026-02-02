"""
Pipeline Health Monitoring Dashboard

Based on:
- "Towards Observability for Production ML Pipelines" (arXiv 2022-07-15)
- "Real-Time ML Pipelines: Machine Learning on Streaming Data" (Conduktor 2026-01-20)
"""

import asyncio
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
from datetime import datetime
from src.config.settings import config
from src.quality.great_expectations_setup import DataQualityScore
from sqlalchemy import create_engine, text
import pandas as pd


@dataclass
class HealthMetric:
    """Health metric data structure"""
    name: str
    status: str  # "healthy", "degraded", "critical"
    value: float
    threshold: float
    timestamp: str


class PipelineHealthMonitor:
    """Monitor health of all pipeline components"""
    
    def __init__(self):
        self.metrics = {}
        self.alerts = []
        self.db_engine = create_engine(config.database.url)
    
    async def monitor_all_components(self) -> Dict:
        """Monitor all components asynchronously"""
        tasks = [
            self.monitor_kafka(),
            self.monitor_timescaledb(),
            self.monitor_flink_jobs(),
            self.monitor_feast_features(),
            self.monitor_model_performance(),
            self.monitor_feature_quality(),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        components = []
        for result in results:
            if isinstance(result, Exception):
                components.append({
                    "component": "Unknown",
                    "status": "critical",
                    "error": str(result)
                })
            else:
                components.append(result)
        
        return self._aggregate_health(components)
    
    async def monitor_kafka(self) -> Dict:
        """
        Monitor Kafka: consumer lag, replication factor, error rate
        
        Based on: "Real-Time ML Pipelines" - Feature Quality Monitoring
        """
        try:
            from kafka import KafkaConsumer
            
            consumer = KafkaConsumer(
                bootstrap_servers=config.kafka.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            
            # Get consumer lag (simplified - in production use Kafka admin API)
            consumer_lag = await self._get_consumer_lag()
            error_rate = await self._get_error_rate()
            throughput = await self._get_throughput()
            
            consumer.close()
            
            metrics = {
                "consumer_lag": consumer_lag,  # Should be < 1000 records
                "replication_factor": 3,
                "error_rate": error_rate,  # Should be < 0.01%
                "message_throughput_ppm": throughput,  # Expected 65k/sec
            }
            
            status = "healthy"
            if consumer_lag > 5000:
                status = "degraded"
            if error_rate > 0.001:
                status = "critical"
            
            return {
                "component": "Kafka",
                "status": status,
                "metrics": metrics
            }
        except Exception as e:
            return {
                "component": "Kafka",
                "status": "critical",
                "error": str(e)
            }
    
    async def monitor_timescaledb(self) -> Dict:
        """
        Monitor TimescaleDB: continuous aggregate freshness, query latency
        
        Based on: TimescaleDB continuous aggregates documentation
        """
        try:
            ca_lag = await self._get_ca_lag()  # Should be < 2 minutes
            query_latency = await self._get_query_latency()  # Should be < 100ms
            storage = await self._get_storage()
            compression = await self._get_compression()  # Usually 10:1
            
            metrics = {
                "continuous_aggregate_lag_seconds": ca_lag,
                "p95_query_latency_ms": query_latency,
                "hot_storage_used_gb": storage,
                "compression_ratio": compression,
            }
            
            status = "healthy"
            if ca_lag > 120:  # 2 minutes
                status = "degraded"
            if query_latency > 500:
                status = "critical"
            
            return {
                "component": "TimescaleDB",
                "status": status,
                "metrics": metrics
            }
        except Exception as e:
            return {
                "component": "TimescaleDB",
                "status": "critical",
                "error": str(e)
            }
    
    async def monitor_flink_jobs(self) -> Dict:
        """Monitor Flink jobs: checkpoint success rate, end-to-end latency"""
        try:
            jobs = await self._list_flink_jobs()
            metrics = {}
            
            for job in jobs:
                job_health = {
                    "checkpoint_success_rate": await self._get_checkpoint_success(job),  # Should be > 99%
                    "e2e_latency_p95_ms": await self._get_e2e_latency(job),  # Should be < 1000ms
                    "backpressure": await self._get_backpressure(job),  # Should be < 10%
                    "state_size_mb": await self._get_state_size(job),
                }
                metrics[job['name']] = job_health
            
            status = "healthy"
            for job_name, job_metrics in metrics.items():
                if job_metrics['checkpoint_success_rate'] < 0.99:
                    status = "degraded"
                if job_metrics['e2e_latency_p95_ms'] > 5000:
                    status = "critical"
            
            return {
                "component": "Flink Jobs",
                "status": status,
                "metrics": metrics
            }
        except Exception as e:
            return {
                "component": "Flink Jobs",
                "status": "critical",
                "error": str(e)
            }
    
    async def monitor_feast_features(self) -> Dict:
        """
        Monitor Feast: materialization freshness, PIT correctness
        
        Based on: Feature Store Architecture papers
        """
        try:
            features = await self._list_feast_features()
            metrics = {}
            
            for feature in features:
                feature_health = {
                    "last_materialization_age_minutes": await self._get_mat_age(feature),
                    "pit_correctness_score": await self._validate_pit(feature),  # Should be = 1.0
                    "online_store_freshness_ms": await self._get_online_freshness(feature),
                    "data_quality_score": await self._get_dq_score(feature),  # Should be > 0.85
                }
                metrics[feature['name']] = feature_health
            
            status = "healthy"
            for feature_name, feature_metrics in metrics.items():
                if feature_metrics['pit_correctness_score'] < 1.0:
                    status = "critical"  # PIT error is critical!
                if feature_metrics['data_quality_score'] < 0.8:
                    status = "degraded"
            
            return {
                "component": "Feast Features",
                "status": status,
                "metrics": metrics
            }
        except Exception as e:
            return {
                "component": "Feast Features",
                "status": "critical",
                "error": str(e)
            }
    
    async def monitor_model_performance(self) -> Dict:
        """
        Monitor model: shadow vs canary PnL comparison
        
        Based on: "Real-World MLOps: Dev to Production" (DZone 2025-06-04)
        """
        try:
            shadow_pnl = await self._get_shadow_pnl('baseline')
            canary_pnl = await self._get_canary_pnl()
            pnl_diff_pct = await self._get_pnl_diff_pct()  # Should be < 10%
            latency_ratio = await self._get_latency_ratio()  # Should be < 1.2x
            drift_score = await self._calculate_drift()  # Should be < 0.2
            
            metrics = {
                "shadow_baseline_pnl_usd": shadow_pnl,
                "canary_pnl_usd": canary_pnl,
                "pnl_diff_percent": pnl_diff_pct,
                "latency_p95_vs_baseline": latency_ratio,
                "model_drift_score": drift_score,
            }
            
            status = "healthy"
            if abs(pnl_diff_pct) > 10:
                status = "degraded"
                if abs(pnl_diff_pct) > 20:
                    status = "critical"  # Auto rollback
            
            return {
                "component": "Model Performance",
                "status": status,
                "metrics": metrics
            }
        except Exception as e:
            return {
                "component": "Model Performance",
                "status": "critical",
                "error": str(e)
            }
    
    async def monitor_feature_quality(self) -> Dict:
        """
        Monitor feature quality: null rate, distribution shift, staleness
        
        Based on: DQSOps framework
        """
        try:
            features = await self._list_feast_features()
            metrics = {}
            
            for feature in features:
                feature_quality = {
                    "null_rate_percent": await self._get_null_rate(feature),  # Should be < 1%
                    "distribution_shift_ks_pvalue": await self._detect_drift(feature),  # Should be > 0.05
                    "staleness_minutes": await self._get_staleness(feature),  # Should be < 5
                    "range_violations_percent": await self._check_range(feature),  # Should be = 0%
                }
                metrics[feature['name']] = feature_quality
            
            status = "healthy"
            for feature_name, quality_metrics in metrics.items():
                if quality_metrics['null_rate_percent'] > 5:
                    status = "degraded"
                if quality_metrics['distribution_shift_ks_pvalue'] < 0.01:
                    status = "degraded"  # Significant distribution shift
                if quality_metrics['staleness_minutes'] > 30:
                    status = "critical"
            
            return {
                "component": "Feature Quality",
                "status": status,
                "metrics": metrics
            }
        except Exception as e:
            return {
                "component": "Feature Quality",
                "status": "critical",
                "error": str(e)
            }
    
    # Helper methods (simplified implementations)
    async def _get_consumer_lag(self) -> float:
        """Get Kafka consumer lag"""
        return 100.0  # Placeholder
    
    async def _get_error_rate(self) -> float:
        """Get Kafka error rate"""
        return 0.0001  # Placeholder
    
    async def _get_throughput(self) -> float:
        """Get message throughput"""
        return 65000.0  # Placeholder
    
    async def _get_ca_lag(self) -> float:
        """Get continuous aggregate lag"""
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT EXTRACT(EPOCH FROM (NOW() - MAX(minute))) as lag_seconds
                    FROM ohlc_1m_agg
                """))
                row = result.fetchone()
                return row[0] if row and row[0] else 0.0
        except:
            return 0.0
    
    async def _get_query_latency(self) -> float:
        """Get P95 query latency"""
        return 50.0  # Placeholder
    
    async def _get_storage(self) -> float:
        """Get storage usage"""
        return 10.5  # Placeholder
    
    async def _get_compression(self) -> float:
        """Get compression ratio"""
        return 10.0  # Placeholder
    
    async def _list_flink_jobs(self) -> List[Dict]:
        """List Flink jobs"""
        return [{"name": "anomaly_detection"}]
    
    async def _get_checkpoint_success(self, job: Dict) -> float:
        """Get checkpoint success rate"""
        return 0.995  # Placeholder
    
    async def _get_e2e_latency(self, job: Dict) -> float:
        """Get end-to-end latency"""
        return 500.0  # Placeholder
    
    async def _get_backpressure(self, job: Dict) -> float:
        """Get backpressure percentage"""
        return 5.0  # Placeholder
    
    async def _get_state_size(self, job: Dict) -> float:
        """Get state size"""
        return 1024.0  # Placeholder
    
    async def _list_feast_features(self) -> List[Dict]:
        """List Feast features"""
        return [
            {"name": "ohlc_1m"},
            {"name": "sma_20"},
            {"name": "volatility_1h"}
        ]
    
    async def _get_mat_age(self, feature: Dict) -> float:
        """Get materialization age"""
        return 2.5  # Placeholder
    
    async def _validate_pit(self, feature: Dict) -> float:
        """Validate PIT correctness"""
        return 1.0  # Placeholder
    
    async def _get_online_freshness(self, feature: Dict) -> float:
        """Get online store freshness"""
        return 50.0  # Placeholder
    
    async def _get_dq_score(self, feature: Dict) -> float:
        """Get data quality score"""
        return 0.92  # Placeholder
    
    async def _get_shadow_pnl(self, model: str) -> float:
        """Get shadow model PnL"""
        return 1000.0  # Placeholder
    
    async def _get_canary_pnl(self) -> float:
        """Get canary model PnL"""
        return 1050.0  # Placeholder
    
    async def _get_pnl_diff_pct(self) -> float:
        """Get PnL difference percentage"""
        return 5.0  # Placeholder
    
    async def _get_latency_ratio(self) -> float:
        """Get latency ratio vs baseline"""
        return 1.1  # Placeholder
    
    async def _calculate_drift(self) -> float:
        """Calculate model drift score"""
        return 0.15  # Placeholder
    
    async def _get_null_rate(self, feature: Dict) -> float:
        """Get null rate percentage"""
        return 0.5  # Placeholder
    
    async def _detect_drift(self, feature: Dict) -> float:
        """Detect distribution drift"""
        return 0.08  # Placeholder
    
    async def _get_staleness(self, feature: Dict) -> float:
        """Get staleness in minutes"""
        return 3.0  # Placeholder
    
    async def _check_range(self, feature: Dict) -> float:
        """Check range violations"""
        return 0.0  # Placeholder
    
    def _aggregate_health(self, components: List[Dict]) -> Dict:
        """Aggregate component health into overall system health"""
        status_priority = {"critical": 3, "degraded": 2, "healthy": 1}
        worst_status = max([status_priority.get(c.get('status', 'unknown'), 0) for c in components])
        
        status_map = {1: "healthy", 2: "degraded", 3: "critical"}
        
        return {
            "overall_status": status_map.get(worst_status, "unknown"),
            "components": components,
            "timestamp": datetime.now().isoformat(),
            "recommendations": self._generate_recommendations(components)
        }
    
    def _generate_recommendations(self, components: List[Dict]) -> List[str]:
        """Generate automatic repair recommendations"""
        recommendations = []
        
        for component in components:
            status = component.get('status', 'unknown')
            comp_name = component.get('component', 'Unknown')
            
            if status == 'critical':
                if comp_name == 'Kafka':
                    recommendations.append("URGENT: Scale Kafka brokers, check consumer lag")
                elif comp_name == 'Flink Jobs':
                    recommendations.append("URGENT: Restart failed Flink jobs, check state backend")
                elif comp_name == 'Model Performance':
                    recommendations.append("URGENT: Trigger automatic model rollback!")
                elif comp_name == 'Feast Features':
                    recommendations.append("URGENT: Check PIT correctness, fix data leakage")
        
        return recommendations


class PrometheusExporter:
    """Export health metrics to Prometheus format"""
    
    @staticmethod
    def export_metrics(health_data: Dict) -> str:
        """
        Export health data as Prometheus metrics
        
        Args:
            health_data: Health monitoring data
            
        Returns:
            Prometheus metrics format string
        """
        lines = []
        
        # Overall status
        status_value = {"healthy": 1, "degraded": 0.5, "critical": 0}.get(
            health_data['overall_status'], 0
        )
        lines.append("# HELP pipeline_overall_status Overall pipeline health status")
        lines.append("# TYPE pipeline_overall_status gauge")
        lines.append(f"pipeline_overall_status {status_value}")
        
        # Component metrics
        for component in health_data.get('components', []):
            component_name = component.get('component', 'unknown').replace(" ", "_").lower()
            status_value = {"healthy": 1, "degraded": 0.5, "critical": 0}.get(
                component.get('status', 'unknown'), 0
            )
            
            lines.append(f"# HELP pipeline_{component_name}_status Component health status")
            lines.append(f"# TYPE pipeline_{component_name}_status gauge")
            lines.append(f"pipeline_{component_name}_status {status_value}")
            
            # Export individual metrics
            metrics = component.get('metrics', {})
            for metric_name, metric_value in metrics.items():
                if isinstance(metric_value, (int, float)):
                    metric_name_clean = metric_name.replace(" ", "_").replace("-", "_").lower()
                    lines.append(f"pipeline_{component_name}_{metric_name_clean} {metric_value}")
        
        return "\n".join(lines)


# Example usage
async def main():
    """Example usage"""
    monitor = PipelineHealthMonitor()
    health_data = await monitor.monitor_all_components()
    
    print("\n" + "="*80)
    print("Pipeline Health Report")
    print("="*80)
    print(f"Overall Status: {health_data['overall_status']}")
    print(f"Timestamp: {health_data['timestamp']}")
    
    print("\nComponent Status:")
    for component in health_data['components']:
        print(f"  {component.get('component', 'Unknown')}: {component.get('status', 'unknown')}")
    
    if health_data['recommendations']:
        print("\nRecommendations:")
        for rec in health_data['recommendations']:
            print(f"  - {rec}")
    
    # Export to Prometheus
    exporter = PrometheusExporter()
    prometheus_metrics = exporter.export_metrics(health_data)
    print("\n" + "="*80)
    print("Prometheus Metrics:")
    print("="*80)
    print(prometheus_metrics)


if __name__ == "__main__":
    asyncio.run(main())
