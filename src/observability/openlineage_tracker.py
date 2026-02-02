"""
OpenLineage Integration for Data Lineage Tracking

Tracks data lineage across the pipeline
Based on: OpenLineage specification
"""

from openlineage.client import OpenLineageClient
from openlineage.client.facet import (
    DataSourceDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
    LifecycleStateChange,
    LifecycleStateChangeFacet
)
from openlineage.client.run import Run, Job, RunEvent, RunState
from datetime import datetime
from typing import Dict, List, Optional
import uuid


class OpenLineageTracker:
    """Track data lineage using OpenLineage"""
    
    def __init__(self, marquez_url: str = "http://localhost:5000"):
        """
        Initialize OpenLineage tracker
        
        Args:
            marquez_url: Marquez API URL
        """
        self.client = OpenLineageClient(marquez_url)
        self.namespace = "financial-timeseries"
    
    def track_kafka_source(
        self,
        topic: str,
        schema: Dict,
        run_id: Optional[str] = None
    ):
        """Track Kafka topic as data source"""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        job = Job(
            namespace=self.namespace,
            name=f"kafka_source_{topic}"
        )
        
        run = Run(
            runId=run_id,
            facets={
                "source": DataSourceDatasetFacet(
                    name=topic,
                    uri=f"kafka://localhost:9092/{topic}"
                ),
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name=k, type=str(v))
                        for k, v in schema.items()
                    ]
                )
            }
        )
        
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(),
            run=run,
            job=job
        )
        
        self.client.emit(event)
        return run_id
    
    def track_timescale_sink(
        self,
        table: str,
        schema: Dict,
        source_run_id: str,
        run_id: Optional[str] = None
    ):
        """Track TimescaleDB table as data sink"""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        job = Job(
            namespace=self.namespace,
            name=f"timescale_sink_{table}"
        )
        
        run = Run(
            runId=run_id,
            facets={
                "destination": DataSourceDatasetFacet(
                    name=table,
                    uri=f"postgresql://timescaledb:5432/financial_db/{table}"
                ),
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name=k, type=str(v))
                        for k, v in schema.items()
                    ]
                )
            }
        )
        
        # Link to source
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(),
            run=run,
            job=job,
            inputs=[f"{self.namespace}:kafka_source_market-data"],
            outputs=[f"{self.namespace}:timescale_sink_{table}"]
        )
        
        self.client.emit(event)
        return run_id
    
    def track_flink_transformation(
        self,
        job_name: str,
        input_datasets: List[str],
        output_datasets: List[str],
        run_id: Optional[str] = None
    ):
        """Track Flink transformation"""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        job = Job(
            namespace=self.namespace,
            name=f"flink_{job_name}"
        )
        
        run = Run(
            runId=run_id,
            facets={
                "transformation": {
                    "type": "flink_streaming",
                    "job_name": job_name
                }
            }
        )
        
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(),
            run=run,
            job=job,
            inputs=[f"{self.namespace}:{ds}" for ds in input_datasets],
            outputs=[f"{self.namespace}:{ds}" for ds in output_datasets]
        )
        
        self.client.emit(event)
        return run_id
    
    def track_spark_transformation(
        self,
        job_name: str,
        input_datasets: List[str],
        output_datasets: List[str],
        run_id: Optional[str] = None
    ):
        """Track Spark transformation"""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        job = Job(
            namespace=self.namespace,
            name=f"spark_{job_name}"
        )
        
        run = Run(
            runId=run_id,
            facets={
                "transformation": {
                    "type": "spark_batch",
                    "job_name": job_name
                }
            }
        )
        
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(),
            run=run,
            job=job,
            inputs=[f"{self.namespace}:{ds}" for ds in input_datasets],
            outputs=[f"{self.namespace}:{ds}" for ds in output_datasets]
        )
        
        self.client.emit(event)
        return run_id
    
    def track_feast_materialization(
        self,
        feature_view: str,
        input_datasets: List[str],
        run_id: Optional[str] = None
    ):
        """Track Feast feature materialization"""
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        job = Job(
            namespace=self.namespace,
            name=f"feast_materialize_{feature_view}"
        )
        
        run = Run(
            runId=run_id,
            facets={
                "materialization": {
                    "type": "feast",
                    "feature_view": feature_view
                }
            }
        )
        
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(),
            run=run,
            job=job,
            inputs=[f"{self.namespace}:{ds}" for ds in input_datasets],
            outputs=[f"{self.namespace}:feast_online_{feature_view}"]
        )
        
        self.client.emit(event)
        return run_id
    
    def complete_run(self, run_id: str, job_name: str, state: RunState = RunState.COMPLETE):
        """Mark a run as complete"""
        job = Job(
            namespace=self.namespace,
            name=job_name
        )
        
        run = Run(runId=run_id)
        
        event = RunEvent(
            eventType=state,
            eventTime=datetime.now(),
            run=run,
            job=job
        )
        
        self.client.emit(event)


# Example usage
if __name__ == "__main__":
    tracker = OpenLineageTracker()
    
    # Track Kafka source
    kafka_run_id = tracker.track_kafka_source(
        topic="market-data",
        schema={"symbol": "string", "price": "double", "volume": "double"}
    )
    
    # Track Flink transformation
    flink_run_id = tracker.track_flink_transformation(
        job_name="anomaly_detection",
        input_datasets=["kafka_source_market-data"],
        output_datasets=["timescale_sink_bidask_spreads", "kafka_source_anomalies"]
    )
    
    # Track TimescaleDB sink
    timescale_run_id = tracker.track_timescale_sink(
        table="bidask_spreads",
        schema={"time": "timestamp", "symbol": "string", "bidask_spread": "double"},
        source_run_id=flink_run_id
    )
    
    # Complete runs
    tracker.complete_run(kafka_run_id, "kafka_source_market-data")
    tracker.complete_run(flink_run_id, "flink_anomaly_detection")
    tracker.complete_run(timescale_run_id, "timescale_sink_bidask_spreads")
