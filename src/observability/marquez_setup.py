"""
Marquez Configuration and Setup

OpenLineage backend for data lineage tracking
Addresses: P2.3 OpenLineage + Marquez
"""

from marquez_client import MarquezClient
from marquez_client.models import (
    JobType,
    RunState,
    DatasetType,
    Dataset,
    Job,
    Run
)
from datetime import datetime
from typing import Dict, List, Optional


class MarquezClientWrapper:
    """Wrapper for Marquez client with convenience methods"""
    
    def __init__(self, marquez_url: str = "http://marquez:5000"):
        """
        Initialize Marquez client
        
        Args:
            marquez_url: Marquez API URL
        """
        self.client = MarquezClient(url=marquez_url)
        self.namespace = "financial-timeseries"
    
    def create_namespace(self, owner: str = "data-engineering"):
        """Create namespace if it doesn't exist"""
        try:
            self.client.create_namespace(
                namespace_name=self.namespace,
                owner_name=owner
            )
        except Exception as e:
            # Namespace might already exist
            print(f"Namespace creation: {e}")
    
    def create_dataset(
        self,
        dataset_name: str,
        dataset_type: DatasetType = DatasetType.DB_TABLE,
        physical_name: str = None,
        source_name: str = None,
        fields: List[Dict] = None
    ):
        """
        Create dataset in Marquez
        
        Args:
            dataset_name: Name of the dataset
            dataset_type: Type of dataset
            physical_name: Physical name (e.g., table name)
            source_name: Source name (e.g., database name)
            fields: List of field definitions
        """
        dataset = Dataset(
            id=dataset_name,
            type=dataset_type,
            name=dataset_name,
            physical_name=physical_name or dataset_name,
            source_name=source_name or "timescaledb",
            fields=fields or []
        )
        
        self.client.create_dataset(
            namespace_name=self.namespace,
            dataset_name=dataset_name,
            dataset=dataset
        )
    
    def create_job(
        self,
        job_name: str,
        job_type: JobType = JobType.BATCH,
        inputs: List[str] = None,
        outputs: List[str] = None,
        location: str = None
    ):
        """
        Create job in Marquez
        
        Args:
            job_name: Name of the job
            job_type: Type of job (BATCH, STREAM, SERVICE)
            inputs: List of input dataset names
            outputs: List of output dataset names
            location: Location of the job (e.g., GitHub URL)
        """
        job = Job(
            id=job_name,
            type=job_type,
            name=job_name,
            inputs=[f"{self.namespace}:{ds}" for ds in (inputs or [])],
            outputs=[f"{self.namespace}:{ds}" for ds in (outputs or [])],
            location=location
        )
        
        self.client.create_job(
            namespace_name=self.namespace,
            job_name=job_name,
            job=job
        )
    
    def start_run(
        self,
        job_name: str,
        run_id: str = None,
        inputs: List[str] = None,
        outputs: List[str] = None
    ) -> str:
        """
        Start a run
        
        Returns:
            Run ID
        """
        import uuid
        if run_id is None:
            run_id = str(uuid.uuid4())
        
        run = Run(
            id=run_id,
            job_name=job_name,
            state=RunState.RUNNING,
            started_at=datetime.now(),
            inputs=[f"{self.namespace}:{ds}" for ds in (inputs or [])],
            outputs=[f"{self.namespace}:{ds}" for ds in (outputs or [])]
        )
        
        self.client.create_job_run(
            namespace_name=self.namespace,
            job_name=job_name,
            run=run
        )
        
        return run_id
    
    def complete_run(self, job_name: str, run_id: str, state: RunState = RunState.COMPLETE):
        """Mark a run as complete"""
        run = Run(
            id=run_id,
            job_name=job_name,
            state=state,
            ended_at=datetime.now()
        )
        
        self.client.mark_job_run_as(
            namespace_name=self.namespace,
            job_name=job_name,
            run_id=run_id,
            run=run
        )


def setup_marquez_lineage():
    """Setup complete data lineage in Marquez"""
    marquez = MarquezClientWrapper()
    
    # Create namespace
    marquez.create_namespace()
    
    # Create datasets
    marquez.create_dataset(
        dataset_name="market_data_raw",
        physical_name="market_data_raw",
        source_name="timescaledb",
        fields=[
            {"name": "symbol", "type": "VARCHAR"},
            {"name": "price", "type": "NUMERIC"},
            {"name": "volume", "type": "NUMERIC"},
            {"name": "trade_id", "type": "VARCHAR"}
        ]
    )
    
    marquez.create_dataset(
        dataset_name="ohlc_1m_agg",
        physical_name="ohlc_1m_agg",
        source_name="timescaledb",
        fields=[
            {"name": "minute", "type": "TIMESTAMP"},
            {"name": "symbol", "type": "VARCHAR"},
            {"name": "open", "type": "NUMERIC"},
            {"name": "high", "type": "NUMERIC"},
            {"name": "low", "type": "NUMERIC"},
            {"name": "close", "type": "NUMERIC"}
        ]
    )
    
    # Create jobs
    marquez.create_job(
        job_name="flink_anomaly_detection",
        job_type=JobType.STREAM,
        inputs=["market_data_raw"],
        outputs=["bidask_spreads", "anomalies"]
    )
    
    marquez.create_job(
        job_name="spark_batch_features",
        job_type=JobType.BATCH,
        inputs=["market_data_raw"],
        outputs=["ohlc_1m_agg", "sma_20_calc", "volatility_1h_agg"]
    )
    
    marquez.create_job(
        job_name="feast_materialization",
        job_type=JobType.BATCH,
        inputs=["ohlc_1m_agg", "sma_20_calc"],
        outputs=["feast_online_store"]
    )


if __name__ == "__main__":
    # Setup lineage
    setup_marquez_lineage()
    print("Marquez lineage setup complete!")
