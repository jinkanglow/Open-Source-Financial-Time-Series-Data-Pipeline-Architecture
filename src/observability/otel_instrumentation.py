"""
OpenTelemetry Instrumentation for Financial Time-Series Pipeline

Provides distributed tracing and metrics collection
"""

import time
from contextlib import contextmanager
from typing import Optional, Dict, Any
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import get_meter_provider, set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.instrumentation.kafka import KafkaInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor


def setup_otel(
    service_name: str = "financial-timeseries-pipeline",
    otlp_endpoint: Optional[str] = None,
    enable_console: bool = False
):
    """
    Setup OpenTelemetry tracing and metrics
    
    Args:
        service_name: Service name for traces
        otlp_endpoint: OTLP endpoint (e.g., "http://jaeger:4317")
        enable_console: Enable console exporter for debugging
    """
    # Create resource
    resource = Resource.create({
        "service.name": service_name,
        "service.version": "2.1",
    })
    
    # Setup tracing
    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer = trace.get_tracer(__name__)
    
    # Add span processors
    if otlp_endpoint:
        otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(otlp_exporter)
        )
    
    if enable_console:
        console_exporter = ConsoleSpanExporter()
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(console_exporter)
        )
    
    # Setup metrics
    if otlp_endpoint:
        # Prometheus metrics
        reader = PrometheusMetricReader()
        set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))
    
    # Auto-instrument libraries
    KafkaInstrumentor().instrument()
    SQLAlchemyInstrumentor().instrument()
    RequestsInstrumentor().instrument()
    
    return tracer


class OTELInstrumentation:
    """Context manager for OpenTelemetry instrumentation"""
    
    def __init__(self, service_name: str = "financial-timeseries-pipeline"):
        self.service_name = service_name
        self.tracer = None
    
    def __enter__(self):
        self.tracer = setup_otel(service_name=self.service_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Cleanup if needed
        pass
    
    @contextmanager
    def span(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """
        Create a span context
        
        Args:
            name: Span name
            attributes: Span attributes
        """
        if self.tracer is None:
            self.tracer = trace.get_tracer(__name__)
        
        with self.tracer.start_as_current_span(name) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, str(value))
            yield span
    
    def record_latency(self, operation: str, latency_ms: float, **attributes):
        """
        Record operation latency
        
        Args:
            operation: Operation name
            latency_ms: Latency in milliseconds
            **attributes: Additional attributes
        """
        if self.tracer is None:
            return
        
        with self.span(f"{operation}_latency", attributes) as span:
            span.set_attribute("latency_ms", latency_ms)
            span.set_attribute("operation", operation)


# Flink-specific instrumentation
class FlinkOTELInstrumentation:
    """OpenTelemetry instrumentation for Flink jobs"""
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.tracer = setup_otel(service_name=f"flink-{job_name}")
    
    @contextmanager
    def state_read_span(self, state_name: str):
        """Instrument state read operations"""
        with self.tracer.start_as_current_span(
            f"flink.state.read",
            attributes={"state_name": state_name, "job": self.job_name}
        ) as span:
            start_time = time.time()
            try:
                yield
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
    
    @contextmanager
    def state_write_span(self, state_name: str):
        """Instrument state write operations"""
        with self.tracer.start_as_current_span(
            f"flink.state.write",
            attributes={"state_name": state_name, "job": self.job_name}
        ) as span:
            start_time = time.time()
            try:
                yield
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
    
    @contextmanager
    def emit_span(self, record_count: int):
        """Instrument record emission"""
        with self.tracer.start_as_current_span(
            f"flink.emit",
            attributes={"record_count": record_count, "job": self.job_name}
        ) as span:
            yield span


# Spark-specific instrumentation
class SparkOTELInstrumentation:
    """OpenTelemetry instrumentation for Spark jobs"""
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.tracer = setup_otel(service_name=f"spark-{job_name}")
    
    @contextmanager
    def transformation_span(self, transformation_name: str, input_rows: int):
        """Instrument Spark transformations"""
        with self.tracer.start_as_current_span(
            f"spark.transformation",
            attributes={
                "transformation": transformation_name,
                "input_rows": input_rows,
                "job": self.job_name
            }
        ) as span:
            start_time = time.time()
            try:
                yield
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
                # Output rows would be set after transformation
                span.set_attribute("output_rows", "unknown")


# Example usage
if __name__ == '__main__':
    # Setup instrumentation
    with OTELInstrumentation("test-service") as otel:
        # Create a span
        with otel.span("test_operation", {"key": "value"}) as span:
            time.sleep(0.1)
            span.set_attribute("result", "success")
        
        # Record latency
        otel.record_latency("database_query", 45.2, table="market_data")
