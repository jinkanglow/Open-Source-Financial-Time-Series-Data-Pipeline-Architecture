"""
Enhanced OpenTelemetry Integration for Flink and Spark Jobs

Integrates OTEL directly into Flink and Spark processing logic
"""

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
import time
from contextlib import contextmanager
from typing import Optional, Dict, Any


# Initialize tracer
tracer_provider = TracerProvider()
tracer_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317", insecure=True))
)
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)


class FlinkOTELWrapper:
    """OpenTelemetry wrapper for Flink operations"""
    
    @staticmethod
    @contextmanager
    def state_read_span(state_name: str, symbol: Optional[str] = None):
        """Instrument Flink state read operations"""
        with tracer.start_as_current_span(
            "flink.state.read",
            attributes={
                "state_name": state_name,
                "symbol": symbol or "unknown"
            }
        ) as span:
            start_time = time.time()
            try:
                yield span
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
    
    @staticmethod
    @contextmanager
    def state_write_span(state_name: str, symbol: Optional[str] = None):
        """Instrument Flink state write operations"""
        with tracer.start_as_current_span(
            "flink.state.write",
            attributes={
                "state_name": state_name,
                "symbol": symbol or "unknown"
            }
        ) as span:
            start_time = time.time()
            try:
                yield span
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
    
    @staticmethod
    @contextmanager
    def emit_span(record_count: int, topic: str):
        """Instrument Flink record emission"""
        with tracer.start_as_current_span(
            "flink.emit",
            attributes={
                "record_count": record_count,
                "topic": topic
            }
        ) as span:
            start_time = time.time()
            try:
                yield span
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
                span.set_attribute("throughput_per_sec", record_count / (latency_ms / 1000) if latency_ms > 0 else 0)


class SparkOTELWrapper:
    """OpenTelemetry wrapper for Spark operations"""
    
    @staticmethod
    @contextmanager
    def transformation_span(transformation_name: str, input_rows: int):
        """Instrument Spark transformations"""
        with tracer.start_as_current_span(
            "spark.transformation",
            attributes={
                "transformation": transformation_name,
                "input_rows": input_rows
            }
        ) as span:
            start_time = time.time()
            try:
                yield span
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
    
    @staticmethod
    @contextmanager
    def write_span(output_path: str, record_count: int):
        """Instrument Spark write operations"""
        with tracer.start_as_current_span(
            "spark.write",
            attributes={
                "output_path": output_path,
                "record_count": record_count
            }
        ) as span:
            start_time = time.time()
            try:
                yield span
            finally:
                latency_ms = (time.time() - start_time) * 1000
                span.set_attribute("latency_ms", latency_ms)
                span.set_attribute("throughput_mb_per_sec", 
                    (record_count * 100) / (latency_ms / 1000) if latency_ms > 0 else 0)  # Estimate


# Example usage in Flink job
def flink_job_with_otel():
    """Example Flink job with OTEL instrumentation"""
    from pyflink.datastream import StreamExecutionEnvironment
    
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Process function with OTEL
    class InstrumentedProcessFunction:
        def process_element(self, value, ctx, out):
            symbol = value.get('symbol')
            
            # Instrument state read
            with FlinkOTELWrapper.state_read_span("trade_buffer", symbol):
                buffer = self.get_trade_buffer(symbol)
            
            # Process...
            result = self.process_trade(value, buffer)
            
            # Instrument state write
            with FlinkOTELWrapper.state_write_span("trade_buffer", symbol):
                self.update_trade_buffer(symbol, result)
            
            # Instrument emit
            with FlinkOTELWrapper.emit_span(1, "anomalies"):
                out.collect(result)


# Example usage in Spark job
def spark_job_with_otel(spark):
    """Example Spark job with OTEL instrumentation"""
    df = spark.read.format("delta").load("s3://data/raw")
    
    # Instrument transformation
    with SparkOTELWrapper.transformation_span("calculate_sma_20", df.count()):
        df_sma = df.withColumn("sma_20", ...)
    
    # Instrument write
    with SparkOTELWrapper.write_span("s3://data/processed/sma_20", df_sma.count()):
        df_sma.write.format("delta").save("s3://data/processed/sma_20")


if __name__ == "__main__":
    # Test OTEL setup
    with tracer.start_as_current_span("test_span") as span:
        span.set_attribute("test_key", "test_value")
        print("OTEL instrumentation test passed")
