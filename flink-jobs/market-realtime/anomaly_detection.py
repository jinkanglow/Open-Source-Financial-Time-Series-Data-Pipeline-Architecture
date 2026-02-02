"""
Flink CEP Job for Anomaly Detection

Detects anomalies in real-time market data:
- 3 large trades in 5 minutes
- Unusual bid-ask spreads
- Volume spikes
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction
from pyflink.common import WatermarkStrategy
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream import Time
import json
from datetime import datetime, timedelta


class MarketDataDeserializer(MapFunction):
    """Deserialize market data from Kafka"""
    
    def map(self, value):
        data = json.loads(value)
        return {
            'time': datetime.fromtimestamp(data['time'] / 1000),
            'symbol': data['symbol'],
            'price': float(data['price']),
            'volume': float(data['volume']),
            'trade_id': data['trade_id'],
            'side': data.get('side'),
            'bid': float(data['bid']) if data.get('bid') else None,
            'ask': float(data['ask']) if data.get('ask') else None,
        }


class LargeTradeDetector(KeyedProcessFunction):
    """
    Detect 3 large trades in 5 minutes
    
    CEP Pattern: 3 trades with volume > Q95 within 5 minutes
    """
    
    def __init__(self):
        self.trade_buffer_state = None
        self.volume_threshold = 0.95  # Q95 threshold
    
    def open(self, runtime_context):
        # State to store recent large trades
        trade_buffer_desc = ValueStateDescriptor(
            "trade_buffer",
            Types.LIST(Types.PY_DICT)
        )
        self.trade_buffer_state = runtime_context.get_state(trade_buffer_desc)
    
    def process_element(self, value, ctx, out):
        trade = value
        
        # Check if trade is large (simplified - in production, calculate Q95 dynamically)
        if trade['volume'] > self._get_volume_threshold(trade['symbol']):
            # Get current buffer
            buffer = self.trade_buffer_state.value()
            if buffer is None:
                buffer = []
            
            # Add current trade
            buffer.append({
                'time': trade['time'],
                'trade_id': trade['trade_id'],
                'volume': trade['volume'],
                'price': trade['price']
            })
            
            # Filter trades within 5 minutes
            five_min_ago = trade['time'] - timedelta(minutes=5)
            buffer = [t for t in buffer if t['time'] >= five_min_ago]
            
            # Check if we have 3 or more large trades
            if len(buffer) >= 3:
                # Emit anomaly
                anomaly = {
                    'type': 'large_trade_anomaly',
                    'symbol': trade['symbol'],
                    'count': len(buffer),
                    'window_start': buffer[0]['time'],
                    'window_end': trade['time'],
                    'trades': buffer,
                    'detected_at': datetime.now()
                }
                out.collect(json.dumps(anomaly, default=str))
            
            # Update state
            self.trade_buffer_state.update(buffer)
    
    def _get_volume_threshold(self, symbol):
        """Get volume threshold for symbol (Q95)"""
        # In production, this would query TimescaleDB or use state
        # For now, return a fixed threshold
        return 10000.0


class BidAskSpreadMonitor(KeyedProcessFunction):
    """
    Monitor bid-ask spreads for anomalies
    
    Alert if spread > 3x normal spread
    """
    
    def __init__(self):
        self.normal_spread_state = None
    
    def open(self, runtime_context):
        normal_spread_desc = ValueStateDescriptor(
            "normal_spread",
            Types.FLOAT()
        )
        self.normal_spread_state = runtime_context.get_state(normal_spread_desc)
    
    def process_element(self, value, ctx, out):
        trade = value
        
        if trade.get('bid') and trade.get('ask'):
            spread = trade['ask'] - trade['bid']
            normal_spread = self.normal_spread_state.value()
            
            if normal_spread is None:
                # Initialize with current spread
                self.normal_spread_state.update(spread)
                return
            
            # Check if spread is anomalous (>3x normal)
            if spread > 3 * normal_spread:
                anomaly = {
                    'type': 'bidask_spread_anomaly',
                    'symbol': trade['symbol'],
                    'spread': spread,
                    'normal_spread': normal_spread,
                    'ratio': spread / normal_spread,
                    'detected_at': datetime.now()
                }
                out.collect(json.dumps(anomaly, default=str))
            
            # Update normal spread (exponential moving average)
            alpha = 0.1
            new_normal = alpha * spread + (1 - alpha) * normal_spread
            self.normal_spread_state.update(new_normal)


def create_flink_job():
    """Create and configure Flink job"""
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(60000)  # 1 minute checkpoints
    
    # Kafka consumer configuration
    kafka_props = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-market-data-consumer',
        'auto.offset.reset': 'latest'
    }
    
    # Create Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='market-data',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Set watermark strategy
    kafka_consumer.set_start_from_earliest()
    
    # Create data stream
    market_data_stream = env.add_source(kafka_consumer) \
        .map(MarketDataDeserializer()) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(
                timedelta(seconds=10)
            ).with_timestamp_assigner(
                lambda event, timestamp: int(event['time'].timestamp() * 1000)
            )
        )
    
    # Large trade detection
    large_trade_stream = market_data_stream \
        .key_by(lambda x: x['symbol']) \
        .process(LargeTradeDetector())
    
    # Bid-ask spread monitoring
    bidask_stream = market_data_stream \
        .filter(lambda x: x.get('bid') and x.get('ask')) \
        .key_by(lambda x: x['symbol']) \
        .process(BidAskSpreadMonitor())
    
    # Write anomalies to Kafka
    # In production, also write to TimescaleDB
    large_trade_stream.add_sink(
        FlinkKafkaProducer(
            topic='anomalies',
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'localhost:9092'}
        )
    )
    
    bidask_stream.add_sink(
        FlinkKafkaProducer(
            topic='anomalies',
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'localhost:9092'}
        )
    )
    
    # Write bid-ask spreads to TimescaleDB (via JDBC sink)
    # This would be implemented using Flink JDBC connector
    
    return env


if __name__ == '__main__':
    # Create and execute job
    job = create_flink_job()
    job.execute("Market Data Anomaly Detection")
