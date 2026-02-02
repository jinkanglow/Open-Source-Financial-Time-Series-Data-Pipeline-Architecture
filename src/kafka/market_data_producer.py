"""
Kafka Producer with Avro Schema Registry

Produces market data events to Kafka with schema validation
"""

import json
import time
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


class MarketDataProducer:
    """Kafka producer for market data with Avro schema"""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        schema_registry_url: str = 'http://localhost:8081',
        topic: str = 'market-data'
    ):
        """
        Initialize market data producer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            topic: Kafka topic name
        """
        self.topic = topic
        self.schema_registry_url = schema_registry_url
        
        # Load Avro schema
        schema_path = 'schemas/market_data.v1.avsc'
        with open(schema_path, 'r') as f:
            schema_str = f.read()
        self.schema = avro.schema.parse(schema_str)
        
        # Create Avro producer
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'schema.registry.url': schema_registry_url,
            'acks': 'all',  # Wait for all replicas
            'enable.idempotence': True,  # Exactly-once semantics
            'max.in.flight.requests.per.connection': 1,
            'retries': 3,
            'compression.type': 'snappy'
        }
        
        self.producer = AvroProducer(
            producer_config,
            default_key_schema=None,
            default_value_schema=self.schema
        )
    
    def produce_trade(
        self,
        symbol: str,
        price: float,
        volume: float,
        trade_id: str,
        side: str = None,
        bid: float = None,
        ask: float = None,
        source: str = None
    ):
        """
        Produce a trade event to Kafka
        
        Args:
            symbol: Stock symbol
            price: Trade price
            volume: Trade volume
            trade_id: Unique trade identifier
            side: Trade side (buy/sell)
            bid: Bid price
            ask: Ask price
            source: Data source identifier
        """
        value = {
            'time': int(datetime.now().timestamp() * 1000),
            'symbol': symbol,
            'price': str(price),  # Avro decimal as string
            'volume': str(volume),
            'trade_id': trade_id,
            'side': side,
            'bid': str(bid) if bid else None,
            'ask': str(ask) if ask else None,
            'source': source
        }
        
        try:
            self.producer.produce(
                topic=self.topic,
                value=value,
                key=symbol,  # Partition by symbol
                callback=self._delivery_callback
            )
            self.producer.poll(0)  # Trigger delivery callbacks
        except SerializerError as e:
            print(f"Serialization error: {e}")
            raise
    
    def _delivery_callback(self, err, msg):
        """Callback for message delivery"""
        if err:
            print(f'Message delivery failed: {err}')
            # In production, send to DLQ
            self._send_to_dlq(msg)
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
    
    def _send_to_dlq(self, msg):
        """Send failed message to Dead Letter Queue"""
        dlq_topic = f'{self.topic}-dlq'
        # Implement DLQ producer logic
        print(f'Sending to DLQ: {dlq_topic}')
    
    def flush(self):
        """Flush pending messages"""
        self.producer.flush()


class MarketDataConsumer:
    """Kafka consumer for market data with Avro schema"""
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        schema_registry_url: str = 'http://localhost:8081',
        topic: str = 'market-data',
        group_id: str = 'market-data-consumer'
    ):
        """
        Initialize market data consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL
            topic: Kafka topic name
            group_id: Consumer group ID
        """
        from confluent_kafka.avro import AvroConsumer
        
        consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'schema.registry.url': schema_registry_url,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # Manual commit for exactly-once
        }
        
        self.consumer = AvroConsumer(consumer_config)
        self.consumer.subscribe([topic])
    
    def consume(self, timeout: float = 1.0):
        """
        Consume messages from Kafka
        
        Args:
            timeout: Timeout in seconds
            
        Yields:
            Market data messages
        """
        while True:
            try:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                yield msg.value()
                # Commit offset after successful processing
                self.consumer.commit()
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error consuming message: {e}")
                # Send to DLQ on error
                continue
        
        self.consumer.close()


if __name__ == '__main__':
    # Example usage
    producer = MarketDataProducer()
    
    # Produce sample trades
    producer.produce_trade(
        symbol='AAPL',
        price=150.25,
        volume=100.0,
        trade_id='TRADE-001',
        side='buy',
        bid=150.20,
        ask=150.30,
        source='NYSE'
    )
    
    producer.flush()
