"""
Test Data Generator

Generates synthetic market data for testing
"""

import random
import time
from datetime import datetime, timedelta
from src.kafka.market_data_producer import MarketDataProducer


def generate_market_data(
    producer: MarketDataProducer,
    symbols: list,
    num_trades: int,
    duration_seconds: int = 60
):
    """
    Generate synthetic market data
    
    Args:
        producer: Kafka producer
        symbols: List of stock symbols
        num_trades: Number of trades to generate
        duration_seconds: Duration over which to spread trades
    """
    base_prices = {symbol: 100.0 + random.uniform(-50, 50) for symbol in symbols}
    base_time = datetime.now()
    interval = duration_seconds / num_trades
    
    for i in range(num_trades):
        symbol = random.choice(symbols)
        base_price = base_prices[symbol]
        
        # Random walk price
        price_change = random.uniform(-0.5, 0.5)
        price = base_price + price_change
        base_prices[symbol] = price
        
        # Generate trade
        volume = random.uniform(10, 1000)
        trade_id = f"TEST-{symbol}-{i:06d}"
        side = random.choice(['buy', 'sell'])
        
        # Bid-ask spread
        spread = random.uniform(0.01, 0.10)
        bid = price - spread / 2
        ask = price + spread / 2
        
        # Produce trade
        producer.produce_trade(
            symbol=symbol,
            price=price,
            volume=volume,
            trade_id=trade_id,
            side=side,
            bid=bid,
            ask=ask,
            source='test_generator'
        )
        
        # Small delay to simulate real-time
        time.sleep(interval)
    
    producer.flush()
    print(f"Generated {num_trades} trades for {len(symbols)} symbols")


def generate_large_trade(
    producer: MarketDataProducer,
    symbol: str,
    volume_multiplier: float = 10.0
):
    """
    Generate a large trade for testing large_trade_flag feature
    
    Args:
        producer: Kafka producer
        symbol: Stock symbol
        volume_multiplier: Multiplier for normal volume
    """
    base_price = 150.0
    normal_volume = 100.0
    large_volume = normal_volume * volume_multiplier
    
    producer.produce_trade(
        symbol=symbol,
        price=base_price,
        volume=large_volume,
        trade_id=f"LARGE-{symbol}-{int(time.time())}",
        side=random.choice(['buy', 'sell']),
        bid=base_price - 0.05,
        ask=base_price + 0.05,
        source='test_large_trade'
    )
    
    producer.flush()
    print(f"Generated large trade for {symbol}: volume={large_volume}")


if __name__ == "__main__":
    # Example usage
    producer = MarketDataProducer()
    
    # Generate test data
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    generate_market_data(producer, symbols, num_trades=1000, duration_seconds=60)
    
    # Generate large trade
    generate_large_trade(producer, 'AAPL', volume_multiplier=15.0)
