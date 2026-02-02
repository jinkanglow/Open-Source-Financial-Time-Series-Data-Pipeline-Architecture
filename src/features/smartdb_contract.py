"""
Smart-DB Contract Python Enforcement

This module enforces the Smart-DB Contract by providing:
1. Python classes for each of the 11 core features
2. SLA validation
3. PIT correctness checks
"""

from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class FeatureSLA(BaseModel):
    """SLA definition for a feature"""
    feature_name: str
    max_freshness_seconds: int
    description: str


class OHLC1M(BaseModel):
    """1-minute OHLC feature - SLA: ≤30s"""
    open: float
    high: float
    low: float
    close: float
    volume: float
    minute: datetime
    symbol: str
    
    @validator('minute')
    def check_freshness(cls, v):
        """Ensure data is fresh (within 30 seconds)"""
        if datetime.now() - v > timedelta(seconds=30):
            raise ValueError(f"OHLC data is stale: {datetime.now() - v} old")
        return v


class SMA20(BaseModel):
    """20-period Simple Moving Average - SLA: ≤2min"""
    sma_20: float
    minute: datetime
    symbol: str
    
    @validator('minute')
    def check_freshness(cls, v):
        """Ensure data is fresh (within 2 minutes)"""
        if datetime.now() - v > timedelta(seconds=120):
            raise ValueError(f"SMA20 data is stale: {datetime.now() - v} old")
        return v


class Volatility1H(BaseModel):
    """1-hour volatility - SLA: ≤2min"""
    volatility_1h: float
    hour: datetime
    symbol: str
    
    @validator('hour')
    def check_freshness(cls, v):
        """Ensure data is fresh (within 2 minutes)"""
        if datetime.now() - v > timedelta(seconds=120):
            raise ValueError(f"Volatility data is stale: {datetime.now() - v} old")
        return v


class VWAP5M(BaseModel):
    """5-minute VWAP - SLA: ≤30s"""
    vwap_5m: float
    five_min: datetime
    symbol: str
    
    @validator('five_min')
    def check_freshness(cls, v):
        """Ensure data is fresh (within 30 seconds)"""
        if datetime.now() - v > timedelta(seconds=30):
            raise ValueError(f"VWAP data is stale: {datetime.now() - v} old")
        return v


class FeaturePITSnapshot(BaseModel):
    """Point-in-time feature snapshot - all 11 features"""
    ohlc_1m_open: Optional[float] = None
    ohlc_1m_high: Optional[float] = None
    ohlc_1m_low: Optional[float] = None
    ohlc_1m_close: Optional[float] = None
    sma_20: Optional[float] = None
    ewm_12: Optional[float] = None
    volatility_1h: Optional[float] = None
    vwap_5m: Optional[float] = None
    large_trade_flag: Optional[bool] = None
    bidask_spread: Optional[float] = None
    trade_imbalance_5m: Optional[float] = None
    regime_tag: Optional[str] = None
    news_sentiment_embedding: Optional[list] = None
    snapshot_time: datetime
    symbol: str
    
    @validator('snapshot_time')
    def check_no_future_leakage(cls, v):
        """Ensure snapshot time is not in the future"""
        if v > datetime.now():
            raise ValueError(f"PIT snapshot time is in the future: {v}")
        return v


class SmartDBContract:
    """Smart-DB Contract enforcement class"""
    
    # Define SLA for all 11 features
    FEATURE_SLAS = {
        'ohlc_1m': FeatureSLA(
            feature_name='ohlc_1m',
            max_freshness_seconds=30,
            description='1-minute OHLC aggregate'
        ),
        'sma_20': FeatureSLA(
            feature_name='sma_20',
            max_freshness_seconds=120,
            description='20-period Simple Moving Average'
        ),
        'ewm_12': FeatureSLA(
            feature_name='ewm_12',
            max_freshness_seconds=120,
            description='12-period Exponential Moving Average'
        ),
        'volatility_1h': FeatureSLA(
            feature_name='volatility_1h',
            max_freshness_seconds=120,
            description='1-hour volatility'
        ),
        'vwap_5m': FeatureSLA(
            feature_name='vwap_5m',
            max_freshness_seconds=30,
            description='5-minute VWAP'
        ),
        'large_trade_flag': FeatureSLA(
            feature_name='large_trade_flag',
            max_freshness_seconds=30,
            description='Large trade flag'
        ),
        'bidask_spread': FeatureSLA(
            feature_name='bidask_spread',
            max_freshness_seconds=1,
            description='Bid-Ask Spread'
        ),
        'trade_imbalance_5m': FeatureSLA(
            feature_name='trade_imbalance_5m',
            max_freshness_seconds=30,
            description='5-minute trade imbalance'
        ),
        'regime_tag': FeatureSLA(
            feature_name='regime_tag',
            max_freshness_seconds=60,
            description='Market regime tag'
        ),
        'news_sentiment_embedding': FeatureSLA(
            feature_name='news_sentiment_embedding',
            max_freshness_seconds=300,
            description='News sentiment embedding (async)'
        ),
        'feature_pit_snapshot': FeatureSLA(
            feature_name='feature_pit_snapshot',
            max_freshness_seconds=0,
            description='Point-in-time snapshot (instant)'
        ),
    }
    
    def __init__(self, db_engine: Engine):
        """
        Initialize Smart-DB Contract with database connection
        
        Args:
            db_engine: SQLAlchemy engine connected to TimescaleDB
        """
        self.db_engine = db_engine
    
    def get_pit_snapshot(
        self, 
        symbol: str, 
        as_of_ts: datetime
    ) -> FeaturePITSnapshot:
        """
        Get point-in-time feature snapshot
        
        Args:
            symbol: Stock symbol
            as_of_ts: Point-in-time timestamp (must be <= now())
            
        Returns:
            FeaturePITSnapshot with all 11 features
            
        Raises:
            ValueError: If as_of_ts is in the future (future leakage)
        """
        if as_of_ts > datetime.now():
            raise ValueError(f"Cannot query future data: {as_of_ts} > {datetime.now()}")
        
        query = text("""
            SELECT * FROM feature_pit_snapshot(:symbol, :as_of_ts)
        """)
        
        with self.db_engine.connect() as conn:
            result = conn.execute(
                query,
                {"symbol": symbol, "as_of_ts": as_of_ts}
            )
            row = result.fetchone()
            
            if row is None:
                raise ValueError(f"No data found for {symbol} at {as_of_ts}")
            
            return FeaturePITSnapshot(
                ohlc_1m_open=row.ohlc_1m_open,
                ohlc_1m_high=row.ohlc_1m_high,
                ohlc_1m_low=row.ohlc_1m_low,
                ohlc_1m_close=row.ohlc_1m_close,
                sma_20=row.sma_20,
                ewm_12=row.ewm_12,
                volatility_1h=row.volatility_1h,
                vwap_5m=row.vwap_5m,
                large_trade_flag=row.large_trade_flag,
                bidask_spread=row.bidask_spread,
                trade_imbalance_5m=row.trade_imbalance_5m,
                regime_tag=row.regime_tag,
                news_sentiment_embedding=row.news_sentiment_embedding,
                snapshot_time=row.snapshot_time,
                symbol=symbol
            )
    
    def validate_sla(self, feature_name: str, last_update_time: datetime) -> bool:
        """
        Validate if a feature meets its SLA
        
        Args:
            feature_name: Name of the feature
            last_update_time: When the feature was last updated
            
        Returns:
            True if SLA is met, False otherwise
        """
        if feature_name not in self.FEATURE_SLAS:
            raise ValueError(f"Unknown feature: {feature_name}")
        
        sla = self.FEATURE_SLAS[feature_name]
        age_seconds = (datetime.now() - last_update_time).total_seconds()
        
        return age_seconds <= sla.max_freshness_seconds
    
    def check_all_features_fresh(self) -> Dict[str, bool]:
        """
        Check freshness of all 11 features
        
        Returns:
            Dictionary mapping feature names to freshness status
        """
        # This would query the database to get last update times
        # For now, return a placeholder structure
        return {
            feature_name: True  # Placeholder - implement actual DB queries
            for feature_name in self.FEATURE_SLAS.keys()
        }
