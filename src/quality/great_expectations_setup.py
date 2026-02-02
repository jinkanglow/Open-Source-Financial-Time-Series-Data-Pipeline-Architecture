"""
Great Expectations + Data Quality Monitoring Framework

Based on:
- "DataLens: ML-Oriented Interactive Tabular Data Quality Dashboard" (arXiv 2025-01-28)
- "DQSOps: Data Quality Scoring Operations Framework" (arXiv 2023-03-27)
- "Enhancing ML Performance through Intelligent Data Quality Assessment" (arXiv 2025-02-18)
- "A Data Quality-Driven View of MLOps" (arXiv 2021-02-15)
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy import stats
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset


class DataQualityExpectations:
    """Define data quality expectations using Great Expectations"""
    
    @staticmethod
    def get_ohlc_expectations() -> Dict:
        """Expectations for OHLC_1m aggregate"""
        return {
            "open > 0": {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "open",
                    "min_value": 0.01,
                    "max_value": 999999
                }
            },
            "volume >= 0": {
                "expectation_type": "expect_column_values_to_be_nonnegative",
                "kwargs": {"column": "volume"}
            },
            "high >= low": {
                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                "kwargs": {
                    "column_A": "high",
                    "column_B": "low"
                }
            },
            "no_nulls": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "close"}
            },
            "timestamp_ordered": {
                "expectation_type": "expect_column_values_to_be_increasing",
                "kwargs": {"column": "minute"}
            }
        }
    
    @staticmethod
    def get_sma_expectations() -> Dict:
        """Expectations for SMA_20"""
        return {
            "sma_not_null": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "sma_20"}
            },
            "sma_in_range": {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "sma_20",
                    "min_value": None,  # Will be calculated dynamically
                    "max_value": None
                }
            }
        }
    
    @staticmethod
    def get_volatility_expectations() -> Dict:
        """Expectations for volatility_1h"""
        return {
            "volatility >= 0": {
                "expectation_type": "expect_column_values_to_be_nonnegative",
                "kwargs": {"column": "volatility_1h"}
            },
            "volatility < 5": {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "volatility_1h",
                    "min_value": 0,
                    "max_value": 5.0  # Financial sanity check
                }
            }
        }


def validate_pit_quality(features_df: pd.DataFrame, reference_timestamp: datetime) -> bool:
    """
    Ensure Feast features have no future data leakage
    
    Based on: "Fix your Models by Fixing your Datasets" (arXiv 2021-12-14)
    
    Args:
        features_df: DataFrame with features
        reference_timestamp: Reference timestamp for PIT query
        
    Returns:
        True if no future data leak detected
        
    Raises:
        AssertionError: If future data leak detected
    """
    if 'event_timestamp' not in features_df.columns:
        return True
    
    future_data = features_df[features_df['event_timestamp'] > reference_timestamp]
    
    if len(future_data) > 0:
        raise AssertionError(
            f"Future data leak detected: {len(future_data)} rows with timestamps "
            f"after {reference_timestamp}"
        )
    
    return True


class DataQualityScore:
    """
    Data Quality Scoring using DQSOps framework
    
    Score = (completeness + consistency + accuracy + timeliness) / 4
    """
    
    def __init__(self, features_df: pd.DataFrame):
        """
        Initialize quality scorer
        
        Args:
            features_df: DataFrame to score
        """
        self.df = features_df.copy()
    
    def calculate_score(self) -> Dict[str, float]:
        """
        Calculate overall data quality score
        
        Returns:
            Dictionary with overall score and component scores
        """
        completeness = self._check_completeness_score()
        consistency = self._check_consistency_score()
        accuracy = self._check_accuracy_score()
        timeliness = self._check_timeliness_score()
        
        final_score = (completeness + consistency + accuracy + timeliness) / 4
        
        return {
            "overall": final_score,
            "completeness": completeness,
            "consistency": consistency,
            "accuracy": accuracy,
            "timeliness": timeliness,
            "timestamp": datetime.now().isoformat()
        }
    
    def _check_completeness_score(self) -> float:
        """Check data completeness (1 - null rate)"""
        if len(self.df) == 0:
            return 0.0
        
        total_cells = len(self.df) * len(self.df.columns)
        null_cells = self.df.isnull().sum().sum()
        
        completeness = 1 - (null_cells / total_cells) if total_cells > 0 else 0.0
        return max(0.0, min(1.0, completeness))
    
    def _check_consistency_score(self) -> float:
        """Check data consistency (e.g., high >= low >= close)"""
        if len(self.df) == 0:
            return 0.0
        
        violations = 0
        total_checks = 0
        
        # Check high >= low
        if 'high' in self.df.columns and 'low' in self.df.columns:
            violations += (self.df['high'] < self.df['low']).sum()
            total_checks += len(self.df)
        
        # Check low <= close (if close exists)
        if 'low' in self.df.columns and 'close' in self.df.columns:
            violations += (self.df['low'] > self.df['close']).sum()
            total_checks += len(self.df)
        
        if total_checks == 0:
            return 1.0
        
        consistency = 1 - (violations / total_checks)
        return max(0.0, min(1.0, consistency))
    
    def _check_accuracy_score(self) -> float:
        """Check data accuracy (outlier detection using z-score)"""
        if len(self.df) == 0 or 'close' not in self.df.columns:
            return 1.0
        
        try:
            close_values = self.df['close'].dropna()
            if len(close_values) < 3:
                return 1.0
            
            z_scores = np.abs(stats.zscore(close_values))
            outliers = (z_scores > 3).sum()
            
            accuracy = 1 - (outliers / len(close_values))
            return max(0.0, min(1.0, accuracy))
        except Exception:
            return 1.0
    
    def _check_timeliness_score(self) -> float:
        """Check data timeliness (freshness)"""
        if len(self.df) == 0:
            return 0.0
        
        # Check for timestamp column
        timestamp_cols = ['timestamp', 'event_timestamp', 'minute', 'time']
        timestamp_col = None
        
        for col in timestamp_cols:
            if col in self.df.columns:
                timestamp_col = col
                break
        
        if timestamp_col is None:
            return 1.0
        
        try:
            last_timestamp = pd.to_datetime(self.df[timestamp_col]).max()
            age_minutes = (datetime.now() - last_timestamp.to_pydatetime()).total_seconds() / 60
            
            # SLA: 5 minutes
            if age_minutes > 30:
                return 0.0
            elif age_minutes > 5:
                return 0.5
            else:
                return 1.0
        except Exception:
            return 1.0


class DataQualityAlerts:
    """Automatic alerting for data quality issues"""
    
    def __init__(self, threshold_score: float = 0.85):
        """
        Initialize alert system
        
        Args:
            threshold_score: Minimum acceptable quality score
        """
        self.threshold = threshold_score
        self.alert_history = []
    
    def check_and_alert(
        self,
        features_df: pd.DataFrame,
        symbol: str,
        send_alert: bool = True
    ) -> Dict:
        """
        Check data quality and send alerts if needed
        
        Args:
            features_df: DataFrame to check
            symbol: Symbol identifier
            send_alert: Whether to send alert
            
        Returns:
            Quality score dictionary
        """
        dq_score = DataQualityScore(features_df).calculate_score()
        
        if dq_score['overall'] < self.threshold:
            alert = {
                "severity": "critical" if dq_score['overall'] < 0.7 else "warning",
                "symbol": symbol,
                "score": dq_score['overall'],
                "timestamp": datetime.now().isoformat(),
                "issues": []
            }
            
            if dq_score['completeness'] < 0.95:
                alert['issues'].append(
                    f"Missing data: {(1-dq_score['completeness'])*100:.2f}%"
                )
            if dq_score['consistency'] < 0.98:
                alert['issues'].append(
                    f"Consistency violations: {(1-dq_score['consistency'])*100:.2f}%"
                )
            if dq_score['accuracy'] < 0.95:
                alert['issues'].append(
                    f"Outliers detected: {(1-dq_score['accuracy'])*100:.2f}%"
                )
            if dq_score['timeliness'] < 1.0:
                alert['issues'].append("Data staleness issue")
            
            self.alert_history.append(alert)
            
            if send_alert:
                self._send_alert(alert)
        
        return dq_score
    
    def _send_alert(self, alert: Dict):
        """Send alert to notification system"""
        # TODO: Integrate with Slack/PagerDuty/email
        print(f"🚨 ALERT [{alert['severity'].upper()}]: {alert['symbol']}")
        print(f"   Quality Score: {alert['score']:.3f}")
        print(f"   Issues: {', '.join(alert['issues'])}")
        print(f"   Timestamp: {alert['timestamp']}")


def detect_distribution_shift(
    baseline_dist: np.ndarray,
    current_dist: np.ndarray
) -> Dict[str, float]:
    """
    Detect distribution shift using Kolmogorov-Smirnov test
    
    Args:
        baseline_dist: Baseline distribution
        current_dist: Current distribution
        
    Returns:
        Dictionary with KS statistic and p-value
    """
    ks_stat, p_value = stats.ks_2samp(baseline_dist, current_dist)
    
    return {
        "ks_statistic": ks_stat,
        "p_value": p_value,
        "shift_detected": p_value < 0.05,
        "severity": "critical" if p_value < 0.01 else "warning" if p_value < 0.05 else "none"
    }


def ci_validate_features(
    features_df: pd.DataFrame,
    reference_timestamp: datetime,
    baseline_distribution: Optional[np.ndarray] = None
) -> bool:
    """
    CI/CD validation function for features
    
    Runs all quality checks and raises AssertionError if any fail
    
    Args:
        features_df: Features DataFrame
        reference_timestamp: Reference timestamp for PIT validation
        baseline_distribution: Baseline distribution for drift detection
        
    Returns:
        True if all checks pass
        
    Raises:
        AssertionError: If any quality check fails
    """
    # 1. PIT correctness test
    validate_pit_quality(features_df, reference_timestamp)
    
    # 2. Quality scoring
    dq_score = DataQualityScore(features_df).calculate_score()
    assert dq_score['overall'] > 0.85, \
        f"Data quality score too low: {dq_score['overall']:.3f}"
    
    # 3. Distribution shift detection
    if baseline_distribution is not None and 'close' in features_df.columns:
        current_dist = features_df['close'].dropna().values
        if len(current_dist) > 0:
            drift_result = detect_distribution_shift(baseline_distribution, current_dist)
            assert drift_result['p_value'] > 0.05, \
                f"Data distribution shift detected: p={drift_result['p_value']:.4f}"
    
    print("✅ All data quality checks passed!")
    return True


# Example usage
if __name__ == "__main__":
    # Create sample data
    sample_data = pd.DataFrame({
        'symbol': ['AAPL'] * 100,
        'close': np.random.normal(150, 5, 100),
        'high': np.random.normal(152, 5, 100),
        'low': np.random.normal(148, 5, 100),
        'volume': np.random.normal(1000000, 100000, 100),
        'timestamp': pd.date_range(end=datetime.now(), periods=100, freq='1min')
    })
    
    # Calculate quality score
    scorer = DataQualityScore(sample_data)
    score = scorer.calculate_score()
    print(f"Data Quality Score: {score}")
    
    # Check and alert
    alert_system = DataQualityAlerts(threshold_score=0.85)
    alert_system.check_and_alert(sample_data, symbol='AAPL')
