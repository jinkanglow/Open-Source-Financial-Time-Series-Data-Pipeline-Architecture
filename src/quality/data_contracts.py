"""
Great Expectations Data Contracts

Defines data quality checks and contracts
Based on: hoangsonww/End-to-End-Data-Pipeline
"""

import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.dataset import PandasDataset
import pandas as pd
from typing import Dict, Any, List


class DataContract:
    """Data contract definition for market data"""
    
    def __init__(self, contract_name: str):
        self.contract_name = contract_name
        self.expectations = []
    
    def add_schema_expectation(self, column: str, expected_type: str):
        """Add schema expectation"""
        self.expectations.append({
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": column}
        })
        self.expectations.append({
            "expectation_type": f"expect_column_values_to_be_of_type",
            "kwargs": {"column": column, "type_": expected_type}
        })
    
    def add_range_expectation(self, column: str, min_value: float = None, max_value: float = None):
        """Add value range expectation"""
        kwargs = {"column": column}
        if min_value is not None:
            kwargs["min_value"] = min_value
        if max_value is not None:
            kwargs["max_value"] = max_value
        
        self.expectations.append({
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": kwargs
        })
    
    def add_completeness_expectation(self, column: str, min_fraction: float = 0.95):
        """Add completeness expectation"""
        self.expectations.append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": column, "mostly": min_fraction}
        })
    
    def add_uniqueness_expectation(self, column: str):
        """Add uniqueness expectation"""
        self.expectations.append({
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": column}
        })


class MarketDataContract(DataContract):
    """Data contract for market data"""
    
    def __init__(self):
        super().__init__("market_data")
        self._define_expectations()
    
    def _define_expectations(self):
        """Define all expectations for market data"""
        # Schema expectations
        self.add_schema_expectation("symbol", "str")
        self.add_schema_expectation("price", "float")
        self.add_schema_expectation("volume", "float")
        self.add_schema_expectation("trade_id", "str")
        
        # Range expectations
        self.add_range_expectation("price", min_value=0.01, max_value=1000000.0)
        self.add_range_expectation("volume", min_value=0.0, max_value=1e12)
        
        # Completeness expectations
        self.add_completeness_expectation("symbol", min_fraction=1.0)
        self.add_completeness_expectation("price", min_fraction=1.0)
        self.add_completeness_expectation("volume", min_fraction=1.0)
        self.add_completeness_expectation("trade_id", min_fraction=1.0)
        
        # Uniqueness expectations
        self.add_uniqueness_expectation("trade_id")
        
        # Custom expectations
        self.expectations.append({
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "side",
                "value_set": ["buy", "sell", None]
            }
        })


class FeatureSLAExpectation:
    """SLA expectations for features"""
    
    @staticmethod
    def ohlc_freshness_expectation(max_age_seconds: int = 30):
        """Expect OHLC data to be fresh (within SLA)"""
        return {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "age_seconds",
                "min_value": 0,
                "max_value": max_age_seconds
            }
        }
    
    @staticmethod
    def sma_20_freshness_expectation(max_age_seconds: int = 120):
        """Expect SMA_20 data to be fresh"""
        return {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "age_seconds",
                "min_value": 0,
                "max_value": max_age_seconds
            }
        }


def validate_data_contract(df: pd.DataFrame, contract: DataContract) -> Dict[str, Any]:
    """
    Validate data against contract
    
    Args:
        df: DataFrame to validate
        contract: Data contract with expectations
        
    Returns:
        Validation result dictionary
    """
    ge_df = ge.from_pandas(df)
    
    results = {
        "contract_name": contract.contract_name,
        "total_expectations": len(contract.expectations),
        "passed": 0,
        "failed": 0,
        "details": []
    }
    
    for expectation in contract.expectations:
        expectation_type = expectation["expectation_type"]
        kwargs = expectation["kwargs"]
        
        try:
            result = getattr(ge_df, expectation_type)(**kwargs)
            if result.success:
                results["passed"] += 1
            else:
                results["failed"] += 1
                results["details"].append({
                    "expectation": expectation_type,
                    "kwargs": kwargs,
                    "success": False,
                    "result": result.result
                })
        except Exception as e:
            results["failed"] += 1
            results["details"].append({
                "expectation": expectation_type,
                "kwargs": kwargs,
                "success": False,
                "error": str(e)
            })
    
    results["success"] = results["failed"] == 0
    return results


def validate_feature_sla(df: pd.DataFrame, feature_name: str, max_age_seconds: int) -> bool:
    """
    Validate feature SLA freshness
    
    Args:
        df: Feature DataFrame with timestamp column
        feature_name: Name of the feature
        max_age_seconds: Maximum age in seconds
        
    Returns:
        True if SLA is met, False otherwise
    """
    if "timestamp" not in df.columns:
        return False
    
    from datetime import datetime
    now = datetime.now()
    
    # Calculate age for each row
    df["age_seconds"] = (now - pd.to_datetime(df["timestamp"])).dt.total_seconds()
    
    # Check if all rows meet SLA
    max_age = df["age_seconds"].max()
    return max_age <= max_age_seconds


def route_to_dlq(df: pd.DataFrame, contract: DataContract, dlq_path: str):
    """
    Route invalid data to Dead Letter Queue
    
    Args:
        df: DataFrame to validate
        contract: Data contract
        dlq_path: Path to DLQ (S3, Kafka topic, etc.)
    """
    validation_result = validate_data_contract(df, contract)
    
    if not validation_result["success"]:
        # In production, write to DLQ
        # For now, just log
        print(f"Data contract validation failed. Routing to DLQ: {dlq_path}")
        print(f"Failed expectations: {validation_result['failed']}")
        
        # Write invalid rows to DLQ
        # df_invalid.to_parquet(dlq_path)  # Example
        pass


if __name__ == "__main__":
    # Example usage
    contract = MarketDataContract()
    
    # Sample data
    sample_data = pd.DataFrame({
        "symbol": ["AAPL", "MSFT"],
        "price": [150.25, 300.50],
        "volume": [1000.0, 2000.0],
        "trade_id": ["TRADE-001", "TRADE-002"],
        "side": ["buy", "sell"]
    })
    
    result = validate_data_contract(sample_data, contract)
    print(f"Validation result: {result}")
