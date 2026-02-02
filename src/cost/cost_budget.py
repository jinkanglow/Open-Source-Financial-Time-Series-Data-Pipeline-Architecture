"""
Cost Budget and Auto-Scaling Configuration

Implements cost budget limits and auto-scaling constraints
Addresses: "成本爆炸 GPU/冷存储未限 → 成本 budget + auto-scaling 限制"
"""

import boto3
from typing import Dict, Optional
from datetime import datetime, timedelta
import json


class CostBudgetMonitor:
    """Monitor and enforce cost budgets"""
    
    def __init__(self, monthly_budget_usd: float = 3000.0):
        """
        Initialize cost budget monitor
        
        Args:
            monthly_budget_usd: Monthly budget in USD
        """
        self.monthly_budget = monthly_budget_usd
        self.current_month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0)
        self.cost_breakdown = {
            "timescaledb": 500.0,  # Hot storage
            "s3_standard": 50.0,    # Warm storage
            "s3_glacier": 10.0,     # Cold storage
            "flink": 800.0,         # Compute
            "spark": 600.0,         # Batch compute
            "kafka": 400.0,         # Streaming
            "gpu": 500.0,           # ML training/inference
            "other": 540.0          # Monitoring, etc.
        }
    
    def get_current_month_cost(self) -> float:
        """
        Get current month's cost
        
        In production, this would query AWS Cost Explorer API
        """
        # Placeholder: return estimated cost
        days_in_month = (datetime.now() - self.current_month_start).days
        daily_rate = sum(self.cost_breakdown.values()) / 30
        return daily_rate * days_in_month
    
    def check_budget_violation(self) -> Dict:
        """
        Check if current cost exceeds budget
        
        Returns:
            Dictionary with budget status
        """
        current_cost = self.get_current_month_cost()
        budget_utilization = (current_cost / self.monthly_budget) * 100
        
        return {
            "current_cost": current_cost,
            "budget": self.monthly_budget,
            "utilization_percent": budget_utilization,
            "violated": current_cost > self.monthly_budget,
            "remaining": self.monthly_budget - current_cost
        }
    
    def enforce_budget(self) -> Dict:
        """
        Enforce budget by scaling down resources
        
        Returns:
            Actions taken
        """
        budget_status = self.check_budget_violation()
        actions = []
        
        if budget_status["violated"]:
            # Scale down non-critical resources
            actions.append({
                "action": "scale_down_spark",
                "reason": "Budget exceeded",
                "target_replicas": 1
            })
            actions.append({
                "action": "scale_down_gpu",
                "reason": "Budget exceeded",
                "target_replicas": 0  # Stop GPU instances
            })
            actions.append({
                "action": "enable_s3_lifecycle",
                "reason": "Move to cheaper storage",
                "target_tier": "glacier"
            })
        
        return {
            "budget_status": budget_status,
            "actions": actions
        }


class AutoScalingLimits:
    """Define auto-scaling limits to prevent cost explosion"""
    
    def __init__(self):
        self.limits = {
            "flink_taskmanagers": {
                "min": 2,
                "max": 10,
                "target_cpu": 70,
                "target_memory": 80
            },
            "spark_executors": {
                "min": 2,
                "max": 20,
                "target_cpu": 70,
                "target_memory": 80
            },
            "gpu_instances": {
                "min": 0,
                "max": 4,  # Limit GPU instances
                "instance_type": "g4dn.xlarge",  # Use cheaper GPU instances
                "spot_enabled": True  # Use spot instances
            },
            "kafka_brokers": {
                "min": 3,
                "max": 6,
                "target_cpu": 70
            }
        }
    
    def get_scaling_policy(self, resource_type: str) -> Dict:
        """Get scaling policy for resource type"""
        return self.limits.get(resource_type, {})
    
    def validate_scaling_request(self, resource_type: str, target_replicas: int) -> bool:
        """Validate if scaling request is within limits"""
        policy = self.get_scaling_policy(resource_type)
        if not policy:
            return False
        
        return policy["min"] <= target_replicas <= policy["max"]


class S3LifecyclePolicy:
    """S3 lifecycle policy for cost optimization"""
    
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
    
    def create_lifecycle_policy(self):
        """
        Create S3 lifecycle policy:
        Standard (30d) → Intelligent-Tiering (60d) → Glacier (>90d)
        """
        lifecycle_config = {
            'Rules': [
                {
                    'Id': 'MoveToIntelligentTiering',
                    'Status': 'Enabled',
                    'Transitions': [
                        {
                            'Days': 30,
                            'StorageClass': 'INTELLIGENT_TIERING'
                        }
                    ],
                    'Filter': {
                        'Prefix': 'feature-store/'
                    }
                },
                {
                    'Id': 'MoveToGlacier',
                    'Status': 'Enabled',
                    'Transitions': [
                        {
                            'Days': 90,
                            'StorageClass': 'GLACIER'
                        }
                    ],
                    'Filter': {
                        'Prefix': 'feature-store/'
                    }
                },
                {
                    'Id': 'DeleteOldData',
                    'Status': 'Enabled',
                    'Expiration': {
                        'Days': 730  # 2 years
                    },
                    'Filter': {
                        'Prefix': 'feature-store/'
                    }
                }
            ]
        }
        
        self.s3_client.put_bucket_lifecycle_configuration(
            Bucket=self.bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        
        return lifecycle_config


# Kubernetes HPA configuration generator
def generate_hpa_config(resource_type: str, limits: AutoScalingLimits) -> Dict:
    """Generate Kubernetes HPA configuration"""
    policy = limits.get_scaling_policy(resource_type)
    
    return {
        "apiVersion": "autoscaling/v2",
        "kind": "HorizontalPodAutoscaler",
        "metadata": {
            "name": f"{resource_type}-hpa",
            "namespace": "financial-timeseries"
        },
        "spec": {
            "scaleTargetRef": {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": resource_type
            },
            "minReplicas": policy["min"],
            "maxReplicas": policy["max"],
            "metrics": [
                {
                    "type": "Resource",
                    "resource": {
                        "name": "cpu",
                        "target": {
                            "type": "Utilization",
                            "averageUtilization": policy["target_cpu"]
                        }
                    }
                },
                {
                    "type": "Resource",
                    "resource": {
                        "name": "memory",
                        "target": {
                            "type": "Utilization",
                            "averageUtilization": policy["target_memory"]
                        }
                    }
                }
            ]
        }
    }


if __name__ == "__main__":
    # Example usage
    budget_monitor = CostBudgetMonitor(monthly_budget_usd=3000.0)
    budget_status = budget_monitor.check_budget_violation()
    print(f"Budget status: {budget_status}")
    
    limits = AutoScalingLimits()
    flink_policy = limits.get_scaling_policy("flink_taskmanagers")
    print(f"Flink scaling policy: {flink_policy}")
    
    # Generate HPA config
    hpa_config = generate_hpa_config("flink-taskmanager", limits)
    print(f"HPA config: {json.dumps(hpa_config, indent=2)}")
