"""
S3 Lifecycle Policy Configuration

Implements storage tiering: Standard (30d) → Intelligent-Tiering (60d) → Glacier (>90d)
"""

import boto3
import json


def create_s3_lifecycle_policy(bucket_name: str):
    """
    Create S3 lifecycle policy for cost optimization
    
    Policy:
    - Standard (0-30 days)
    - Intelligent-Tiering (30-90 days)
    - Glacier (>90 days)
    - Delete (>730 days / 2 years)
    """
    s3_client = boto3.client('s3')
    
    lifecycle_configuration = {
        'Rules': [
            {
                'Id': 'MoveToIntelligentTieringAfter30Days',
                'Status': 'Enabled',
                'Filter': {
                    'Prefix': 'feature-store/'
                },
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'INTELLIGENT_TIERING'
                    }
                ]
            },
            {
                'Id': 'MoveToGlacierAfter90Days',
                'Status': 'Enabled',
                'Filter': {
                    'Prefix': 'feature-store/'
                },
                'Transitions': [
                    {
                        'Days': 90,
                        'StorageClass': 'GLACIER'
                    }
                ]
            },
            {
                'Id': 'DeleteAfter2Years',
                'Status': 'Enabled',
                'Filter': {
                    'Prefix': 'feature-store/'
                },
                'Expiration': {
                    'Days': 730  # 2 years
                }
            },
            {
                'Id': 'MoveRawDataToGlacier',
                'Status': 'Enabled',
                'Filter': {
                    'Prefix': 'raw/'
                },
                'Transitions': [
                    {
                        'Days': 90,
                        'StorageClass': 'GLACIER'
                    }
                ]
            }
        ]
    }
    
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_configuration
        )
        print(f"Lifecycle policy created for bucket: {bucket_name}")
        return lifecycle_configuration
    except Exception as e:
        print(f"Error creating lifecycle policy: {e}")
        raise


def get_storage_cost_estimate(bucket_name: str) -> dict:
    """
    Estimate storage costs based on lifecycle policy
    
    Returns:
        Cost breakdown by storage class
    """
    s3_client = boto3.client('s3')
    
    # Get bucket size by storage class (simplified)
    # In production, use CloudWatch metrics or S3 Inventory
    
    cost_estimate = {
        "standard": {
            "size_gb": 100,
            "cost_per_gb_month": 0.023,
            "monthly_cost": 2.30
        },
        "intelligent_tiering": {
            "size_gb": 200,
            "cost_per_gb_month": 0.023,  # Same as standard
            "monthly_cost": 4.60
        },
        "glacier": {
            "size_gb": 500,
            "cost_per_gb_month": 0.004,
            "monthly_cost": 2.00
        },
        "total_monthly_cost": 8.90
    }
    
    return cost_estimate


if __name__ == "__main__":
    # Example usage
    bucket_name = "financial-timeseries-feature-store"
    
    # Create lifecycle policy
    policy = create_s3_lifecycle_policy(bucket_name)
    print(f"Lifecycle policy: {json.dumps(policy, indent=2)}")
    
    # Get cost estimate
    cost = get_storage_cost_estimate(bucket_name)
    print(f"Storage cost estimate: {json.dumps(cost, indent=2)}")
