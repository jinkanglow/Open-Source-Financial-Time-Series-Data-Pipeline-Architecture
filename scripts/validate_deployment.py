"""
Deployment Validation Script

Validates that all components are properly configured and ready for deployment
"""

import sys
import os
from typing import List, Tuple
from src.config.settings import config
from src.utils.helpers import validate_environment, check_feature_freshness
from sqlalchemy import create_engine, text


class DeploymentValidator:
    """Validates deployment readiness"""
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def validate_database(self) -> bool:
        """Validate database connection and schema"""
        try:
            engine = create_engine(config.database.url)
            with engine.connect() as conn:
                # Check if TimescaleDB extension is installed
                result = conn.execute(text("SELECT * FROM pg_extension WHERE extname = 'timescaledb'"))
                if result.fetchone() is None:
                    self.errors.append("TimescaleDB extension not installed")
                    return False
                
                # Check if continuous aggregates exist
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM timescaledb_information.continuous_aggregates
                    WHERE view_name = 'ohlc_1m_agg'
                """))
                if result.fetchone()[0] == 0:
                    self.errors.append("Continuous aggregate 'ohlc_1m_agg' not found")
                    return False
                
                return True
        except Exception as e:
            self.errors.append(f"Database validation failed: {e}")
            return False
    
    def validate_kafka(self) -> bool:
        """Validate Kafka connectivity"""
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=config.kafka.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            topics = consumer.list_consumer_groups()
            consumer.close()
            return True
        except Exception as e:
            self.warnings.append(f"Kafka validation warning: {e}")
            return False
    
    def validate_redis(self) -> bool:
        """Validate Redis connectivity"""
        try:
            import redis
            r = redis.Redis(
                host=config.redis.host,
                port=config.redis.port,
                password=config.redis.password,
                socket_connect_timeout=5
            )
            r.ping()
            return True
        except Exception as e:
            self.warnings.append(f"Redis validation warning: {e}")
            return False
    
    def validate_feature_sla(self) -> bool:
        """Validate feature freshness SLAs"""
        try:
            engine = create_engine(config.database.url)
            
            # Check OHLC freshness
            ohlc_status = check_feature_freshness(
                engine, 'ohlc_1m', config.feature_sla.ohlc_sla_seconds
            )
            if ohlc_status.get('violated'):
                self.warnings.append(
                    f"OHLC freshness SLA violated: {ohlc_status.get('age_seconds')}s > {config.feature_sla.ohlc_sla_seconds}s"
                )
            
            return True
        except Exception as e:
            self.warnings.append(f"Feature SLA validation warning: {e}")
            return False
    
    def validate_environment_vars(self) -> bool:
        """Validate required environment variables"""
        env_status = validate_environment()
        missing = [var for var, is_set in env_status.items() if not is_set]
        
        if missing:
            self.warnings.append(f"Missing optional environment variables: {', '.join(missing)}")
        
        return True
    
    def validate_all(self) -> Tuple[bool, List[str], List[str]]:
        """Run all validations"""
        print("🔍 Running deployment validation...")
        print()
        
        # Run validations
        self.validate_database()
        self.validate_kafka()
        self.validate_redis()
        self.validate_feature_sla()
        self.validate_environment_vars()
        
        # Print results
        if self.errors:
            print("❌ Errors found:")
            for error in self.errors:
                print(f"  - {error}")
            print()
        
        if self.warnings:
            print("⚠️  Warnings:")
            for warning in self.warnings:
                print(f"  - {warning}")
            print()
        
        if not self.errors and not self.warnings:
            print("✅ All validations passed!")
            return True, [], []
        elif not self.errors:
            print("✅ Deployment ready (with warnings)")
            return True, [], self.warnings
        else:
            print("❌ Deployment not ready")
            return False, self.errors, self.warnings


def main():
    """Main entry point"""
    validator = DeploymentValidator()
    success, errors, warnings = validator.validate_all()
    
    if not success:
        sys.exit(1)
    
    if warnings:
        print("⚠️  Deployment ready but has warnings. Review before production deployment.")
        sys.exit(0)
    
    print("✅ Deployment validation complete. Ready for production!")


if __name__ == "__main__":
    main()
