"""
Pipeline Health Monitor

Monitors overall pipeline health and generates reports
"""

import sys
from datetime import datetime
from src.config.settings import config
from src.utils.helpers import check_feature_freshness, validate_environment
from src.features.smartdb_contract import SmartDBContract
from sqlalchemy import create_engine, text


class PipelineHealthMonitor:
    """Monitor pipeline health"""
    
    def __init__(self):
        self.db_engine = create_engine(config.database.url)
        self.contract = SmartDBContract(self.db_engine)
        self.health_status = {
            'database': False,
            'features': {},
            'kafka': False,
            'redis': False,
            'overall': False
        }
    
    def check_database_health(self):
        """Check database connectivity and schema"""
        print("Checking database health...")
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            
            # Check TimescaleDB extension
            with self.db_engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM pg_extension WHERE extname = 'timescaledb'
                """))
                has_timescale = result.fetchone()[0] > 0
            
            if has_timescale:
                self.health_status['database'] = True
                print("  ✓ Database: Healthy (TimescaleDB extension active)")
            else:
                print("  ✗ Database: TimescaleDB extension not found")
                
        except Exception as e:
            print(f"  ✗ Database: {e}")
    
    def check_feature_health(self):
        """Check feature freshness and availability"""
        print("Checking feature health...")
        
        features_to_check = {
            'ohlc_1m': config.feature_sla.ohlc_sla_seconds,
            'sma_20': config.feature_sla.sma_20_sla_seconds,
            'vwap_5m': config.feature_sla.vwap_sla_seconds,
        }
        
        all_fresh = True
        for feature_name, max_age in features_to_check.items():
            status = check_feature_freshness(self.db_engine, feature_name, max_age)
            is_fresh = status.get('status') == 'fresh'
            self.health_status['features'][feature_name] = is_fresh
            
            if is_fresh:
                print(f"  ✓ {feature_name}: Fresh ({status.get('age_seconds', 0):.1f}s)")
            else:
                print(f"  ✗ {feature_name}: Stale or unavailable")
                all_fresh = False
        
        return all_fresh
    
    def check_kafka_health(self):
        """Check Kafka connectivity"""
        print("Checking Kafka health...")
        try:
            from kafka import KafkaConsumer
            consumer = KafkaConsumer(
                bootstrap_servers=config.kafka.bootstrap_servers,
                consumer_timeout_ms=5000
            )
            topics = consumer.list_consumer_groups()
            consumer.close()
            self.health_status['kafka'] = True
            print("  ✓ Kafka: Healthy")
        except Exception as e:
            print(f"  ✗ Kafka: {e}")
    
    def check_redis_health(self):
        """Check Redis connectivity"""
        print("Checking Redis health...")
        try:
            import redis
            r = redis.Redis(
                host=config.redis.host,
                port=config.redis.port,
                password=config.redis.password,
                socket_connect_timeout=5
            )
            r.ping()
            self.health_status['redis'] = True
            print("  ✓ Redis: Healthy")
        except Exception as e:
            print(f"  ✗ Redis: {e}")
    
    def check_data_volume(self):
        """Check data volume statistics"""
        print("\nData Volume Statistics:")
        try:
            with self.db_engine.connect() as conn:
                # Raw data count
                result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM market_data_raw
                    WHERE time > NOW() - INTERVAL '24 hours'
                """))
                raw_count = result.fetchone().count
                print(f"  Raw data (last 24h): {raw_count:,} rows")
                
                # Aggregate count
                result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM ohlc_1m_agg
                    WHERE minute > NOW() - INTERVAL '24 hours'
                """))
                agg_count = result.fetchone().count
                print(f"  OHLC aggregates (last 24h): {agg_count:,} buckets")
                
        except Exception as e:
            print(f"  ⚠ Could not retrieve data volume: {e}")
    
    def generate_report(self):
        """Generate health report"""
        print("\n" + "="*80)
        print("Pipeline Health Report")
        print("="*80)
        print(f"Generated at: {datetime.now()}")
        print()
        
        # Overall health
        all_checks = [
            self.health_status['database'],
            self.health_status['kafka'],
            self.health_status['redis'],
            all(self.health_status['features'].values())
        ]
        
        self.health_status['overall'] = all(all_checks)
        
        if self.health_status['overall']:
            print("✓ Overall Status: HEALTHY")
        else:
            print("✗ Overall Status: UNHEALTHY")
        
        print("\nComponent Status:")
        print(f"  Database: {'✓' if self.health_status['database'] else '✗'}")
        print(f"  Kafka: {'✓' if self.health_status['kafka'] else '✗'}")
        print(f"  Redis: {'✓' if self.health_status['redis'] else '✗'}")
        
        print("\nFeature Status:")
        for feature, is_healthy in self.health_status['features'].items():
            print(f"  {feature}: {'✓' if is_healthy else '✗'}")
    
    def run_all_checks(self):
        """Run all health checks"""
        print("\n" + "="*80)
        print("Pipeline Health Monitor")
        print("="*80)
        
        self.check_database_health()
        self.check_feature_health()
        self.check_kafka_health()
        self.check_redis_health()
        self.check_data_volume()
        self.generate_report()
        
        return self.health_status['overall']


def main():
    """Main entry point"""
    monitor = PipelineHealthMonitor()
    is_healthy = monitor.run_all_checks()
    sys.exit(0 if is_healthy else 1)


if __name__ == "__main__":
    main()
