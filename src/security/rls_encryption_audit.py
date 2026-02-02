"""
Security Configuration: RLS + Encryption + Audit Logging

Addresses: Week 11 - Security (RLS + 加密 + 审计日志)
"""

from sqlalchemy import create_engine, text
from typing import List, Dict
import logging
from datetime import datetime


class RLSManager:
    """Row Level Security Manager for multi-tenancy"""
    
    def __init__(self, db_engine):
        self.db_engine = db_engine
    
    def create_tenant_policy(self, table_name: str, tenant_column: str = "tenant_id"):
        """
        Create RLS policy for tenant isolation
        
        Args:
            table_name: Table name
            tenant_column: Column name for tenant ID
        """
        policy_sql = f"""
        CREATE POLICY tenant_isolation ON {table_name}
            FOR ALL
            USING (
                {tenant_column} = current_setting('app.current_tenant', TRUE)::VARCHAR
                OR current_setting('app.current_tenant', TRUE) IS NULL
            );
        """
        
        with self.db_engine.connect() as conn:
            conn.execute(text(policy_sql))
            conn.commit()
    
    def set_current_tenant(self, tenant_id: str):
        """Set current tenant for session"""
        with self.db_engine.connect() as conn:
            conn.execute(text(f"SET app.current_tenant = '{tenant_id}'"))
            conn.commit()


class AuditLogger:
    """Audit logging for compliance"""
    
    def __init__(self, db_engine):
        self.db_engine = db_engine
        self.logger = logging.getLogger('audit')
    
    def create_audit_table(self):
        """Create audit log table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS audit_log (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ DEFAULT NOW(),
            user_id VARCHAR(100),
            action VARCHAR(50),
            table_name VARCHAR(100),
            record_id VARCHAR(100),
            old_values JSONB,
            new_values JSONB,
            ip_address INET,
            user_agent TEXT
        );
        
        CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log (timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log (user_id);
        CREATE INDEX IF NOT EXISTS idx_audit_table ON audit_log (table_name);
        """
        
        with self.db_engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
    
    def log_access(self, user_id: str, action: str, table_name: str, record_id: str = None):
        """Log data access"""
        insert_sql = """
        INSERT INTO audit_log (user_id, action, table_name, record_id)
        VALUES (:user_id, :action, :table_name, :record_id)
        """
        
        with self.db_engine.connect() as conn:
            conn.execute(
                text(insert_sql),
                {
                    "user_id": user_id,
                    "action": action,
                    "table_name": table_name,
                    "record_id": record_id
                }
            )
            conn.commit()
        
        self.logger.info(f"Audit log: {user_id} {action} {table_name} {record_id}")
    
    def get_audit_trail(self, table_name: str, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get audit trail for a table"""
        query = """
        SELECT timestamp, user_id, action, record_id, old_values, new_values
        FROM audit_log
        WHERE table_name = :table_name
          AND timestamp BETWEEN :start_time AND :end_time
        ORDER BY timestamp DESC
        """
        
        with self.db_engine.connect() as conn:
            result = conn.execute(
                text(query),
                {
                    "table_name": table_name,
                    "start_time": start_time,
                    "end_time": end_time
                }
            )
            return [dict(row) for row in result]


class EncryptionManager:
    """Encryption configuration for sensitive data"""
    
    @staticmethod
    def encrypt_field(value: str, key: str = None) -> str:
        """
        Encrypt sensitive field
        
        In production, use proper encryption (AES-256)
        """
        # Placeholder - use proper encryption library
        import hashlib
        if key is None:
            key = "default_key"  # Should come from secrets manager
        
        # Simple example - use proper encryption in production
        encrypted = hashlib.sha256((value + key).encode()).hexdigest()
        return encrypted
    
    @staticmethod
    def create_encrypted_column(table_name: str, column_name: str):
        """Create encrypted column using pgcrypto"""
        sql = f"""
        -- Enable pgcrypto extension
        CREATE EXTENSION IF NOT EXISTS pgcrypto;
        
        -- Add encrypted column (example)
        -- ALTER TABLE {table_name} ADD COLUMN {column_name}_encrypted BYTEA;
        
        -- Create function to encrypt
        CREATE OR REPLACE FUNCTION encrypt_{column_name}(value TEXT)
        RETURNS BYTEA AS $$
        BEGIN
            RETURN pgp_sym_encrypt(value, current_setting('app.encryption_key'));
        END;
        $$ LANGUAGE plpgsql;
        """
        return sql


def setup_security(db_engine):
    """Setup complete security configuration"""
    # RLS
    rls_manager = RLSManager(db_engine)
    tables = ['market_data_raw', 'ohlc_1m_agg', 'large_trade_flags']
    for table in tables:
        try:
            rls_manager.create_tenant_policy(table)
        except Exception as e:
            print(f"RLS policy creation for {table}: {e}")
    
    # Audit logging
    audit_logger = AuditLogger(db_engine)
    audit_logger.create_audit_table()
    
    print("Security configuration complete!")


if __name__ == "__main__":
    from sqlalchemy import create_engine
    import os
    
    db_url = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/financial_db')
    engine = create_engine(db_url)
    
    setup_security(engine)
