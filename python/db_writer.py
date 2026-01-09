#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Writer for HDFS Audit
Writes audit results to MySQL database
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor
from dbutils.pooled_db import PooledDB

from hdfs_counter_client import CounterResult

logger = logging.getLogger(__name__)


class AuditDbWriter:
    """Write audit results to MySQL database"""
    
    # SQL statements
    INSERT_SQL = """
        INSERT INTO audit_result (
            task_name, interface_id, partner_id, province_id,
            table_name, hdfs_path, data_date,
            row_count, file_count, total_size_bytes,
            status, error_msg, duration_ms, created_at
        ) VALUES (
            %(task_name)s, %(interface_id)s, %(partner_id)s, %(province_id)s,
            %(table_name)s, %(hdfs_path)s, %(data_date)s,
            %(row_count)s, %(file_count)s, %(total_size_bytes)s,
            %(status)s, %(error_msg)s, %(duration_ms)s, %(created_at)s
        )
    """
    
    UPSERT_SQL = """
        INSERT INTO audit_result (
            task_name, interface_id, partner_id, province_id,
            table_name, hdfs_path, data_date,
            row_count, file_count, total_size_bytes,
            status, error_msg, duration_ms, created_at
        ) VALUES (
            %(task_name)s, %(interface_id)s, %(partner_id)s, %(province_id)s,
            %(table_name)s, %(hdfs_path)s, %(data_date)s,
            %(row_count)s, %(file_count)s, %(total_size_bytes)s,
            %(status)s, %(error_msg)s, %(duration_ms)s, %(created_at)s
        )
        ON DUPLICATE KEY UPDATE
            interface_id = VALUES(interface_id),
            partner_id = VALUES(partner_id),
            hdfs_path = VALUES(hdfs_path),
            row_count = VALUES(row_count),
            file_count = VALUES(file_count),
            total_size_bytes = VALUES(total_size_bytes),
            status = VALUES(status),
            error_msg = VALUES(error_msg),
            duration_ms = VALUES(duration_ms),
            created_at = VALUES(created_at)
    """
    
    def __init__(self, db_config: Dict[str, Any], pool_size: int = 5):
        """
        Initialize database writer
        
        Args:
            db_config: MySQL connection configuration
            pool_size: Connection pool size
        """
        self.db_config = db_config
        
        # Create connection pool
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=pool_size,
            mincached=1,
            maxcached=pool_size,
            blocking=True,
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            charset=db_config.get('charset', 'utf8mb4'),
            cursorclass=DictCursor
        )
        
        logger.info(f"Database connection pool initialized for {db_config['host']}:{db_config['port']}")
    
    @contextmanager
    def get_connection(self):
        """Get database connection from pool"""
        conn = self.pool.connection()
        try:
            yield conn
        finally:
            conn.close()
    
    def write_result(self, task_name: str, table_name: str, hdfs_path: str,
                     data_date: str, result: CounterResult,
                     interface_id: str = '', partner_id: str = '', province_id: str = '',
                     upsert: bool = True) -> int:
        """
        Write audit result to database
        
        Args:
            task_name: Scheduling task name
            table_name: Hive table name
            hdfs_path: Full HDFS path
            data_date: Data date (YYYYMMDD)
            result: CounterResult from hdfs-counter
            interface_id: Interface ID
            partner_id: Partner ID
            province_id: Province ID
            upsert: If True, update existing record; otherwise insert only
            
        Returns:
            Number of affected rows
        """
        # Prepare data
        data = {
            'task_name': task_name,
            'interface_id': interface_id,
            'partner_id': partner_id,
            'province_id': province_id,
            'table_name': table_name,
            'hdfs_path': hdfs_path,
            'data_date': datetime.strptime(data_date, '%Y%m%d').date(),
            'row_count': result.row_count,
            'file_count': result.file_count,
            'total_size_bytes': result.total_size_bytes,
            'status': result.status,
            'error_msg': result.get_error_message(),
            'duration_ms': result.duration_ms,
            'created_at': datetime.now()
        }
        
        sql = self.UPSERT_SQL if upsert else self.INSERT_SQL
        
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    affected = cursor.execute(sql, data)
                    conn.commit()
                    
                    logger.info(
                        f"Wrote audit result: {table_name}, date={data_date}, "
                        f"province_id={province_id}, rows={result.row_count}, status={result.status}"
                    )
                    return affected
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to write audit result: {e}")
                raise
    
    def write_job_result(self, job: Dict[str, Any], result: CounterResult,
                         upsert: bool = True) -> int:
        """
        Write audit result from job configuration
        
        Args:
            job: Audit job dict (contains task_name, interface_id, partner_id, province_id, etc.)
            result: CounterResult
            upsert: If True, update existing record
            
        Returns:
            Number of affected rows
        """
        return self.write_result(
            task_name=job['task_name'],
            table_name=job['table_name'],
            hdfs_path=job['hdfs_path'],
            data_date=job['data_date'],
            result=result,
            interface_id=job.get('interface_id', ''),
            partner_id=job.get('partner_id', ''),
            province_id=job.get('province_id', ''),
            upsert=upsert
        )
    
    def get_result(self, table_name: str, data_date: str, 
                   province_id: str = '') -> Optional[Dict[str, Any]]:
        """
        Query audit result
        
        Args:
            table_name: Hive table name
            data_date: Data date (YYYYMMDD)
            province_id: Province ID (default empty string)
            
        Returns:
            Result dict or None if not found
        """
        sql = """
            SELECT * FROM audit_result 
            WHERE table_name = %s AND data_date = %s AND province_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (
                    table_name, 
                    datetime.strptime(data_date, '%Y%m%d').date(),
                    province_id
                ))
                return cursor.fetchone()
    
    def get_results_by_partner(self, partner_id: str, data_date: str) -> list:
        """
        Query audit results by partner ID
        
        Args:
            partner_id: Partner ID
            data_date: Data date (YYYYMMDD)
            
        Returns:
            List of result dicts
        """
        sql = """
            SELECT * FROM audit_result 
            WHERE partner_id = %s AND data_date = %s
            ORDER BY table_name
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (
                    partner_id,
                    datetime.strptime(data_date, '%Y%m%d').date()
                ))
                return cursor.fetchall()
    
    def get_results_by_interface_id(self, interface_id: str, data_date: str) -> list:
        """
        Query audit results by interface ID
        
        Args:
            interface_id: Interface ID
            data_date: Data date (YYYYMMDD)
            
        Returns:
            List of result dicts
        """
        sql = """
            SELECT * FROM audit_result 
            WHERE interface_id = %s AND data_date = %s
            ORDER BY table_name
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (
                    interface_id,
                    datetime.strptime(data_date, '%Y%m%d').date()
                ))
                return cursor.fetchall()
    
    def close(self):
        """Close connection pool"""
        if self.pool:
            self.pool.close()
            logger.info("Database connection pool closed")


def create_writer_from_config(config_path: str) -> AuditDbWriter:
    """
    Create AuditDbWriter from configuration file
    
    Args:
        config_path: Path to db_config.yaml
        
    Returns:
        AuditDbWriter instance
    """
    from config_loader import DbConfigLoader
    
    loader = DbConfigLoader(config_path)
    db_config = loader.get_mysql_config()
    return AuditDbWriter(db_config)


if __name__ == '__main__':
    # Test database writer
    import sys
    
    logging.basicConfig(level=logging.DEBUG)
    
    # Create a mock result
    result = CounterResult(
        path='/warehouse/test/dt=20260101',
        row_count=1000000,
        file_count=10,
        success_file_count=10,
        total_size_bytes=1073741824,
        status='success',
        duration_ms=1500,
        errors=[]
    )
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
        writer = create_writer_from_config(config_path)
        
        # Write test result with new fields
        writer.write_result(
            task_name='test_task',
            table_name='test.table',
            hdfs_path='/warehouse/test/dt=20260101',
            data_date='20260101',
            result=result,
            interface_id='1001',
            partner_id='P001',
            province_id='11'
        )
        
        # Query result
        saved = writer.get_result('test.table', '20260101', '11')
        print(f"Saved result: {saved}")
        
        writer.close()
    else:
        print("Usage: python db_writer.py <db_config.yaml>")
