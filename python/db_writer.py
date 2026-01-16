#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Writer for HDFS Audit
Writes audit results to MySQL database (append mode)
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
    """Write audit results to MySQL database (append mode, no upsert)"""
    
    # SQL statement for append mode
    INSERT_SQL = """
        INSERT INTO audit_result (
            task_name, interface_id, platform_id, partner_id,
            table_name, hdfs_path,
            period_type, batch_no, data_date, data_month, data_hour,
            row_count, file_count, total_size_bytes,
            status, error_msg, duration_ms, created_at
        ) VALUES (
            %(task_name)s, %(interface_id)s, %(platform_id)s, %(partner_id)s,
            %(table_name)s, %(hdfs_path)s,
            %(period_type)s, %(batch_no)s, %(data_date)s, %(data_month)s, %(data_hour)s,
            %(row_count)s, %(file_count)s, %(total_size_bytes)s,
            %(status)s, %(error_msg)s, %(duration_ms)s, %(created_at)s
        )
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
                     result: CounterResult,
                     interface_id: str = '',
                     platform_id: str = '',
                     partner_id: str = '',
                     period_type: str = 'daily',
                     batch_no: str = '',
                     data_date: Optional[str] = None,
                     data_month: Optional[str] = None,
                     data_hour: Optional[str] = None) -> int:
        """
        Write audit result to database (append mode)
        
        Args:
            task_name: Scheduling task name
            table_name: Hive table name
            hdfs_path: Full HDFS path
            result: CounterResult from hdfs-counter
            interface_id: Interface ID
            platform_id: Platform ID (province platform code)
            partner_id: Partner ID
            period_type: Period type (hourly/daily/monthly)
            batch_no: Batch number from scheduling platform
            data_date: Data date (YYYYMMDD) for daily/hourly tasks
            data_month: Data month (YYYYMM) for monthly tasks
            data_hour: Data hour (HH) for hourly tasks
            
        Returns:
            Number of affected rows
        """
        # Parse data_date to DATE type if provided
        parsed_date = None
        if data_date:
            try:
                parsed_date = datetime.strptime(data_date, '%Y%m%d').date()
            except ValueError:
                logger.warning(f"Invalid data_date format: {data_date}, expected YYYYMMDD")
        
        # Prepare data
        data = {
            'task_name': task_name,
            'interface_id': interface_id,
            'platform_id': platform_id,
            'partner_id': partner_id,
            'table_name': table_name,
            'hdfs_path': hdfs_path,
            'period_type': period_type,
            'batch_no': batch_no,
            'data_date': parsed_date,
            'data_month': data_month,
            'data_hour': data_hour,
            'row_count': result.row_count,
            'file_count': result.file_count,
            'total_size_bytes': result.total_size_bytes,
            'status': result.status,
            'error_msg': result.get_error_message(),
            'duration_ms': result.duration_ms,
            'created_at': datetime.now()
        }
        
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    affected = cursor.execute(self.INSERT_SQL, data)
                    conn.commit()
                    
                    logger.info(
                        f"Wrote audit result: {table_name}, period={period_type}, "
                        f"date={data_date}, month={data_month}, platform_id={platform_id}, "
                        f"rows={result.row_count}, status={result.status}"
                    )
                    return affected
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to write audit result: {e}")
                raise
    
    def write_job_result(self, job: Dict[str, Any], result: CounterResult) -> int:
        """
        Write audit result from job configuration
        
        Args:
            job: Audit job dict containing:
                - task_name: Task name
                - table_name: Hive table name
                - hdfs_path: Full HDFS path
                - interface_id: Interface ID
                - platform_id: Platform ID
                - partner_id: Partner ID
                - period_type: Period type (hourly/daily/monthly)
                - batch_no: Batch number
                - data_date: Data date (YYYYMMDD)
                - data_month: Data month (YYYYMM)
                - data_hour: Data hour (HH)
            result: CounterResult
            
        Returns:
            Number of affected rows
        """
        return self.write_result(
            task_name=job['task_name'],
            table_name=job['table_name'],
            hdfs_path=job['hdfs_path'],
            result=result,
            interface_id=job.get('interface_id', ''),
            platform_id=job.get('platform_id', ''),
            partner_id=job.get('partner_id', ''),
            period_type=job.get('period_type', 'daily'),
            batch_no=job.get('batch_no', ''),
            data_date=job.get('data_date'),
            data_month=job.get('data_month'),
            data_hour=job.get('data_hour')
        )
    
    def get_latest_result(self, table_name: str, 
                          data_date: Optional[str] = None,
                          data_month: Optional[str] = None,
                          platform_id: str = '') -> Optional[Dict[str, Any]]:
        """
        Query the latest audit result for a table
        
        Args:
            table_name: Hive table name
            data_date: Data date (YYYYMMDD) for daily tasks
            data_month: Data month (YYYYMM) for monthly tasks
            platform_id: Platform ID (default empty string)
            
        Returns:
            Result dict or None if not found
        """
        if data_date:
            sql = """
                SELECT * FROM audit_result 
                WHERE table_name = %s AND data_date = %s AND platform_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """
            params = (
                table_name,
                datetime.strptime(data_date, '%Y%m%d').date(),
                platform_id
            )
        elif data_month:
            sql = """
                SELECT * FROM audit_result 
                WHERE table_name = %s AND data_month = %s AND platform_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """
            params = (table_name, data_month, platform_id)
        else:
            sql = """
                SELECT * FROM audit_result 
                WHERE table_name = %s AND platform_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """
            params = (table_name, platform_id)
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()
    
    def get_results_by_partner(self, partner_id: str, 
                               data_date: Optional[str] = None,
                               data_month: Optional[str] = None) -> list:
        """
        Query audit results by partner ID
        
        Args:
            partner_id: Partner ID
            data_date: Data date (YYYYMMDD) for daily tasks
            data_month: Data month (YYYYMM) for monthly tasks
            
        Returns:
            List of result dicts
        """
        if data_date:
            sql = """
                SELECT * FROM audit_result 
                WHERE partner_id = %s AND data_date = %s
                ORDER BY table_name, created_at DESC
            """
            params = (partner_id, datetime.strptime(data_date, '%Y%m%d').date())
        elif data_month:
            sql = """
                SELECT * FROM audit_result 
                WHERE partner_id = %s AND data_month = %s
                ORDER BY table_name, created_at DESC
            """
            params = (partner_id, data_month)
        else:
            raise ValueError("Either data_date or data_month must be provided")
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
    
    def get_results_by_interface_id(self, interface_id: str,
                                    data_date: Optional[str] = None,
                                    data_month: Optional[str] = None) -> list:
        """
        Query audit results by interface ID
        
        Args:
            interface_id: Interface ID
            data_date: Data date (YYYYMMDD) for daily tasks
            data_month: Data month (YYYYMM) for monthly tasks
            
        Returns:
            List of result dicts
        """
        if data_date:
            sql = """
                SELECT * FROM audit_result 
                WHERE interface_id = %s AND data_date = %s
                ORDER BY table_name, created_at DESC
            """
            params = (interface_id, datetime.strptime(data_date, '%Y%m%d').date())
        elif data_month:
            sql = """
                SELECT * FROM audit_result 
                WHERE interface_id = %s AND data_month = %s
                ORDER BY table_name, created_at DESC
            """
            params = (interface_id, data_month)
        else:
            raise ValueError("Either data_date or data_month must be provided")
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
    
    def get_results_by_batch(self, batch_no: str) -> list:
        """
        Query audit results by batch number
        
        Args:
            batch_no: Batch number
            
        Returns:
            List of result dicts
        """
        sql = """
            SELECT * FROM audit_result 
            WHERE batch_no = %s
            ORDER BY task_name, created_at DESC
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (batch_no,))
                return cursor.fetchall()
    
    def get_results_by_period_type(self, period_type: str, 
                                   data_date: Optional[str] = None,
                                   data_month: Optional[str] = None) -> list:
        """
        Query audit results by period type
        
        Args:
            period_type: Period type (hourly/daily/monthly)
            data_date: Data date (YYYYMMDD)
            data_month: Data month (YYYYMM)
            
        Returns:
            List of result dicts
        """
        if period_type == 'monthly' and data_month:
            sql = """
                SELECT * FROM audit_result 
                WHERE period_type = %s AND data_month = %s
                ORDER BY task_name, created_at DESC
            """
            params = (period_type, data_month)
        elif data_date:
            sql = """
                SELECT * FROM audit_result 
                WHERE period_type = %s AND data_date = %s
                ORDER BY task_name, created_at DESC
            """
            params = (period_type, datetime.strptime(data_date, '%Y%m%d').date())
        else:
            raise ValueError("data_date required for hourly/daily, data_month required for monthly")
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
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
        
        # Test 1: Write daily task result
        print("Test 1: Writing daily task result...")
        writer.write_result(
            task_name='P_TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET_10100',
            table_name='ods_bss.TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET',
            hdfs_path='hdfs://beh002/hive/warehouse/ods_bss.db/to_d_svc_ur_user_5g_term_switch_basic_asset/statis_ymd=20260101/prov_id=10100',
            result=result,
            interface_id='03041',
            platform_id='10100',
            partner_id='2',
            period_type='daily',
            batch_no='20260101',
            data_date='20260101',
            data_month='202601'
        )
        
        # Test 2: Write monthly task result
        print("Test 2: Writing monthly task result...")
        writer.write_result(
            task_name='70018_P_TO_M_ACCT_AM_PAY_FLOW',
            table_name='ods_bss.TO_M_ACCT_AM_PAY_FLOW',
            hdfs_path='hdfs://beh002/hive/warehouse/ods_bss.db/to_m_acct_am_pay_flow/statis_ym=202601',
            result=result,
            interface_id='07001',
            platform_id='70018',
            partner_id='2',
            period_type='monthly',
            batch_no='202601',
            data_month='202601'
        )
        
        # Query results
        print("\nQuerying results...")
        daily_result = writer.get_latest_result(
            'ods_bss.TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET',
            data_date='20260101',
            platform_id='10100'
        )
        print(f"Daily result: {daily_result}")
        
        monthly_result = writer.get_latest_result(
            'ods_bss.TO_M_ACCT_AM_PAY_FLOW',
            data_month='202601',
            platform_id='70018'
        )
        print(f"Monthly result: {monthly_result}")
        
        writer.close()
    else:
        print("Usage: python db_writer.py <db_config.yaml>")
