#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task Fetcher for HDFS Audit
Fetches completed scheduling tasks from the scheduling platform (ClickHouse)
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class TaskFetcher(ABC):
    """Abstract base class for task fetcher implementations"""
    
    @abstractmethod
    def get_completed_tasks(self, start_time: datetime, end_time: datetime, data_date: Optional[str] = None) -> List[str]:
        """
        Get list of completed task names within the time range
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            data_date: Optional business date (YYYYMMDD) for extra filtering
            
        Returns:
            List of completed task names
        """
        pass


class MockTaskFetcher(TaskFetcher):
    """
    Mock implementation for testing
    Returns all configured task names as "completed"
    """
    
    def __init__(self, task_names: List[str]):
        """
        Initialize mock fetcher
        
        Args:
            task_names: List of task names to return as completed
        """
        self.task_names = task_names
    
    def get_completed_tasks(self, start_time: datetime, end_time: datetime, data_date: Optional[str] = None) -> List[str]:
        """Return all configured tasks as completed"""
        logger.info(f"MockTaskFetcher: Returning {len(self.task_names)} tasks as completed")
        return self.task_names


class ClickHouseTaskFetcher(TaskFetcher):
    """
    Fetch completed tasks from scheduling platform via ClickHouse
    
    从 ClickHouse 获取调度平台在指定时间范围内完成的任务清单
    """
    
    def __init__(self, ch_config: Dict[str, Any]):
        """
        Initialize ClickHouse task fetcher
        
        Args:
            ch_config: ClickHouse connection configuration
                - host: ClickHouse server host
                - port: ClickHouse native port (default 9000)
                - database: Database name
                - user: Username
                - password: Password
                - query_template: SQL template for querying completed tasks
        """
        self.host = ch_config['host']
        self.port = ch_config.get('port', 9000)
        self.database = ch_config.get('database', 'default')
        self.user = ch_config.get('user', 'default')
        self.password = ch_config.get('password', '')
        self.query_template = ch_config.get('query_template', self._default_query_template())
        
        # Timezone handling for start/end time formatting
        # If set, runner should build window in this TZ, and we also interpret naive datetimes as this TZ.
        self.timezone: Optional[str] = ch_config.get('timezone')
        self._tzinfo = ZoneInfo(self.timezone) if self.timezone else None
        
        self._client = None
        logger.info(f"ClickHouseTaskFetcher initialized for {self.host}:{self.port}/{self.database}")
    
    def _default_query_template(self) -> str:
        """Default SQL template for querying completed tasks"""
        return """
            SELECT DISTINCT task_name
            FROM task_instance
            WHERE status = 'SUCCESS'
              AND end_time >= '{start_time}'
              AND end_time < '{end_time}'
        """
    
    def now(self) -> datetime:
        """Return current time in configured timezone (if any)."""
        if self._tzinfo:
            return datetime.now(self._tzinfo)
        return datetime.now()
    
    def _format_time(self, dt: datetime) -> str:
        """Format datetime according to configured timezone and ClickHouse string format."""
        if self._tzinfo:
            # If dt is naive, interpret it in the configured TZ.
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=self._tzinfo)
            else:
                dt = dt.astimezone(self._tzinfo)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    def _get_client(self):
        """Get or create ClickHouse client (lazy initialization)"""
        if self._client is None:
            try:
                from clickhouse_driver import Client
                self._client = Client(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                logger.debug("ClickHouse client created successfully")
            except ImportError:
                raise ImportError(
                    "clickhouse-driver is required for ClickHouseTaskFetcher. "
                    "Install it with: pip install clickhouse-driver"
                )
        return self._client
    
    def get_completed_tasks(self, start_time: datetime, end_time: datetime, data_date: Optional[str] = None) -> List[str]:
        """
        Fetch completed tasks from ClickHouse
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            data_date: Optional business date (YYYYMMDD) for extra filtering. If query_template
                       contains '{data_date}', it will be filled.
            
        Returns:
            List of completed task names
        """
        client = self._get_client()
        
        # Format query with time range
        fmt_args = {
            'start_time': self._format_time(start_time),
            'end_time': self._format_time(end_time),
            'data_date': data_date or ''
        }
        query = self.query_template.format(**fmt_args)
        
        logger.debug(f"Executing ClickHouse query: {query}")
        
        try:
            result = client.execute(query)
            task_names = [row[0] for row in result]
            logger.info(f"ClickHouseTaskFetcher: Found {len(task_names)} completed tasks")
            return task_names
        except Exception as e:
            logger.error(f"Failed to fetch tasks from ClickHouse: {e}")
            raise
    
    def close(self):
        """Close ClickHouse connection"""
        if self._client:
            self._client.disconnect()
            self._client = None
            logger.debug("ClickHouse client closed")


def create_task_fetcher(fetcher_type: str, **kwargs) -> TaskFetcher:
    """
    Factory function to create task fetcher
    
    Args:
        fetcher_type: Type of fetcher ('mock', 'clickhouse')
        **kwargs: Arguments to pass to the fetcher constructor
        
    Returns:
        TaskFetcher instance
    """
    if fetcher_type == 'mock':
        task_names = kwargs.get('task_names', [])
        return MockTaskFetcher(task_names)
    elif fetcher_type == 'clickhouse':
        ch_config = kwargs.get('ch_config', {})
        return ClickHouseTaskFetcher(ch_config)
    else:
        raise ValueError(f"Unknown fetcher type: {fetcher_type}")


if __name__ == '__main__':
    # Test mock fetcher
    logging.basicConfig(level=logging.DEBUG)
    
    mock_tasks = ['dw_user_daily', 'dw_order_daily', 'ods_log_import']
    fetcher = create_task_fetcher('mock', task_names=mock_tasks)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    completed = fetcher.get_completed_tasks(start_time, end_time)
    print(f"Completed tasks: {completed}")
    
    # Test ClickHouse fetcher configuration (without actual connection)
    print("\nClickHouse fetcher example configuration:")
    ch_config = {
        'host': 'localhost',
        'port': 9000,
        'database': 'scheduler',
        'user': 'default',
        'password': '',
        'query_template': """
            SELECT DISTINCT task_name
            FROM task_instance
            WHERE status = 'SUCCESS'
              AND end_time >= '{start_time}'
              AND end_time < '{end_time}'
        """
    }
    print(f"  Config: {ch_config}")
