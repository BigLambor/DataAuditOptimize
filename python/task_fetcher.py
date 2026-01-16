#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Task Fetcher for HDFS Audit
Fetches completed scheduling tasks from the scheduling platform (ClickHouse)
"""

import logging
import re
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


# Type alias for completed tasks with metadata
# Key: task_name, Value: dict with period_type, batch_no, data_date, data_month, data_hour
CompletedTasksDict = Dict[str, Dict[str, Any]]


def parse_batch_no(batch_no: str, period_type: str) -> Dict[str, Optional[str]]:
    """
    解析 batch_no 获取业务日期信息
    
    Args:
        batch_no: 批次号
            - hourly:  YYYYMMDDHH (10位), e.g. '2026010516'
            - daily:   YYYYMMDD (8位), e.g. '20260105'
            - monthly: YYYYMM (6位), e.g. '202601'
        period_type: 周期类型 (hourly/daily/monthly)
        
    Returns:
        Dict with keys: data_date, data_month, data_hour
    """
    result = {
        'data_date': None,
        'data_month': None,
        'data_hour': None
    }
    
    if not batch_no:
        return result
    
    # 清理 batch_no，只保留数字
    batch_no = re.sub(r'\D', '', str(batch_no))
    
    if period_type == 'hourly' and len(batch_no) >= 10:
        # YYYYMMDDHH -> data_date=YYYYMMDD, data_hour=HH
        result['data_date'] = batch_no[:8]
        result['data_month'] = batch_no[:6]
        result['data_hour'] = batch_no[8:10]
    elif period_type == 'daily' and len(batch_no) >= 8:
        # YYYYMMDD -> data_date=YYYYMMDD
        result['data_date'] = batch_no[:8]
        result['data_month'] = batch_no[:6]
    elif period_type == 'monthly' and len(batch_no) >= 6:
        # YYYYMM -> data_month=YYYYMM
        result['data_month'] = batch_no[:6]
    else:
        # 尝试根据长度自动推断
        if len(batch_no) >= 10:
            result['data_date'] = batch_no[:8]
            result['data_month'] = batch_no[:6]
            result['data_hour'] = batch_no[8:10]
        elif len(batch_no) == 8:
            result['data_date'] = batch_no
            result['data_month'] = batch_no[:6]
        elif len(batch_no) == 6:
            result['data_month'] = batch_no
        
        logger.warning(f"batch_no={batch_no} 长度与 period_type={period_type} 不匹配，已尝试自动解析")
    
    return result


class TaskFetcher(ABC):
    """Abstract base class for task fetcher implementations"""
    
    @abstractmethod
    def get_completed_tasks(self, start_time: datetime, end_time: datetime, data_date: Optional[str] = None) -> Union[List[str], CompletedTasksDict]:
        """
        Get completed tasks within the time range
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            data_date: Optional business date (YYYYMMDD) for extra filtering
            
        Returns:
            Either:
            - List[str]: Simple list of task names (legacy format)
            - CompletedTasksDict: Dict with task metadata including period_type, batch_no, 
              data_date, data_month, data_hour (new format)
        """
        pass


class MockTaskFetcher(TaskFetcher):
    """
    Mock implementation for testing
    Returns all configured task names as "completed"
    """
    
    def __init__(self, task_names: Union[List[str], CompletedTasksDict]):
        """
        Initialize mock fetcher
        
        Args:
            task_names: List of task names or CompletedTasksDict to return as completed
        """
        self.task_names = task_names
    
    def get_completed_tasks(self, start_time: datetime, end_time: datetime, data_date: Optional[str] = None) -> CompletedTasksDict:
        """Return all configured tasks as completed"""
        logger.info(f"MockTaskFetcher: Returning {len(self.task_names)} tasks as completed")
        
        # 转换为 CompletedTasksDict 格式
        if isinstance(self.task_names, dict):
            return self.task_names
        else:
            # 从 List[str] 转换，使用默认值
            return {
                name: {
                    'period_type': None,
                    'batch_no': None,
                    'data_date': None,
                    'data_month': None,
                    'data_hour': None
                }
                for name in self.task_names
            }


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
    
    def get_completed_tasks(self, start_time: datetime, end_time: datetime, data_date: Optional[str] = None) -> CompletedTasksDict:
        """
        Fetch completed tasks from ClickHouse
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            data_date: Optional business date (YYYYMMDD) for extra filtering. If query_template
                       contains '{data_date}', it will be filled.
            
        Returns:
            Dict mapping task_name -> task metadata including:
            - period_type: 周期类型 (hourly/daily/monthly)
            - batch_no: 原始批次号
            - data_date: 业务日期 (YYYYMMDD)
            - data_month: 业务月份 (YYYYMM)
            - data_hour: 小时 (HH, 仅 hourly)
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
            
            # 检查返回的列数，支持新旧两种格式
            # 旧格式: 只有 task_name (1列)
            # 新格式: task_name, period_type, batch_no (3列)
            completed_tasks: CompletedTasksDict = {}
            
            for row in result:
                if len(row) >= 3:
                    # 新格式: (task_name, period_type, batch_no)
                    task_name = str(row[0])
                    period_type = str(row[1]) if row[1] else 'daily'
                    batch_no = str(row[2]) if row[2] else ''
                    
                    # 解析 batch_no 获取业务日期
                    parsed = parse_batch_no(batch_no, period_type)
                    
                    completed_tasks[task_name] = {
                        'period_type': period_type,
                        'batch_no': batch_no,
                        'data_date': parsed['data_date'],
                        'data_month': parsed['data_month'],
                        'data_hour': parsed['data_hour']
                    }
                else:
                    # 旧格式: 只有 task_name，保持向后兼容
                    task_name = str(row[0])
                    completed_tasks[task_name] = {
                        'period_type': None,
                        'batch_no': None,
                        'data_date': None,
                        'data_month': None,
                        'data_hour': None
                    }
            
            logger.info(f"ClickHouseTaskFetcher: Found {len(completed_tasks)} completed tasks")
            return completed_tasks
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
    
    # 测试新格式的 mock 数据
    mock_tasks = {
        'P_TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET_10100': {
            'period_type': 'daily',
            'batch_no': '20260115',
            'data_date': '20260115',
            'data_month': '202601',
            'data_hour': None
        },
        '70018_P_TO_M_ACCT_AM_PAY_FLOW': {
            'period_type': 'monthly',
            'batch_no': '202601',
            'data_date': None,
            'data_month': '202601',
            'data_hour': None
        }
    }
    fetcher = create_task_fetcher('mock', task_names=mock_tasks)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    completed = fetcher.get_completed_tasks(start_time, end_time)
    print(f"Completed tasks: {completed}")
    
    # 测试 batch_no 解析
    print("\n测试 batch_no 解析:")
    test_cases = [
        ('2026010516', 'hourly'),   # -> data_date=20260105, hour=16
        ('20260105', 'daily'),      # -> data_date=20260105
        ('202601', 'monthly'),      # -> data_month=202601
    ]
    for batch_no, period_type in test_cases:
        result = parse_batch_no(batch_no, period_type)
        print(f"  {period_type}: batch_no={batch_no} -> {result}")
    
    # Test ClickHouse fetcher configuration (without actual connection)
    print("\nClickHouse fetcher example configuration:")
    ch_config = {
        'host': 'localhost',
        'port': 9000,
        'database': 'scheduler',
        'user': 'default',
        'password': '',
        'query_template': """
            SELECT job_code AS task_name, 
                   batch_type AS period_type, 
                   batch_no
            FROM uops_daas.ods_all_dacp_dataflow_job_trigger_rt
            WHERE state = 1
              AND complete_dt >= '{start_time}'
              AND complete_dt < '{end_time}'
        """
    }
    print(f"  Config: {ch_config}")
