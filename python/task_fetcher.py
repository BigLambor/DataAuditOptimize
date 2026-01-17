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


# Type alias for task metadata
TaskMetadata = Dict[str, Any]  # {period_type, batch_no, data_date, data_month, data_hour}

# Type alias for completed tasks with metadata (supports multiple batches per task)
# Structure: {task_name: {batch_no: TaskMetadata}}
# - Outer key: task_name
# - Inner key: batch_no (for deduplication: same task + same batch = one entry)
# - Value: metadata dict with period_type, batch_no, data_date, data_month, data_hour
CompletedTasksDict = Dict[str, Dict[str, TaskMetadata]]

# Legacy format for backward compatibility
LegacyCompletedTasksDict = Dict[str, TaskMetadata]


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
            CompletedTasksDict: Dict with structure {task_name: {batch_no: metadata}}
            - Supports multiple batches per task
            - Automatically deduplicates by (task_name, batch_no)
            - metadata includes: period_type, batch_no, data_date, data_month, data_hour
            
            Also supports legacy formats for backward compatibility:
            - List[str]: Simple list of task names
        """
        pass


class MockTaskFetcher(TaskFetcher):
    """
    Mock implementation for testing
    Returns all configured task names as "completed"
    """
    
    def __init__(self, task_names: Union[List[str], CompletedTasksDict, LegacyCompletedTasksDict]):
        """
        Initialize mock fetcher
        
        Args:
            task_names: Can be one of:
                - List[str]: Simple list of task names
                - CompletedTasksDict: {task_name: {batch_no: metadata}} (new format)
                - LegacyCompletedTasksDict: {task_name: metadata} (old format, auto-converted)
        """
        self.task_names = task_names
    
    def get_completed_tasks(self, start_time: datetime, end_time: datetime, data_date: Optional[str] = None) -> CompletedTasksDict:
        """Return all configured tasks as completed"""
        logger.info(f"MockTaskFetcher: Returning {len(self.task_names)} tasks as completed")
        
        if isinstance(self.task_names, list):
            # 从 List[str] 转换，使用默认值
            # 使用空字符串作为 batch_no key（表示未知批次）
            return {
                name: {
                    '': {
                        'period_type': None,
                        'batch_no': '',
                        'data_date': None,
                        'data_month': None,
                        'data_hour': None
                    }
                }
                for name in self.task_names
            }
        elif isinstance(self.task_names, dict):
            # 检测是新格式还是旧格式
            # 新格式: {task_name: {batch_no: metadata}}
            # 旧格式: {task_name: metadata}
            first_value = next(iter(self.task_names.values()), None) if self.task_names else None
            
            if first_value is None:
                return {}
            
            # 判断是否为新格式（内层 value 也是 dict 且有 batch_no 作为 key）
            if isinstance(first_value, dict):
                inner_first = next(iter(first_value.values()), None) if first_value else None
                if isinstance(inner_first, dict) and 'period_type' in inner_first:
                    # 新格式，直接返回
                    return self.task_names
            
            # 旧格式，需要转换
            # {task_name: metadata} -> {task_name: {batch_no: metadata}}
            result: CompletedTasksDict = {}
            for task_name, metadata in self.task_names.items():
                if isinstance(metadata, dict):
                    batch_no = metadata.get('batch_no', '') or ''
                    result[task_name] = {batch_no: metadata}
                else:
                    # 非法格式，跳过
                    logger.warning(f"Invalid metadata format for task {task_name}, skipping")
            return result
        else:
            return {}


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
                - host: ClickHouse server host (单节点，向后兼容)
                - hosts: ClickHouse server hosts list (多节点高可用，优先使用)
                         第一个为主节点，其余为备用节点
                - port: ClickHouse native port (default 9000)
                - database: Database name
                - user: Username
                - password: Password
                - query_template: SQL template for querying completed tasks
        """
        # 支持 hosts 列表（高可用）或单个 host（向后兼容）
        hosts_config = ch_config.get('hosts')
        if hosts_config and isinstance(hosts_config, list) and len(hosts_config) > 0:
            # 新格式：hosts 列表
            self.host = str(hosts_config[0])
            self.alt_hosts = hosts_config[1:] if len(hosts_config) > 1 else []
        else:
            # 旧格式：单个 host
            self.host = ch_config['host']
            self.alt_hosts = []
        
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
        
        # 日志输出节点信息
        if self.alt_hosts:
            logger.info(
                f"ClickHouseTaskFetcher initialized for {self.host}:{self.port}/{self.database} "
                f"(alt_hosts: {', '.join(self.alt_hosts)})"
            )
        else:
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
                
                # 构建 alt_hosts 字符串（格式: "host1:port,host2:port"）
                alt_hosts_str = None
                if self.alt_hosts:
                    alt_hosts_str = ','.join(
                        f"{h}:{self.port}" if ':' not in str(h) else str(h)
                        for h in self.alt_hosts
                    )
                
                self._client = Client(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    alt_hosts=alt_hosts_str  # 备用节点，主节点不可用时自动切换
                )
                
                if alt_hosts_str:
                    logger.debug(f"ClickHouse client created with alt_hosts: {alt_hosts_str}")
                else:
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
            Dict with structure {task_name: {batch_no: metadata}}
            - Supports multiple batches per task (e.g., backfill + normal run)
            - Automatically deduplicates by (task_name, batch_no)
            - metadata includes: period_type, batch_no, data_date, data_month, data_hour
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
            total_records = 0
            
            for row in result:
                total_records += 1
                
                if len(row) >= 3:
                    # 新格式: (task_name, period_type, batch_no)
                    task_name = str(row[0])
                    period_type = str(row[1]) if row[1] else 'daily'
                    batch_no = str(row[2]) if row[2] else ''
                    
                    # 解析 batch_no 获取业务日期
                    parsed = parse_batch_no(batch_no, period_type)
                    
                    metadata: TaskMetadata = {
                        'period_type': period_type,
                        'batch_no': batch_no,
                        'data_date': parsed['data_date'],
                        'data_month': parsed['data_month'],
                        'data_hour': parsed['data_hour']
                    }
                    
                    # 初始化 task_name 的批次字典
                    if task_name not in completed_tasks:
                        completed_tasks[task_name] = {}
                    
                    # 按 (task_name, batch_no) 去重
                    # 同一任务同一批次运行多次，只保留一条（后面的覆盖前面的，或者跳过）
                    if batch_no not in completed_tasks[task_name]:
                        completed_tasks[task_name][batch_no] = metadata
                    else:
                        # 已存在，记录日志但不覆盖（保留第一次出现的）
                        logger.debug(f"Duplicate (task={task_name}, batch={batch_no}), keeping first occurrence")
                else:
                    # 旧格式: 只有 task_name，保持向后兼容
                    task_name = str(row[0])
                    
                    if task_name not in completed_tasks:
                        completed_tasks[task_name] = {}
                    
                    # 使用空字符串作为 batch_no key
                    if '' not in completed_tasks[task_name]:
                        completed_tasks[task_name][''] = {
                            'period_type': None,
                            'batch_no': '',
                            'data_date': None,
                            'data_month': None,
                            'data_hour': None
                        }
            
            # 统计信息
            total_batches = sum(len(batches) for batches in completed_tasks.values())
            logger.info(
                f"ClickHouseTaskFetcher: Found {total_records} records -> "
                f"{len(completed_tasks)} unique tasks, {total_batches} unique (task, batch) combinations"
            )
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
    
    print("=" * 60)
    print("测试 1: 新格式 mock 数据 (多批次)")
    print("=" * 60)
    
    # 测试新格式的 mock 数据 (支持同一任务多批次)
    mock_tasks_new: CompletedTasksDict = {
        'P_TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET_10100': {
            '20260115': {
                'period_type': 'daily',
                'batch_no': '20260115',
                'data_date': '20260115',
                'data_month': '202601',
                'data_hour': None
            },
            '20260116': {  # 同一任务，不同批次
                'period_type': 'daily',
                'batch_no': '20260116',
                'data_date': '20260116',
                'data_month': '202601',
                'data_hour': None
            }
        },
        '70018_P_TO_M_ACCT_AM_PAY_FLOW': {
            '202601': {
                'period_type': 'monthly',
                'batch_no': '202601',
                'data_date': None,
                'data_month': '202601',
                'data_hour': None
            }
        }
    }
    fetcher = create_task_fetcher('mock', task_names=mock_tasks_new)
    
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    completed = fetcher.get_completed_tasks(start_time, end_time)
    print(f"\n返回结构:")
    for task_name, batches in completed.items():
        print(f"  任务: {task_name}")
        for batch_no, meta in batches.items():
            print(f"    批次 {batch_no}: data_date={meta.get('data_date')}, data_month={meta.get('data_month')}")
    
    print("\n" + "=" * 60)
    print("测试 2: 旧格式 mock 数据 (自动转换)")
    print("=" * 60)
    
    # 测试旧格式自动转换
    mock_tasks_legacy = {
        'P_TO_D_SVC_UR_USER_5G_TERM_SWITCH_BASIC_ASSET_10100': {
            'period_type': 'daily',
            'batch_no': '20260115',
            'data_date': '20260115',
            'data_month': '202601',
            'data_hour': None
        }
    }
    fetcher_legacy = create_task_fetcher('mock', task_names=mock_tasks_legacy)
    completed_legacy = fetcher_legacy.get_completed_tasks(start_time, end_time)
    print(f"旧格式转换结果: {completed_legacy}")
    
    print("\n" + "=" * 60)
    print("测试 3: List[str] 格式 (自动转换)")
    print("=" * 60)
    
    # 测试 List[str] 格式
    mock_tasks_list = ['task_a', 'task_b']
    fetcher_list = create_task_fetcher('mock', task_names=mock_tasks_list)
    completed_list = fetcher_list.get_completed_tasks(start_time, end_time)
    print(f"List 格式转换结果: {completed_list}")
    
    print("\n" + "=" * 60)
    print("测试 4: batch_no 解析")
    print("=" * 60)
    
    test_cases = [
        ('2026010516', 'hourly'),   # -> data_date=20260105, hour=16
        ('20260105', 'daily'),      # -> data_date=20260105
        ('202601', 'monthly'),      # -> data_month=202601
    ]
    for batch_no, period_type in test_cases:
        result = parse_batch_no(batch_no, period_type)
        print(f"  {period_type}: batch_no={batch_no} -> {result}")
    
    print("\n" + "=" * 60)
    print("测试 5: 统计信息")
    print("=" * 60)
    
    # 统计
    total_tasks = len(completed)
    total_batches = sum(len(batches) for batches in completed.values())
    print(f"任务数: {total_tasks}, 批次数: {total_batches}")
