#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration loader for HDFS Audit
Loads and validates YAML configuration files
"""

import os
import yaml
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Load and manage configuration from YAML files"""
    
    def __init__(self, config_path: str):
        """
        Initialize config loader
        
        Args:
            config_path: Path to the main config.yaml file
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        logger.info(f"Loaded configuration from {self.config_path}")
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate configuration structure"""
        if 'schedules' not in self.config:
            raise ValueError("Configuration must contain 'schedules' section")
        
        for schedule in self.config['schedules']:
            if 'task_name' not in schedule:
                raise ValueError("Each schedule must have 'task_name'")
            if 'tables' not in schedule:
                raise ValueError(f"Schedule '{schedule.get('task_name')}' must have 'tables'")
            
            for table in schedule['tables']:
                # partition_template is optional to support non-partition tables
                required_fields = ['name', 'hdfs_path', 'format']
                for field in required_fields:
                    if field not in table:
                        raise ValueError(
                            f"Table in schedule '{schedule['task_name']}' missing required field: {field}"
                        )
    
    def get_defaults(self) -> Dict[str, Any]:
        """Get default configuration values"""
        return self.config.get('defaults', {})
    
    def get_limits(self) -> Dict[str, Any]:
        """
        Get safety limits to avoid overload. All fields are optional.
        
        Supported keys:
          - max_python_concurrency: int
          - max_jar_threads: int
          - max_effective_parallelism: int  (python_concurrency * jar_threads upper bound)
        """
        defaults = self.get_defaults()
        return defaults.get('limits', {})
    
    def get_jar_options(self) -> Dict[str, Any]:
        """Get default options for hdfs-counter.jar"""
        defaults = self.get_defaults()
        return defaults.get('jar_options', {'threads': 10})
    
    def get_python_concurrency(self) -> int:
        """
        Get Python concurrency level (how many tables to audit in parallel)
        
        Returns:
            Number of concurrent audit jobs, default 1 (serial execution)
        """
        defaults = self.get_defaults()
        return defaults.get('python_concurrency', 1)
    
    def clamp_python_concurrency(self, concurrency: int) -> int:
        """Clamp python concurrency based on configured limits."""
        limits = self.get_limits()
        max_c = limits.get('max_python_concurrency')
        if isinstance(max_c, int) and max_c > 0 and concurrency > max_c:
            logger.warning(f"python_concurrency {concurrency} exceeds max_python_concurrency {max_c}, clamping")
            return max_c
        return concurrency
    
    def clamp_jar_threads(self, threads: int) -> int:
        """Clamp jar threads based on configured limits."""
        limits = self.get_limits()
        max_t = limits.get('max_jar_threads')
        if isinstance(max_t, int) and max_t > 0 and threads > max_t:
            logger.warning(f"jar threads {threads} exceeds max_jar_threads {max_t}, clamping")
            return max_t
        return threads
    
    def clamp_effective_parallelism(self, python_concurrency: int, jar_threads: int) -> int:
        """
        Clamp python_concurrency further so that python_concurrency * jar_threads
        does not exceed max_effective_parallelism.
        """
        limits = self.get_limits()
        max_eff = limits.get('max_effective_parallelism')
        if not (isinstance(max_eff, int) and max_eff > 0):
            return python_concurrency
        
        # Avoid division by zero; jar_threads should be >=1
        jar_threads = max(1, jar_threads)
        eff = python_concurrency * jar_threads
        if eff <= max_eff:
            return python_concurrency
        
        clamped = max(1, max_eff // jar_threads)
        if clamped != python_concurrency:
            logger.warning(
                f"effective parallelism {eff} (=python_concurrency {python_concurrency} * jar_threads {jar_threads}) "
                f"exceeds max_effective_parallelism {max_eff}, clamping python_concurrency to {clamped}"
            )
        return clamped
    
    def get_schedules(self) -> List[Dict[str, Any]]:
        """Get all schedule configurations"""
        return self.config.get('schedules', [])
    
    def get_schedule_by_task(self, task_name: str) -> Optional[Dict[str, Any]]:
        """
        Get schedule configuration by task name
        
        Args:
            task_name: Name of the scheduling task
            
        Returns:
            Schedule configuration dict or None if not found
        """
        for schedule in self.get_schedules():
            if schedule['task_name'] == task_name:
                return schedule
        return None
    
    def get_all_task_names(self) -> List[str]:
        """Get list of all configured task names"""
        return [s['task_name'] for s in self.get_schedules()]
    
    def resolve_data_date(self, data_date: Optional[str] = None) -> str:
        """
        Resolve data date based on input or default configuration
        
        Args:
            data_date: Optional date string (YYYYMMDD format) or special value
            
        Returns:
            Resolved date string in YYYYMMDD format
        """
        if data_date is None:
            # Use default from config
            defaults = self.get_defaults()
            data_date = defaults.get('data_date', '${yesterday}')
        
        # Handle special date variables
        today = datetime.now()
        
        if data_date == '${yesterday}':
            return (today - timedelta(days=1)).strftime('%Y%m%d')
        elif data_date == '${today}':
            return today.strftime('%Y%m%d')
        else:
            # Assume it's a literal date string
            # Validate format
            try:
                datetime.strptime(data_date, '%Y%m%d')
                return data_date
            except ValueError:
                raise ValueError(f"Invalid date format: {data_date}. Expected YYYYMMDD")
    
    def build_audit_jobs(self, completed_tasks, data_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Build list of audit jobs based on completed tasks
        
        Args:
            completed_tasks: 已完成任务，支持三种格式:
                - List[str]: 简单的任务名列表（旧格式，向后兼容）
                - Dict[str, Dict[str, Any]]: 任务名 -> 单个元数据字典（旧格式）
                - Dict[str, Dict[str, Dict]]: 任务名 -> {批次号 -> 元数据}（新格式，支持多批次）
                  元数据包含: period_type, batch_no, data_date, data_month, data_hour
            data_date: Data date in YYYYMMDD format, or None to auto-resolve
                       - If provided: used for all jobs (overrides batch_no derived date)
                       - If None: prefer batch_no derived date, fallback to period_type rules
            
        Returns:
            List of audit job configurations
        """
        jobs = []
        jar_options = self.get_jar_options()
        
        # 统一转换为新格式: {task_name: {batch_no: metadata}}
        completed_tasks_dict = self._normalize_completed_tasks(completed_tasks)
        
        for schedule in self.get_schedules():
            task_name = schedule['task_name']
            
            if task_name not in completed_tasks_dict:
                continue
            
            # 获取该任务的所有批次
            task_batches = completed_tasks_dict.get(task_name)
            if not task_batches:
                continue
            
            # 获取调度级别的字段
            interface_id = str(schedule.get('interface_id', ''))
            platform_id = str(schedule.get('platform_id', ''))
            partner_id = str(schedule.get('partner_id', ''))
            config_period_type = schedule.get('period_type', 'daily')
            
            # 遍历每个批次
            for batch_no, task_meta in task_batches.items():
                # 校验 period_type 是否一致
                if task_meta and task_meta.get('period_type'):
                    ck_period_type = task_meta.get('period_type')
                    if ck_period_type != config_period_type:
                        logger.warning(
                            f"任务 {task_name} 批次 {batch_no} 的 period_type 不匹配: "
                            f"config={config_period_type}, ClickHouse={ck_period_type}，跳过此批次"
                        )
                        continue
                
                # 解析业务日期，优先级: 命令行参数 > batch_no解析 > period_type自动推导
                resolved_date, resolved_month, resolved_hour = self._resolve_dates_for_job(
                    task_name=task_name,
                    task_meta=task_meta,
                    data_date=data_date,
                    config_period_type=config_period_type
                )
                
                # 为该批次的每个表生成 job
                for table in schedule['tables']:
                    job = self._build_single_job(
                        schedule=schedule,
                        table=table,
                        task_meta=task_meta,
                        jar_options=jar_options,
                        resolved_date=resolved_date,
                        resolved_month=resolved_month,
                        resolved_hour=resolved_hour
                    )
                    
                    if job is not None:
                        jobs.append(job)
        
        return jobs
    
    def _normalize_completed_tasks(self, completed_tasks) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        将各种格式的 completed_tasks 统一转换为新格式
        
        新格式: {task_name: {batch_no: metadata}}
        """
        if isinstance(completed_tasks, list):
            # List[str] -> {task_name: {'': default_metadata}}
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
                for name in completed_tasks
            }
        
        if not isinstance(completed_tasks, dict) or not completed_tasks:
            return {}
        
        # 检测是新格式还是旧格式
        first_value = next(iter(completed_tasks.values()), None)
        
        if first_value is None:
            # {task_name: None} -> {task_name: {'': default_metadata}}
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
                for name in completed_tasks.keys()
            }
        
        if isinstance(first_value, dict):
            # 判断内层结构
            inner_first = next(iter(first_value.values()), None) if first_value else None
            
            if isinstance(inner_first, dict) and 'period_type' in inner_first:
                # 新格式: {task_name: {batch_no: metadata}}
                return completed_tasks
            
            if 'period_type' in first_value or 'batch_no' in first_value:
                # 旧格式: {task_name: metadata} -> {task_name: {batch_no: metadata}}
                result = {}
                for task_name, metadata in completed_tasks.items():
                    if isinstance(metadata, dict):
                        batch_no = str(metadata.get('batch_no', '') or '')
                        result[task_name] = {batch_no: metadata}
                    else:
                        result[task_name] = {
                            '': {
                                'period_type': None,
                                'batch_no': '',
                                'data_date': None,
                                'data_month': None,
                                'data_hour': None
                            }
                        }
                return result
        
        # 未知格式，返回空
        logger.warning(f"Unknown completed_tasks format, returning empty dict")
        return {}
    
    def _resolve_dates_for_job(self, task_name: str, task_meta: Optional[Dict[str, Any]],
                               data_date: Optional[str], config_period_type: str) -> tuple:
        """
        解析业务日期
        
        优先级: 命令行参数 > batch_no解析 > period_type自动推导
        
        Returns:
            (resolved_date, resolved_month, resolved_hour)
        """
        if data_date is not None:
            resolved_month = data_date[:6] if len(data_date) >= 6 else None
            resolved_hour = None
            # monthly 任务不需要 data_date，只需要 data_month
            if config_period_type == 'monthly':
                resolved_date = None
            else:
                resolved_date = data_date
        elif task_meta:
            # 从 batch_no 解析的日期
            resolved_date = task_meta.get('data_date')
            resolved_month = task_meta.get('data_month')
            resolved_hour = task_meta.get('data_hour')
            
            # 如果 batch_no 没有解析出日期，回退到自动推导
            if config_period_type == 'monthly':
                # monthly 任务：确保 data_month 有值，data_date 强制为 None
                resolved_date = None
                if not resolved_month:
                    fallback_date = self.resolve_data_date_for_period(config_period_type)
                    resolved_month = fallback_date[:6]
                    logger.warning(f"任务 {task_name} 的 data_month 为空，使用自动推导: {resolved_month}")
            elif not resolved_date:
                # daily/hourly 任务：确保 data_date 有值
                resolved_date = self.resolve_data_date_for_period(config_period_type)
                resolved_month = resolved_date[:6] if resolved_date else None
        else:
            # 无元数据，使用自动推导
            resolved_date = self.resolve_data_date_for_period(config_period_type)
            resolved_month = resolved_date[:6] if resolved_date else None
            resolved_hour = None
            
            # monthly 任务可以不需要 data_date
            if config_period_type == 'monthly':
                resolved_date = None
        
        return resolved_date, resolved_month, resolved_hour
    
    def _build_single_job(self, schedule: Dict[str, Any], table: Dict[str, Any],
                          task_meta: Optional[Dict[str, Any]], jar_options: Dict[str, Any],
                          resolved_date: Optional[str], resolved_month: Optional[str],
                          resolved_hour: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        构建单个 audit job
        
        Returns:
            job dict, or None if should be skipped
        """
        task_name = schedule['task_name']
        interface_id = str(schedule.get('interface_id', ''))
        platform_id = str(schedule.get('platform_id', ''))
        partner_id = str(schedule.get('partner_id', ''))
        config_period_type = schedule.get('period_type', 'daily')
        
        # Build full HDFS path with optional partition
        partition_template = table.get('partition_template')
        if partition_template:
            partition = self._resolve_partition_extended(
                partition_template, 
                data_date=resolved_date,
                data_month=resolved_month,
                data_hour=resolved_hour
            )
            full_path = self._join_hdfs_path(table['hdfs_path'], partition)
        else:
            full_path = table['hdfs_path']
        
        # Get threads (table-level override or global default)
        threads = table.get('threads', jar_options.get('threads', 10))
        if not isinstance(threads, int):
            try:
                threads = int(threads)
            except Exception:
                threads = jar_options.get('threads', 10)
        threads = max(1, self.clamp_jar_threads(threads))
        
        # Get delimiter for textfile
        delimiter = table.get('delimiter', '\\n')
        
        # batch_no: 确保为字符串，避免 None
        batch_no = ''
        if task_meta and task_meta.get('batch_no'):
            batch_no = str(task_meta.get('batch_no'))
        
        job = {
            'task_name': task_name,
            'interface_id': interface_id,
            'platform_id': platform_id,
            'partner_id': partner_id,
            'period_type': config_period_type,
            'table_name': table['name'],
            'hdfs_path': full_path,
            'format': table['format'].lower(),
            'threads': threads,
            'delimiter': delimiter,
            'data_date': resolved_date,
            'data_month': resolved_month,
            'data_hour': resolved_hour,
            'batch_no': batch_no
        }
        
        # 校验 hdfs_path 是否包含未替换的变量
        if '${' in full_path:
            logger.warning(
                f"跳过任务 {task_name}/{table['name']}: hdfs_path 包含未替换的变量: {full_path}"
            )
            return None
        
        logger.info(
            f"Created audit job: table={table['name']}, path={full_path}, "
            f"batch_no={batch_no}, data_date={resolved_date}, data_month={resolved_month}"
        )
        
        return job
    
    def _resolve_partition_extended(self, partition_template: str, 
                                    data_date: Optional[str] = None,
                                    data_month: Optional[str] = None,
                                    data_hour: Optional[str] = None) -> str:
        """
        Resolve partition template with extended variables
        
        Args:
            partition_template: Template like "statis_ymd=${data_date}/hour=${data_hour}"
            data_date: Data date string (YYYYMMDD format)
            data_month: Data month string (YYYYMM format)
            data_hour: Data hour string (HH format, 00-23)
            
        Returns:
            Resolved partition string
        """
        result = partition_template
        
        # 替换 ${data_date}
        if data_date:
            result = result.replace('${data_date}', data_date)
        
        # 替换 ${data_month}
        if data_month:
            result = result.replace('${data_month}', data_month)
        elif data_date and len(data_date) >= 6:
            # 从 data_date 推导 data_month
            result = result.replace('${data_month}', data_date[:6])
        
        # 替换 ${data_hour}
        if data_hour:
            result = result.replace('${data_hour}', data_hour)
        
        return result
    
    def resolve_data_date_for_period(self, period_type: str) -> str:
        """
        Resolve data date based on period type
        
        Rules:
            - daily: yesterday (YYYYMMDD)
            - monthly: last month's last day (for deriving YYYYMM)
            - hourly/minutely: today (YYYYMMDD)
        
        Args:
            period_type: Period type string (daily, monthly, hourly, minutely)
            
        Returns:
            Data date string in YYYYMMDD format
        """
        today = datetime.now()
        if period_type in ('hourly', 'minutely'):
            return today.strftime('%Y%m%d')
        elif period_type == 'monthly':
            # 月度任务默认稽核上个月的数据
            # 返回上个月的最后一天，这样取前6位就是上个月的 YYYYMM
            first_of_this_month = today.replace(day=1)
            last_of_prev_month = first_of_this_month - timedelta(days=1)
            return last_of_prev_month.strftime('%Y%m%d')
        else:
            # daily or unknown defaults to yesterday
            return (today - timedelta(days=1)).strftime('%Y%m%d')
    
    def _resolve_partition(self, partition_template: str, data_date: str) -> str:
        """
        Resolve partition template with actual values
        
        Args:
            partition_template: Template like "dt=${data_date}/dp=Japan"
            data_date: Data date string (YYYYMMDD format)
            
        Returns:
            Resolved partition string
        """
        result = partition_template
        
        # 替换 ${data_date}
        result = result.replace('${data_date}', data_date)
        
        # 替换 ${data_month}（基于数据日期所在月份，格式 YYYYMM）
        if '${data_month}' in result:
            data_month = self._compute_data_month(data_date)
            result = result.replace('${data_month}', data_month)
        
        return result
    
    def _compute_data_month(self, data_date: str) -> str:
        """
        计算数据日期所在月份（YYYYMM）
        
        逻辑：取数据日期的 YYYYMM
        示例：数据日期 20260101 -> data_month = 202601
        
        Returns:
            上个月，格式 YYYYMM
        """
        try:
            parsed = datetime.strptime(data_date, '%Y%m%d')
        except ValueError:
            raise ValueError(f"Invalid data_date format: {data_date}. Expected YYYYMMDD")
        return parsed.strftime('%Y%m')
    
    def _join_hdfs_path(self, base: str, suffix: str) -> str:
        """Join HDFS path parts safely without creating double slashes."""
        if not base:
            return suffix
        if not suffix:
            return base
        return base.rstrip('/') + '/' + suffix.lstrip('/')


class DbConfigLoader:
    """Load database configuration"""
    
    def __init__(self, config_path: str):
        """
        Initialize database config loader
        
        Args:
            config_path: Path to db_config.yaml
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self._load_config()
    
    def _load_config(self) -> None:
        """Load database configuration from YAML file"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Database configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        logger.info(f"Loaded database configuration from {self.config_path}")
    
    def get_mysql_config(self) -> Dict[str, Any]:
        """
        Get MySQL connection configuration
        
        Environment variables override config file values:
            MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD, MYSQL_CHARSET
        """
        mysql_config = self.config.get('mysql', {}).copy()
        
        # Environment variable overrides (priority: env > config file)
        env_mappings = {
            'host': 'MYSQL_HOST',
            'port': 'MYSQL_PORT',
            'database': 'MYSQL_DATABASE',
            'user': 'MYSQL_USER',
            'password': 'MYSQL_PASSWORD',
            'charset': 'MYSQL_CHARSET',
        }
        
        for config_key, env_var in env_mappings.items():
            env_value = os.environ.get(env_var)
            if env_value is not None:
                # Convert port to int if needed
                if config_key == 'port':
                    try:
                        mysql_config[config_key] = int(env_value)
                    except ValueError:
                        logger.warning(f"Invalid {env_var} value: {env_value}, using config file value")
                else:
                    mysql_config[config_key] = env_value
                logger.debug(f"MySQL {config_key} loaded from environment variable {env_var}")
        
        # Validate required fields
        required = ['host', 'port', 'database', 'user', 'password']
        for field in required:
            if field not in mysql_config or mysql_config[field] is None:
                raise ValueError(
                    f"MySQL configuration missing required field: {field}. "
                    f"Set via config file or environment variable {env_mappings.get(field, field.upper())}"
                )
        
        return mysql_config
    
    def get_clickhouse_config(self) -> Dict[str, Any]:
        """
        Get ClickHouse connection configuration
        
        Supports two formats:
            - hosts: List of hosts for high availability (recommended)
            - host: Single host (backward compatible)
        
        Environment variables override config file values:
            CLICKHOUSE_HOST: Single host or comma-separated list (e.g., "host1,host2,host3")
            CLICKHOUSE_PORT, CLICKHOUSE_DATABASE, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
        
        Returns:
            ClickHouse configuration dict
        """
        ch_config = self.config.get('clickhouse', {}).copy()
        
        # Environment variable overrides (priority: env > config file)
        env_mappings = {
            'port': 'CLICKHOUSE_PORT',
            'database': 'CLICKHOUSE_DATABASE',
            'user': 'CLICKHOUSE_USER',
            'password': 'CLICKHOUSE_PASSWORD',
        }
        
        for config_key, env_var in env_mappings.items():
            env_value = os.environ.get(env_var)
            if env_value is not None:
                # Convert port to int if needed
                if config_key == 'port':
                    try:
                        ch_config[config_key] = int(env_value)
                    except ValueError:
                        logger.warning(f"Invalid {env_var} value: {env_value}, using config file value")
                else:
                    ch_config[config_key] = env_value
                logger.debug(f"ClickHouse {config_key} loaded from environment variable {env_var}")
        
        # Special handling for CLICKHOUSE_HOST: supports comma-separated list for HA
        # e.g., "host1,host2,host3" -> hosts: ["host1", "host2", "host3"]
        env_host = os.environ.get('CLICKHOUSE_HOST')
        if env_host:
            host_list = [h.strip() for h in env_host.split(',') if h.strip()]
            if len(host_list) > 1:
                # Multiple hosts -> set as 'hosts' list
                ch_config['hosts'] = host_list
                ch_config.pop('host', None)  # Remove single host if exists
                logger.debug(f"ClickHouse hosts loaded from CLICKHOUSE_HOST: {host_list}")
            elif len(host_list) == 1:
                # Single host
                ch_config['host'] = host_list[0]
                logger.debug(f"ClickHouse host loaded from CLICKHOUSE_HOST: {host_list[0]}")
        
        # Validate: need either 'hosts' list or 'host' string
        has_hosts = ch_config.get('hosts') and isinstance(ch_config.get('hosts'), list) and len(ch_config.get('hosts')) > 0
        has_host = ch_config.get('host') and ch_config.get('host') is not None
        
        if not has_hosts and not has_host:
            raise ValueError(
                "ClickHouse configuration missing required field: 'hosts' or 'host'. "
                "Set CLICKHOUSE_HOST environment variable (supports comma-separated list for HA)"
            )
        
        return ch_config
    
    def has_clickhouse_config(self) -> bool:
        """Check if ClickHouse configuration exists (supports both 'hosts' list and 'host' string)"""
        if 'clickhouse' not in self.config:
            return False
        ch_config = self.config.get('clickhouse', {})
        # Check for either hosts list or host string
        has_hosts = ch_config.get('hosts') and isinstance(ch_config.get('hosts'), list) and len(ch_config.get('hosts')) > 0
        has_host = 'host' in ch_config and ch_config.get('host')
        return has_hosts or has_host


if __name__ == '__main__':
    # Test configuration loading
    import sys
    
    logging.basicConfig(level=logging.DEBUG)
    
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = '../config/config.yaml'
    
    try:
        loader = ConfigLoader(config_path)
        print(f"Loaded {len(loader.get_schedules())} schedules")
        print(f"Task names: {loader.get_all_task_names()}")
        
        # Test job building
        date = loader.resolve_data_date()
        print(f"Resolved date: {date}")
        
        jobs = loader.build_audit_jobs(loader.get_all_task_names(), date)
        print(f"Generated {len(jobs)} audit jobs")
        for job in jobs:
            print(f"  - {job['table_name']}: {job['hdfs_path']}")
            print(f"    interface_id: {job['interface_id']}, platform_id: {job['platform_id']}, partner_id: {job['partner_id']}")
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
