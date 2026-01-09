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
    
    def build_audit_jobs(self, completed_tasks: List[str], data_date: str) -> List[Dict[str, Any]]:
        """
        Build list of audit jobs based on completed tasks
        
        Args:
            completed_tasks: List of completed task names
            data_date: Data date in YYYYMMDD format
            
        Returns:
            List of audit job configurations
        """
        jobs = []
        jar_options = self.get_jar_options()
        
        for schedule in self.get_schedules():
            task_name = schedule['task_name']
            
            if task_name not in completed_tasks:
                continue
            
            # 获取调度级别的字段
            interface_id = schedule.get('interface_id', '')
            partner_id = schedule.get('partner_id', '')
            
            for table in schedule['tables']:
                # Build full HDFS path with optional partition
                partition_template = table.get('partition_template')
                if partition_template:
                    partition = self._resolve_partition(partition_template, data_date)
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
                
                # 获取表级别的 province_id（可选）
                province_id = table.get('province_id', '')
                
                job = {
                    'task_name': task_name,
                    'interface_id': interface_id,
                    'partner_id': partner_id,
                    'province_id': province_id,
                    'table_name': table['name'],
                    'hdfs_path': full_path,
                    'format': table['format'].lower(),
                    'threads': threads,
                    'delimiter': delimiter,
                    'data_date': data_date
                }
                
                jobs.append(job)
                logger.info(f"Created audit job for table: {table['name']}, path: {full_path}, province_id: {province_id}")
        
        return jobs
    
    def _resolve_partition(self, partition_template: str, data_date: str) -> str:
        """
        Resolve partition template with actual values
        
        Args:
            partition_template: Template like "dt=${data_date}/dp=Japan"
            data_date: Data date string
            
        Returns:
            Resolved partition string
        """
        return partition_template.replace('${data_date}', data_date)
    
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
        """Get MySQL connection configuration"""
        mysql_config = self.config.get('mysql', {})
        
        # Validate required fields
        required = ['host', 'port', 'database', 'user', 'password']
        for field in required:
            if field not in mysql_config:
                raise ValueError(f"MySQL configuration missing required field: {field}")
        
        return mysql_config
    
    def get_clickhouse_config(self) -> Dict[str, Any]:
        """
        Get ClickHouse connection configuration
        
        Returns:
            ClickHouse configuration dict
        """
        ch_config = self.config.get('clickhouse', {})
        
        # Validate required fields
        required = ['host']
        for field in required:
            if field not in ch_config:
                raise ValueError(f"ClickHouse configuration missing required field: {field}")
        
        return ch_config
    
    def has_clickhouse_config(self) -> bool:
        """Check if ClickHouse configuration exists"""
        return 'clickhouse' in self.config and 'host' in self.config.get('clickhouse', {})


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
            print(f"    interface_id: {job['interface_id']}, partner_id: {job['partner_id']}, province_id: {job['province_id']}")
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
