#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDFS Counter Client
Wrapper for calling hdfs-counter.jar CLI tool
"""

import os
import json
import subprocess
import logging
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class CounterResult:
    """Result from hdfs-counter.jar execution"""
    path: str
    row_count: int
    file_count: int
    success_file_count: int
    total_size_bytes: int
    status: str  # 'success', 'partial', 'failed'
    duration_ms: int
    errors: list
    
    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'CounterResult':
        """Create CounterResult from JSON dict"""
        return cls(
            path=data.get('path', ''),
            row_count=data.get('row_count', -1),
            file_count=data.get('file_count', 0),
            success_file_count=data.get('success_file_count', 0),
            total_size_bytes=data.get('total_size_bytes', 0),
            status=data.get('status', 'failed'),
            duration_ms=data.get('duration_ms', 0),
            errors=data.get('errors', [])
        )
    
    @classmethod
    def create_error(cls, path: str, error_msg: str) -> 'CounterResult':
        """Create error result"""
        return cls(
            path=path,
            row_count=-1,
            file_count=0,
            success_file_count=0,
            total_size_bytes=0,
            status='failed',
            duration_ms=0,
            errors=[{'file': '', 'error': error_msg}]
        )
    
    def is_success(self) -> bool:
        """Check if counting was fully successful"""
        return self.status == 'success'
    
    def is_partial(self) -> bool:
        """Check if counting was partially successful"""
        return self.status == 'partial'
    
    def get_error_message(self) -> Optional[str]:
        """Get combined error message"""
        if not self.errors:
            return None
        return json.dumps(self.errors, ensure_ascii=False)


class HdfsCounterClient:
    """Client for hdfs-counter.jar CLI tool"""
    
    # 环境变量名
    ENV_JAR_PATH = 'HDFS_COUNTER_JAR'
    
    def __init__(self, jar_path: Optional[str] = None, java_home: Optional[str] = None,
                 hadoop_conf_dir: Optional[str] = None,
                 timeout: int = 3600):
        """
        Initialize HDFS Counter client
        
        Args:
            jar_path: Path to hdfs-counter.jar (可选，优先从环境变量 HDFS_COUNTER_JAR 获取)
            java_home: Optional JAVA_HOME path (默认从环境变量 JAVA_HOME 获取)
            hadoop_conf_dir: Optional HADOOP_CONF_DIR path (默认从环境变量 HADOOP_CONF_DIR 获取)
            timeout: Command timeout in seconds (default: 1 hour)
        """
        # 优先级: 参数 > 环境变量
        self.jar_path = jar_path or os.environ.get(self.ENV_JAR_PATH)
        self.java_home = java_home or os.environ.get('JAVA_HOME')
        self.hadoop_conf_dir = hadoop_conf_dir or os.environ.get('HADOOP_CONF_DIR')
        self.timeout = timeout
        
        # Validate jar path is provided
        if not self.jar_path:
            raise ValueError(
                f"hdfs-counter.jar path not provided. "
                f"Set via parameter or environment variable {self.ENV_JAR_PATH}"
            )
        
        # Validate jar exists
        if not os.path.exists(self.jar_path):
            raise FileNotFoundError(f"hdfs-counter.jar not found: {self.jar_path}")
        
        # Determine java command
        if java_home:
            self.java_cmd = os.path.join(java_home, 'bin', 'java')
        else:
            self.java_cmd = 'java'
    
    def count(self, hdfs_path: str, file_format: str,
              threads: int = 10, delimiter: str = '\\n') -> CounterResult:
        """
        Count rows in HDFS path
        
        Args:
            hdfs_path: HDFS directory path
            file_format: File format (orc, parquet, textfile)
            threads: Number of threads for parallel processing
            delimiter: Line delimiter for textfile format
            
        Returns:
            CounterResult with row count and status
        """
        # Build command
        cmd = [
            self.java_cmd,
            '-jar', self.jar_path,
            '--path', hdfs_path,
            '--format', file_format,
            '--threads', str(threads)
        ]
        
        # Add delimiter for textfile
        if file_format.lower() == 'textfile':
            cmd.extend(['--delimiter', delimiter])
        
        logger.info(f"Executing: {' '.join(cmd)}")
        
        # Set environment
        env = os.environ.copy()
        if self.hadoop_conf_dir:
            env['HADOOP_CONF_DIR'] = self.hadoop_conf_dir
        
        try:
            # Execute command
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env=env
            )
            
            # Log stderr if any
            if process.stderr:
                logger.warning(f"stderr: {process.stderr}")
            
            # Parse JSON output (be tolerant to leading/trailing non-JSON noise)
            if process.stdout:
                data, err = self._try_parse_json(process.stdout)
                if data is not None:
                    result = CounterResult.from_json(data)
                    logger.info(
                        f"Count result for {hdfs_path}: "
                        f"rows={result.row_count}, files={result.file_count}, "
                        f"status={result.status} (exit={process.returncode})"
                    )
                    return result
                
                logger.error(f"Failed to parse JSON output: {err}. Raw stdout: {process.stdout}")
                # If process failed, attach stderr for debugging
                if process.returncode != 0 and process.stderr:
                    err = f"{err}; stderr={process.stderr}"
                return CounterResult.create_error(hdfs_path, f"Invalid JSON output: {err}")
            
            # No stdout at all
            msg = f"No output from command, exit code: {process.returncode}"
            if process.stderr:
                msg += f", stderr: {process.stderr}"
            return CounterResult.create_error(hdfs_path, msg)
        
        except subprocess.TimeoutExpired:
            logger.error(f"Command timed out after {self.timeout} seconds")
            return CounterResult.create_error(hdfs_path, f"Timeout after {self.timeout}s")
        
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            return CounterResult.create_error(hdfs_path, str(e))
    
    def _try_parse_json(self, text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Try parsing JSON from text output.
        Some Java logs or wrappers may add extra lines; we try to locate the first JSON object.
        """
        s = text.strip()
        if not s:
            return None, "empty output"
        
        # Fast path: pure JSON
        try:
            return json.loads(s), None
        except json.JSONDecodeError:
            pass
        
        # Tolerant path: find a JSON object inside the output
        start = s.find('{')
        if start == -1:
            return None, "no '{' found in output"
        
        decoder = json.JSONDecoder()
        for i in range(start, len(s)):
            if s[i] != '{':
                continue
            try:
                obj, end = decoder.raw_decode(s, i)
                if isinstance(obj, dict):
                    return obj, None
            except json.JSONDecodeError:
                continue
        
        return None, "unable to decode JSON object from output"
    
    def count_job(self, job: Dict[str, Any]) -> CounterResult:
        """
        Execute counting for an audit job
        
        Args:
            job: Audit job dict containing hdfs_path, format, threads, delimiter
            
        Returns:
            CounterResult
        """
        return self.count(
            hdfs_path=job['hdfs_path'],
            file_format=job['format'],
            threads=job.get('threads', 10),
            delimiter=job.get('delimiter', '\\n')
        )


if __name__ == '__main__':
    # Test client
    import sys
    
    logging.basicConfig(level=logging.DEBUG)
    
    # 支持两种调用方式:
    # 1. python hdfs_counter_client.py <hdfs_path> <format>  (jar 从环境变量 HDFS_COUNTER_JAR 获取)
    # 2. python hdfs_counter_client.py <jar_path> <hdfs_path> <format>  (显式指定 jar)
    
    if len(sys.argv) == 3:
        # 从环境变量获取 jar_path
        jar_path = None
        hdfs_path = sys.argv[1]
        file_format = sys.argv[2]
    elif len(sys.argv) >= 4:
        jar_path = sys.argv[1]
        hdfs_path = sys.argv[2]
        file_format = sys.argv[3]
    else:
        print("Usage:")
        print("  python hdfs_counter_client.py <hdfs_path> <format>")
        print("    (requires env HDFS_COUNTER_JAR)")
        print("  python hdfs_counter_client.py <jar_path> <hdfs_path> <format>")
        sys.exit(1)
    
    try:
        client = HdfsCounterClient(jar_path)
        result = client.count(hdfs_path, file_format)
        
        print(f"Path: {result.path}")
        print(f"Row count: {result.row_count}")
        print(f"File count: {result.file_count}")
        print(f"Status: {result.status}")
        if result.errors:
            print(f"Errors: {result.errors}")
    except (ValueError, FileNotFoundError) as e:
        print(f"Error: {e}")
        sys.exit(1)

