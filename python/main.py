#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDFS Audit Main Program
Orchestrates the data audit process:
1. Get completed scheduling tasks from ClickHouse
2. Match with configuration to find audit jobs
3. Execute hdfs-counter for each job
4. Write results to MySQL
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from config_loader import ConfigLoader, DbConfigLoader
from task_fetcher import create_task_fetcher, TaskFetcher, ClickHouseTaskFetcher
from hdfs_counter_client import HdfsCounterClient, CounterResult
from db_writer import AuditDbWriter
from watermark_store import FileWatermarkStore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('hdfs_audit.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class HdfsAuditRunner:
    """Main runner for HDFS audit process"""
    
    def __init__(self, config_path: str, db_config_path: str, jar_path: str,
                 java_home: Optional[str] = None,
                 hadoop_conf_dir: Optional[str] = None):
        """
        Initialize audit runner
        
        Args:
            config_path: Path to config.yaml
            db_config_path: Path to db_config.yaml
            jar_path: Path to hdfs-counter.jar
            java_home: Optional JAVA_HOME path
            hadoop_conf_dir: Optional HADOOP_CONF_DIR path
        """
        # Load configurations
        self.config_loader = ConfigLoader(config_path)
        self.db_config_loader = DbConfigLoader(db_config_path)
        self._db_config_path = db_config_path
        
        # Initialize clients
        self.counter_client = HdfsCounterClient(
            jar_path=jar_path,
            java_home=java_home,
            hadoop_conf_dir=hadoop_conf_dir
        )
        
        self.db_writer = AuditDbWriter(
            db_config=self.db_config_loader.get_mysql_config()
        )
        
        # Initialize task fetcher from ClickHouse if configured
        self.task_fetcher: Optional[TaskFetcher] = None
        self._clickhouse_config: Dict[str, Any] = {}
        if self.db_config_loader.has_clickhouse_config():
            ch_config = self.db_config_loader.get_clickhouse_config()
            self._clickhouse_config = ch_config or {}
            self.task_fetcher = ClickHouseTaskFetcher(ch_config)
            logger.info("ClickHouse task fetcher initialized")
        
        logger.info("HdfsAuditRunner initialized")
    
    def set_task_fetcher(self, fetcher: TaskFetcher):
        """Set task fetcher implementation"""
        self.task_fetcher = fetcher
    
    def _process_single_job(self, job: Dict[str, Any]) -> Tuple[Dict[str, Any], CounterResult]:
        """
        Process a single audit job (for use in thread pool)
        
        Args:
            job: Audit job configuration
            
        Returns:
            Tuple of (job, result)
        """
        logger.info(f"Processing job: {job['table_name']} (province_id={job.get('province_id', '')})")
        result = self.counter_client.count_job(job)
        return job, result
    
    def run(self, data_date: Optional[str] = None,
            task_names: Optional[List[str]] = None,
            dry_run: bool = False,
            concurrency: Optional[int] = None,
            hours_lookback: float = 24.0,
            watermark_enabled: bool = True,
            watermark_path: Optional[str] = None,
            watermark_overlap_seconds: int = 600,
            watermark_max_window_hours: float = 24.0,
            watermark_init_now: bool = False,
            watermark_reset: bool = False,
            skip_clickhouse: bool = False) -> dict:
        """
        Run the audit process
        
        Args:
            data_date: Data date (YYYYMMDD), defaults to yesterday
            task_names: Optional list of task names to process (overrides task fetcher)
            dry_run: If True, only print jobs without executing
            concurrency: Number of concurrent jobs (overrides config)
            hours_lookback: Hours to look back for completed tasks (default 24.0). Supports decimals, e.g. 0.5 = 30 minutes.
            
        Returns:
            Summary dict with success/fail counts
        """
        start_time = datetime.now()
        logger.info(f"Starting audit run at {start_time}")
        
        # Resolve data date
        resolved_date = self.config_loader.resolve_data_date(data_date)
        logger.info(f"Data date: {resolved_date}")
        
        # Get completed tasks
        if task_names:
            # Use provided task names
            completed_tasks = task_names
            logger.info(f"Using provided task names: {completed_tasks}")
        elif skip_clickhouse:
            # Force skip ClickHouse even if configured
            completed_tasks = self.config_loader.get_all_task_names()
            logger.info(f"Skip ClickHouse enabled, using all configured tasks: {completed_tasks}")
        elif self.task_fetcher:
            # Fetch from ClickHouse scheduling platform
            # Prefer fetcher timezone if available to avoid time window mismatch
            if isinstance(self.task_fetcher, ClickHouseTaskFetcher) and getattr(self.task_fetcher, "timezone", None):
                end_time = self.task_fetcher.now()
            else:
                end_time = datetime.now()

            # Watermark settings (CLI overrides config; config overrides defaults)
            if watermark_path is None:
                watermark_path = (self._clickhouse_config or {}).get("watermark_path")
                if watermark_path and not os.path.isabs(watermark_path):
                    # Interpret config-relative paths relative to db_config.yaml location
                    base_dir = os.path.dirname(os.path.abspath(self._db_config_path))
                    watermark_path = os.path.normpath(os.path.join(base_dir, watermark_path))
            if watermark_enabled and (self._clickhouse_config or {}).get("watermark_enabled") is False:
                watermark_enabled = False
            if (self._clickhouse_config or {}).get("watermark_overlap_seconds") is not None:
                try:
                    watermark_overlap_seconds = int((self._clickhouse_config or {}).get("watermark_overlap_seconds"))
                except Exception:
                    pass
            watermark_overlap_seconds = max(0, int(watermark_overlap_seconds))
            if (self._clickhouse_config or {}).get("watermark_max_window_hours") is not None:
                try:
                    watermark_max_window_hours = float((self._clickhouse_config or {}).get("watermark_max_window_hours"))
                except Exception:
                    pass
            # <=0 means unlimited
            watermark_max_window_hours = float(watermark_max_window_hours)

            store = FileWatermarkStore(watermark_path) if (watermark_enabled and watermark_path) else None
            if store and watermark_reset:
                store.reset()
                logger.warning(f"Watermark reset: {watermark_path}")

            fetch_start_time = None
            watermark_base_time = None
            if store:
                state = store.load()
                if state and state.last_end_time:
                    watermark_base_time = state.last_end_time
                    # Align tzinfo defensively (avoid naive/aware arithmetic errors)
                    try:
                        if end_time.tzinfo and watermark_base_time.tzinfo is None:
                            watermark_base_time = watermark_base_time.replace(tzinfo=end_time.tzinfo)
                        if (end_time.tzinfo is None) and (watermark_base_time.tzinfo is not None):
                            watermark_base_time = watermark_base_time.replace(tzinfo=None)
                    except Exception:
                        pass

                    fetch_start_time = watermark_base_time
                    if watermark_overlap_seconds > 0:
                        fetch_start_time = fetch_start_time - timedelta(seconds=watermark_overlap_seconds)
                        logger.info(f"Using watermark with overlap {watermark_overlap_seconds}s, start_time={fetch_start_time}")
                    else:
                        logger.info(f"Using watermark, start_time={fetch_start_time}")
                elif watermark_init_now:
                    # Cold start shortcut: initialize watermark to now and skip historical backfill.
                    store.save(end_time)
                    logger.warning(f"Watermark initialized to now: {watermark_path} -> {end_time}")
                    completed_tasks = []
                    jobs = []
                    logger.warning("No audit jobs to execute (watermark init only)")
                    return {'total': 0, 'success': 0, 'partial': 0, 'failed': 0}

            # Fallback to lookback window if no watermark
            if fetch_start_time is None:
                fetch_start_time = end_time - timedelta(hours=float(hours_lookback))
                logger.info(f"Using lookback window start_time={fetch_start_time} (hours_lookback={hours_lookback})")
            else:
                # If watermark is too old, process in chunks to avoid huge backfill in one run.
                # We base the chunk on the non-overlapped watermark_base_time so we never skip data.
                if watermark_base_time and watermark_max_window_hours > 0:
                    desired_end = end_time
                    max_end = watermark_base_time + timedelta(hours=float(watermark_max_window_hours))
                    if desired_end > max_end:
                        end_time = max_end
                        logger.warning(
                            f"Watermark lag is large; limiting this run window to {watermark_max_window_hours}h. "
                            f"Window: {fetch_start_time} to {end_time}. Next runs will continue catching up."
                        )

            # Guard: watermark in future -> fallback
            if fetch_start_time >= end_time:
                logger.warning(
                    f"Invalid time window (start_time >= end_time). "
                    f"start_time={fetch_start_time}, end_time={end_time}. Falling back to lookback."
                )
                fetch_start_time = end_time - timedelta(hours=float(hours_lookback))

            logger.info(f"Fetching completed tasks from ClickHouse ({fetch_start_time} to {end_time})")
            completed_tasks = self.task_fetcher.get_completed_tasks(fetch_start_time, end_time, resolved_date)
            logger.info(f"Fetched {len(completed_tasks)} completed tasks from ClickHouse")
        else:
            # Default: use all configured tasks
            completed_tasks = self.config_loader.get_all_task_names()
            logger.info(f"Using all configured tasks: {completed_tasks}")
        
        # Build audit jobs
        jobs = self.config_loader.build_audit_jobs(completed_tasks, resolved_date)
        logger.info(f"Generated {len(jobs)} audit jobs")
        
        if not jobs:
            logger.warning("No audit jobs to execute")
            # Even if no jobs, advance watermark on success to avoid re-querying the same window forever.
            try:
                if (
                    watermark_enabled
                    and watermark_path
                    and isinstance(self.task_fetcher, ClickHouseTaskFetcher)
                    and 'end_time' in locals()
                    and not task_names
                ):
                    FileWatermarkStore(watermark_path).save(end_time)
                    logger.info(f"Watermark updated (no jobs): {watermark_path} -> {end_time}")
            except Exception as e:
                logger.warning(f"Failed to update watermark (no jobs): {e}")
            return {'total': 0, 'success': 0, 'partial': 0, 'failed': 0}
        
        # Get concurrency setting
        if concurrency is None:
            concurrency = self.config_loader.get_python_concurrency()
        
        # Apply safety clamps for concurrency vs jar threads
        concurrency = max(1, int(concurrency))
        concurrency = self.config_loader.clamp_python_concurrency(concurrency)
        max_threads_in_jobs = max((int(j.get('threads', 1)) for j in jobs), default=1)
        concurrency = self.config_loader.clamp_effective_parallelism(concurrency, max_threads_in_jobs)
        
        logger.info(
            f"Python concurrency: {concurrency} (max job threads={max_threads_in_jobs}, "
            f"effective={concurrency * max_threads_in_jobs})"
        )
        
        # Execute jobs
        results = {
            'total': len(jobs),
            'success': 0,
            'partial': 0,
            'failed': 0,
            'details': []
        }
        
        if dry_run:
            # Dry run mode - just print jobs
            for job in jobs:
                logger.info(f"[DRY RUN] Would execute: {job}")
                results['details'].append({
                    'table': job['table_name'],
                    'path': job['hdfs_path'],
                    'interface_id': job.get('interface_id', ''),
                    'partner_id': job.get('partner_id', ''),
                    'province_id': job.get('province_id', ''),
                    'status': 'dry_run'
                })
        elif concurrency <= 1:
            # Serial execution
            results = self._run_serial(jobs, results)
        else:
            # Parallel execution
            results = self._run_parallel(jobs, results, concurrency)
        
        # Summary
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Audit run completed in {duration:.1f}s. "
            f"Total: {results['total']}, "
            f"Success: {results['success']}, "
            f"Partial: {results['partial']}, "
            f"Failed: {results['failed']}"
        )

        # Advance watermark only when the run is successful (avoid missing tasks on retry)
        try:
            if (
                watermark_enabled
                and watermark_path
                and isinstance(self.task_fetcher, ClickHouseTaskFetcher)
                and 'end_time' in locals()
                and results.get('failed', 0) == 0
                and not task_names
            ):
                FileWatermarkStore(watermark_path).save(end_time)
                logger.info(f"Watermark updated: {watermark_path} -> {end_time}")
        except Exception as e:
            logger.warning(f"Failed to update watermark: {e}")
        
        return results
    
    def _run_serial(self, jobs: List[Dict[str, Any]], results: dict) -> dict:
        """Run jobs serially (one by one)"""
        for i, job in enumerate(jobs, 1):
            logger.info(f"Processing job {i}/{len(jobs)}: {job['table_name']} (province_id={job.get('province_id', '')})")
            
            try:
                result = self.counter_client.count_job(job)
                self.db_writer.write_job_result(job, result)
                self._update_results(results, job, result)
            except Exception as e:
                logger.error(f"Error processing job {job['table_name']}: {e}")
                results['failed'] += 1
                results['details'].append({
                    'table': job['table_name'],
                    'path': job['hdfs_path'],
                    'interface_id': job.get('interface_id', ''),
                    'partner_id': job.get('partner_id', ''),
                    'province_id': job.get('province_id', ''),
                    'status': 'error',
                    'error': str(e)
                })
        
        return results
    
    def _run_parallel(self, jobs: List[Dict[str, Any]], results: dict, 
                      concurrency: int) -> dict:
        """Run jobs in parallel using thread pool"""
        logger.info(f"Starting parallel execution with {concurrency} workers")
        
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            # Submit all jobs
            future_to_job = {
                executor.submit(self._process_single_job, job): job 
                for job in jobs
            }
            
            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                completed += 1
                
                try:
                    job, result = future.result()
                    # Write to database (thread-safe with connection pool)
                    self.db_writer.write_job_result(job, result)
                    self._update_results(results, job, result)
                    logger.info(f"Completed {completed}/{len(jobs)}: {job['table_name']} - {result.status}")
                except Exception as e:
                    logger.error(f"Error processing job {job['table_name']}: {e}")
                    results['failed'] += 1
                    results['details'].append({
                        'table': job['table_name'],
                        'path': job['hdfs_path'],
                        'interface_id': job.get('interface_id', ''),
                        'partner_id': job.get('partner_id', ''),
                        'province_id': job.get('province_id', ''),
                        'status': 'error',
                        'error': str(e)
                    })
        
        return results
    
    def _update_results(self, results: dict, job: Dict[str, Any], 
                        result: CounterResult) -> None:
        """Update results dict with job result"""
        if result.is_success():
            results['success'] += 1
        elif result.is_partial():
            results['partial'] += 1
        else:
            results['failed'] += 1
        
        results['details'].append({
            'table': job['table_name'],
            'path': job['hdfs_path'],
            'interface_id': job.get('interface_id', ''),
            'partner_id': job.get('partner_id', ''),
            'province_id': job.get('province_id', ''),
            'row_count': result.row_count,
            'status': result.status
        })
    
    def close(self):
        """Clean up resources"""
        if self.db_writer:
            self.db_writer.close()
        if isinstance(self.task_fetcher, ClickHouseTaskFetcher):
            self.task_fetcher.close()


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='HDFS Data Audit Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run audit for yesterday's data (fetch completed tasks from ClickHouse)
  python main.py
  
  # Run audit for specific date
  python main.py --date 20260115
  
  # Run audit for specific tasks only (skip ClickHouse query)
  python main.py --tasks dw_user_daily,dw_order_daily
  
  # Run with 10 concurrent jobs
  python main.py --concurrency 10
  
  # Look back 48 hours for completed tasks
  python main.py --hours-lookback 48
  
  # Dry run (print jobs without executing)
  python main.py --dry-run
  
  # Use custom configuration paths
  python main.py --config /path/to/config.yaml --db-config /path/to/db_config.yaml
        """
    )
    
    parser.add_argument(
        '--date', '-d',
        help='Data date in YYYYMMDD format (default: yesterday)'
    )
    
    parser.add_argument(
        '--tasks', '-t',
        help='Comma-separated list of task names to process (overrides ClickHouse query)'
    )
    
    parser.add_argument(
        '--config', '-c',
        default='../config/config.yaml',
        help='Path to config.yaml (default: ../config/config.yaml)'
    )
    
    parser.add_argument(
        '--db-config',
        default='../config/db_config.yaml',
        help='Path to db_config.yaml (default: ../config/db_config.yaml)'
    )
    
    parser.add_argument(
        '--jar',
        default='../java/hdfs-counter/target/hdfs-counter-1.0.0.jar',
        help='Path to hdfs-counter.jar'
    )
    
    parser.add_argument(
        '--java-home',
        help='JAVA_HOME path (optional)'
    )
    
    parser.add_argument(
        '--hadoop-conf-dir',
        help='HADOOP_CONF_DIR path (optional)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print jobs without executing'
    )
    
    parser.add_argument(
        '--concurrency', '-n',
        type=int,
        help='Number of concurrent jobs (overrides config python_concurrency)'
    )
    
    parser.add_argument(
        '--hours-lookback',
        type=float,
        default=24.0,
        help='Hours to look back for completed tasks in ClickHouse (default: 24.0). Supports decimals, e.g. 0.5'
    )

    parser.add_argument(
        '--watermark-path',
        help='Path to ClickHouse watermark JSON file (enables incremental fetching when set). '
             'Can also be set in db_config.yaml: clickhouse.watermark_path'
    )

    parser.add_argument(
        '--watermark-overlap-seconds',
        type=int,
        default=600,
        help='Overlap seconds when using watermark to tolerate ingestion delay (default: 600)'
    )

    parser.add_argument(
        '--watermark-max-window-hours',
        type=float,
        default=24.0,
        help='Max hours per run when catching up from an old watermark (default: 24.0). '
             'Set <=0 for unlimited.'
    )

    parser.add_argument(
        '--watermark-init-now',
        action='store_true',
        help='If watermark file does not exist, initialize it to now and exit (skip historical backfill)'
    )

    parser.add_argument(
        '--disable-watermark',
        action='store_true',
        help='Disable watermark incremental fetching even if watermark_path is configured'
    )

    parser.add_argument(
        '--watermark-reset',
        action='store_true',
        help='Reset (delete) watermark file before running'
    )
    
    parser.add_argument(
        '--skip-clickhouse',
        action='store_true',
        help='Skip ClickHouse task fetching and audit all configured tasks/tables from config.yaml'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Resolve paths relative to script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    def resolve_path(path: str) -> str:
        if os.path.isabs(path):
            return path
        return os.path.normpath(os.path.join(script_dir, path))
    
    config_path = resolve_path(args.config)
    db_config_path = resolve_path(args.db_config)
    jar_path = resolve_path(args.jar)
    watermark_path = resolve_path(args.watermark_path) if args.watermark_path else None
    
    # Parse task names
    task_names = None
    if args.tasks:
        task_names = [t.strip() for t in args.tasks.split(',')]
    
    try:
        # Initialize runner
        runner = HdfsAuditRunner(
            config_path=config_path,
            db_config_path=db_config_path,
            jar_path=jar_path,
            java_home=args.java_home,
            hadoop_conf_dir=args.hadoop_conf_dir
        )
        
        # Run audit
        results = runner.run(
            data_date=args.date,
            task_names=task_names,
            dry_run=args.dry_run,
            concurrency=args.concurrency,
            hours_lookback=args.hours_lookback,
            watermark_enabled=(not args.disable_watermark),
            watermark_path=watermark_path,
            watermark_overlap_seconds=args.watermark_overlap_seconds,
            watermark_max_window_hours=args.watermark_max_window_hours,
            watermark_init_now=args.watermark_init_now,
            watermark_reset=args.watermark_reset,
            skip_clickhouse=args.skip_clickhouse
        )
        
        # Print summary
        print("\n" + "=" * 60)
        print("AUDIT SUMMARY")
        print("=" * 60)
        print(f"Total jobs:    {results['total']}")
        print(f"Success:       {results['success']}")
        print(f"Partial:       {results['partial']}")
        print(f"Failed:        {results['failed']}")
        print("=" * 60)
        
        # Print details
        if results['details']:
            print("\nDetails:")
            for detail in results['details']:
                status_icon = "✓" if detail['status'] == 'success' else "✗" if detail['status'] in ('failed', 'error') else "○"
                province_info = f" [province:{detail.get('province_id', '')}]" if detail.get('province_id') else ""
                print(f"  {status_icon} {detail['table']}{province_info}: {detail.get('row_count', 'N/A')} rows - {detail['status']}")
        
        # Exit code based on results
        if results['failed'] > 0:
            sys.exit(1)
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Audit failed: {e}")
        sys.exit(1)
    finally:
        if 'runner' in locals():
            runner.close()


if __name__ == '__main__':
    main()
