#!/usr/bin/env python3
"""
config.py: Application configuration for the NUMA-Optimized WARC Downloader & Cleaner.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
import psutil

# Constants
DEFAULT_DASHBOARD_REFRESH_HZ = 1
DEFAULT_STATS_SAVE_INTERVAL_S = 15
DOWNLOAD_RETRY_MAX = 5
DOWNLOAD_RETRY_WAIT_BASE_S = 3
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1MB
MIN_CLEAN_TEXT_LENGTH = 100
BASE_COMMON_CRAWL_URL = "https://data.commoncrawl.org/"

class AppConfig:
    """Loads and stores application configuration."""
    def __init__(self):
        self.SCRIPT_DIR = Path(__file__).resolve().parent
        self._parse_args()
        self._set_paths_and_sizes()
        self._validate_thread_counts()

       
        self.BASE_COMMON_CRAWL_URL = BASE_COMMON_CRAWL_URL
        self.DOWNLOAD_RETRY_MAX = DOWNLOAD_RETRY_MAX
        self.DOWNLOAD_RETRY_WAIT_BASE_S = DOWNLOAD_RETRY_WAIT_BASE_S
        self.DOWNLOAD_CHUNK_SIZE = DOWNLOAD_CHUNK_SIZE
        self.MIN_CLEAN_TEXT_LENGTH = MIN_CLEAN_TEXT_LENGTH

    def _parse_args(self):
        parser = argparse.ArgumentParser(description="NUMA-Optimized WARC Downloader & Cleaner.")
        parser.add_argument("--download-threads", type=int, default=4, help="Number of concurrent download threads (will be affinitized to Node 0).")
        parser.add_argument("--reserved-threads-total", type=int, default=4, help="Total threads reserved for system (not used by download/clean), distributed.")
        parser.add_argument("--init-cleaned-scan", action="store_true", help="Scan OUTPUT_DIR for already cleaned .txt files on startup if tracker is missing.")
        parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level.")
        parser.add_argument("--dashboard-refresh-hz", type=float, default=DEFAULT_DASHBOARD_REFRESH_HZ, help="Dashboard refresh rate in Hz.")
        parser.add_argument("--stats-save-interval-s", type=int, default=DEFAULT_STATS_SAVE_INTERVAL_S, help="Interval for periodically saving stats in seconds.")
        parser.add_argument("--ramdisk-size-gb", type=int, default=25, help="Size in GB for EACH NUMA RAM disk.")
        parser.add_argument("--max-cleaner-write-sem", type=int, default=40, help="Max concurrent writes by all cleaners to final storage.")
       
        parser.add_argument("--pipeline-depth-multiplier", type=int, default=3, help="Multiplier for download threads to set total in-flight files (pipeline depth).")

        args = parser.parse_args()
        self.DOWNLOAD_THREADS_CONFIG = args.download_threads
        self.RESERVED_THREADS_TOTAL_CONFIG = args.reserved_threads_total
        self.INITIAL_CLEANED_SCAN = args.init_cleaned_scan
        self.LOG_LEVEL = args.log_level.upper()
        self.DASHBOARD_REFRESH_RATE_HZ = args.dashboard_refresh_hz
        self.STATS_SAVE_INTERVAL_S = args.stats_save_interval_s
        self.RAMDISK_SIZE_EACH_GB = args.ramdisk_size_gb
        self.WRITE_SEMAPHORE_VALUE = args.max_cleaner_write_sem
       
        self.PIPELINE_DEPTH_MULTIPLIER = args.pipeline_depth_multiplier

    def _set_paths_and_sizes(self):
        self.WARC_LIST_FILE = self.SCRIPT_DIR / 'warc.paths'
        self.OUTPUT_DIR = Path('/media/unknown/DataDrive_SAS/cleaned_data')
        self.RAMDISK_NODE0_PATH = Path('/mnt/ramdisk_node0')
        self.RAMDISK_NODE1_PATH = Path('/mnt/ramdisk_node1')
        self.RAMDISK_SIZE_STR = f'{self.RAMDISK_SIZE_EACH_GB}G'
        self.LOG_DIR = self.SCRIPT_DIR / "logs_warc_pipeline"
        self.ERROR_LOG_FILE = self.LOG_DIR / 'pipeline_errors.log'
        self.STATS_FILE = self.LOG_DIR / 'master_stats.json'
        self.CLEANED_FILES_TRACKER_FILE = self.LOG_DIR / 'cleaned_files.json'

    def _validate_thread_counts(self):
        self.NUM_LOGICAL_CPUS = psutil.cpu_count(logical=True)
        self.NUM_PHYSICAL_CPUS = psutil.cpu_count(logical=False)
        if self.NUM_LOGICAL_CPUS is None or self.NUM_PHYSICAL_CPUS is None :
             logging.critical("Could not determine CPU counts. Exiting.")
             sys.exit(1)
        if self.NUM_LOGICAL_CPUS < 2 :
             logging.warning("System has fewer than 2 logical CPUs. NUMA-specific optimizations may not be effective or applicable.")
             self.CORES_PER_NODE = self.NUM_LOGICAL_CPUS
        else:
            self.CORES_PER_NODE = self.NUM_LOGICAL_CPUS // 2

        if self.CORES_PER_NODE == 0 and self.NUM_LOGICAL_CPUS > 0:
            self.CORES_PER_NODE = self.NUM_LOGICAL_CPUS

        if self.DOWNLOAD_THREADS_CONFIG >= self.CORES_PER_NODE and self.NUM_LOGICAL_CPUS >=2 :
            logging.warning(f"Download threads ({self.DOWNLOAD_THREADS_CONFIG}) >= cores on Node 0 ({self.CORES_PER_NODE}). Reducing download threads.")
            self.DOWNLOAD_THREADS_CONFIG = max(1, self.CORES_PER_NODE - 1 if self.CORES_PER_NODE > 1 else 1)

        reserved_per_node = self.RESERVED_THREADS_TOTAL_CONFIG // 2 if self.NUM_LOGICAL_CPUS >=2 else self.RESERVED_THREADS_TOTAL_CONFIG
        self.CLEAN_CPUS_NODE0 = max(1, self.CORES_PER_NODE - self.DOWNLOAD_THREADS_CONFIG - reserved_per_node)
        if self.NUM_LOGICAL_CPUS >= 2 :
            self.CLEAN_CPUS_NODE1 = max(0, (self.NUM_LOGICAL_CPUS - self.CORES_PER_NODE) - reserved_per_node)
        else:
            self.CLEAN_CPUS_NODE1 = 0

        if self.CLEAN_CPUS_NODE0 == 0 and self.CLEAN_CPUS_NODE1 == 0 and self.NUM_LOGICAL_CPUS > self.DOWNLOAD_THREADS_CONFIG + self.RESERVED_THREADS_TOTAL_CONFIG:
            logging.warning("No CPUs allocated for cleaning despite available cores. Check thread/reserved core configuration calculations.")
        elif self.CLEAN_CPUS_NODE0 == 0 and self.CLEAN_CPUS_NODE1 == 0 :
             logging.warning("No CPUs available for cleaning based on current configuration (download/reserved threads).")

        logging.info(f"CPU Config: Logical={self.NUM_LOGICAL_CPUS}, Physical={self.NUM_PHYSICAL_CPUS}, Cores/Node (Est.): {self.CORES_PER_NODE if self.NUM_LOGICAL_CPUS >=2 else self.NUM_LOGICAL_CPUS}")
        logging.info(f"Thread Allocation: Download (on Node 0 or all)={self.DOWNLOAD_THREADS_CONFIG}")
        logging.info(f"Cleaner Allocation: Node 0 Cleaners={self.CLEAN_CPUS_NODE0}, Node 1 Cleaners={self.CLEAN_CPUS_NODE1}")
        logging.info(f"Total Reserved Cores (Approx. Distributed): {self.RESERVED_THREADS_TOTAL_CONFIG}")
