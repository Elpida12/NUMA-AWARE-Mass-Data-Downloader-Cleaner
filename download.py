#!/usr/bin/env python3
"""
download.py: Downloader components (Task and Manager).
"""
from __future__ import annotations 

import logging
import os
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

from multiprocessing import Queue as MPQueue, Event as MPEvent, BoundedSemaphore as MPBoundedSemaphore
from threading import Event as ThreadEvent

from config import AppConfig
from utils import RamDiskManager, CoreAllocator, SystemUtils
from storage import StatsController

try:
    from multiprocessing.queues import Full as FullQueueException
except ImportError:
    FullQueueException = Exception

class DownloadTask:
    """Represents a single file download operation."""
    def __init__(self, relative_warc_path: str, app_config: AppConfig,
                 ram_disk_selector_fn: "Callable[[int], tuple[RamDiskManager | None, MPQueue | None]]",
                 stats_controller_proxy: StatsController, 
                 stop_event: "ThreadEvent | MPEvent", 
                 logger: logging.Logger,
                 session: "requests.Session",
                 download_slot_semaphore: "MPBoundedSemaphore"):
        self.relative_warc_path = relative_warc_path
        self.config = app_config
        self.ram_disk_selector_fn = ram_disk_selector_fn
        self.stats_controller_proxy = stats_controller_proxy
        self.stop_event = stop_event
        self.logger = logger
        self.warc_basename = Path(relative_warc_path).name
        self.session = session
        self.download_slot_semaphore = download_slot_semaphore

    def run(self) -> str | None:
        self.logger.debug(f"Attempting to acquire download slot for {self.warc_basename}...")

        slot_acquired = False
        while not self.stop_event.is_set():
            try:
                if self.download_slot_semaphore.acquire(timeout=1.0):
                    slot_acquired = True
                    break
            except (BrokenPipeError, EOFError):
                self.logger.warning("IPC channel for semaphore broke. Exiting task.")
                return None

        if not slot_acquired:
            self.logger.warning(f"Could not acquire download slot for {self.warc_basename}. Task will not run.")
            return None

        try:
            self.logger.info(f"Acquired download slot. Starting download task for: {self.warc_basename}")

            if self.stats_controller_proxy.is_already_cleaned(self.warc_basename):
                self.logger.info(f"{self.warc_basename} skipped, already marked as cleaned.")
                return None 

            if self.stop_event.is_set():
                self.logger.info(f"Stop signal received, skipping download of {self.warc_basename}")
                return None 

            full_url = self.config.BASE_COMMON_CRAWL_URL + self.relative_warc_path
            estimated_file_size = 0

            try:
                head_resp = self.session.head(full_url, timeout=20, allow_redirects=True)
                head_resp.raise_for_status()
                content_length = head_resp.headers.get('Content-Length')
                if content_length and content_length.isdigit():
                    estimated_file_size = int(content_length)
                else:
                    self.logger.warning(f"Content-Length header missing or invalid for {self.warc_basename}. Assuming 50MB.")
                    estimated_file_size = 50 * 1024 * 1024 
            except requests.exceptions.RequestException as e:
                self.logger.error(f"[HEAD FAILED] {self.warc_basename}: {e}. Skipping.")
                return None
            
            if estimated_file_size == 0:
                self.logger.warning(f"{self.warc_basename} reported size 0. Skipping.")
                return None

            target_ramdisk_mgr: RamDiskManager | None = None
            target_output_queue: MPQueue | None = None
            
            retry_max = self.config.DOWNLOAD_RETRY_MAX
            retry_wait_base = self.config.DOWNLOAD_RETRY_WAIT_BASE_S

            for attempt in range(retry_max):
                if self.stop_event.is_set(): return None
                
                target_ramdisk_mgr, target_output_queue = self.ram_disk_selector_fn(estimated_file_size)
                if target_ramdisk_mgr and target_output_queue:
                    if target_ramdisk_mgr.is_verified_mounted():
                        break
                    else:
                        self.logger.warning(f"Selected RAM disk {target_ramdisk_mgr.get_path()} for {self.warc_basename} is not verified mounted. Retrying...")
                        target_ramdisk_mgr = None 
                
                wait_time = min(20, retry_wait_base * (attempt + 1))
                self.logger.info(f"No suitable/mounted RAM disk for {self.warc_basename} ({estimated_file_size/(1024**2):.1f}MB). Retry {attempt+1}/{retry_max} in {wait_time}s...")
                if self.stop_event.wait(wait_time): return None
            
            if not target_ramdisk_mgr or not target_output_queue or not target_ramdisk_mgr.is_verified_mounted():
                self.logger.error(f"Failed to find usable RAM disk for {self.warc_basename} ({estimated_file_size/(1024**2):.1f}MB) after retries. Skipping.")
                return None

            ram_disk_path = target_ramdisk_mgr.get_path()
            destination_warc_path = ram_disk_path / self.warc_basename
            temp_download_path = destination_warc_path.with_suffix(destination_warc_path.suffix + '.part')

            self.logger.info(f"Downloading {self.warc_basename} ({estimated_file_size/(1024**2):.2f}MB) to {destination_warc_path.parent} (NUMA Node {target_ramdisk_mgr.numa_node})")
            
            actual_downloaded_bytes = 0
            try:
                if temp_download_path.exists(): temp_download_path.unlink()

                with open(temp_download_path, 'wb') as f_out:
                    with self.session.get(full_url, stream=True, timeout=(15, 300)) as resp:
                        resp.raise_for_status()
                        for chunk in resp.iter_content(chunk_size=self.config.DOWNLOAD_CHUNK_SIZE):
                            if self.stop_event.is_set():
                                self.logger.info(f"Download of {self.warc_basename} interrupted.")
                                if temp_download_path.exists(): temp_download_path.unlink(missing_ok=True)
                                return None
                            if chunk:
                                f_out.write(chunk)
                                actual_downloaded_bytes += len(chunk)
                
                temp_download_path.rename(destination_warc_path)
                self.logger.info(f"Completed download of {self.warc_basename} ({actual_downloaded_bytes/(1024**2):.2f}MB).")

            except requests.exceptions.RequestException as e:
                self.logger.error(f"[DOWNLOAD FAILED] {self.warc_basename}: {e}")
                if temp_download_path.exists(): temp_download_path.unlink(missing_ok=True)
                return None
            except OSError as e:
                self.logger.error(f"[FILE SYSTEM ERROR] during download of {self.warc_basename} to {temp_download_path}: {e}", exc_info=True)
                if temp_download_path.exists(): temp_download_path.unlink(missing_ok=True)
                return None
            except Exception as e:
                self.logger.error(f"[UNEXPECTED DOWNLOAD ERROR] for {self.warc_basename}: {e}", exc_info=True)
                if temp_download_path.exists(): temp_download_path.unlink(missing_ok=True)
                return None

            self.stats_controller_proxy.record_warc_downloaded(self.warc_basename, actual_downloaded_bytes)
            
            try:
                target_output_queue.put(str(destination_warc_path), timeout=10) 
                self.logger.info(f"{self.warc_basename} added to queue for NUMA Node {target_ramdisk_mgr.numa_node} cleaners.")
                
                slot_acquired = False 
                return self.warc_basename

            except FullQueueException:
                self.logger.error(f"Queue for RAM disk {ram_disk_path.name} is full. Cannot add {self.warc_basename}.")
                if destination_warc_path.exists():
                    self.logger.warning(f"Removing {destination_warc_path} as its processing queue is full.")
                    destination_warc_path.unlink(missing_ok=True)
                self.stats_controller_proxy.revert_warc_download_stats(self.warc_basename, actual_downloaded_bytes)
                return None
            except Exception as e:
                self.logger.error(f"Failed to queue {self.warc_basename} after download: {e}", exc_info=True)
                if destination_warc_path.exists():
                    destination_warc_path.unlink(missing_ok=True)
                self.stats_controller_proxy.revert_warc_download_stats(self.warc_basename, actual_downloaded_bytes)
                return None
        finally:
            if slot_acquired:
                self.logger.debug(f"Releasing download slot due to task failure/skip for {self.warc_basename}.")
            try:
                self.download_slot_semaphore.release()
            except Exception as e:
                self.logger.error(f"Error releasing download slot in finally block: {e}")


class Downloader:
    """Manages the overall download process using a thread pool, affinitized to Node 0."""
    def __init__(self, app_config: AppConfig, core_allocator: CoreAllocator,
                 ramdisk_managers: list[RamDiskManager], 
                 output_queues: list[MPQueue],
                 stats_controller_proxy: StatsController, 
                 stop_event: "ThreadEvent | MPEvent",
                 download_slot_semaphore: "MPBoundedSemaphore"):
        self.config = app_config
        self.core_allocator = core_allocator
        self.ramdisk_node0_mgr = ramdisk_managers[0]
        self.ramdisk_node1_mgr = ramdisk_managers[1]
        self.output_queue_node0 = output_queues[0]
        self.output_queue_node1 = output_queues[1]
        self.stats_controller_proxy = stats_controller_proxy
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.__class__.__name__)
        self.download_slot_semaphore = download_slot_semaphore
        
        self.shared_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.config.DOWNLOAD_THREADS_CONFIG,
            pool_maxsize=self.config.DOWNLOAD_THREADS_CONFIG * 2,
            max_retries=requests.adapters.Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        )
        self.shared_session.mount('http://', adapter)
        self.shared_session.mount('https://', adapter)

        self.thread_pool = ThreadPoolExecutor(
            max_workers=self.config.DOWNLOAD_THREADS_CONFIG, 
            thread_name_prefix='DownloaderTask',
            initializer=self._init_downloader_thread,
        )
        self.download_futures: list[ThreadPoolExecutor.Future] = []
        self._is_all_tasks_submitted = False
        self._monitor_thread: threading.Thread | None = None

    def _init_downloader_thread(self):
        pid = os.getpid()
        if self.core_allocator.download_cores_node0:
            try:
                SystemUtils.set_affinity(self.core_allocator.download_cores_node0, pid=pid)
                logging.debug(f"Downloader worker (PID {pid}, Thread {threading.get_ident()}) affinity set to Node 0 cores: {self.core_allocator.download_cores_node0}")
            except Exception as e:
                 logging.error(f"Error setting affinity in _init_downloader_thread for PID {pid}: {e}")
        else:
            logging.warning(f"No specific download_cores_node0 defined for downloader thread affinity (PID {pid}).")

    def _select_ramdisk_and_queue(self, file_size_bytes: int) -> tuple[RamDiskManager | None, MPQueue | None]:
        needed_space = int(file_size_bytes * 1.10)

        if self.ramdisk_node0_mgr.is_verified_mounted():
            free_node0, total_node0 = self.ramdisk_node0_mgr.get_usage()
            if total_node0 > 0 and free_node0 >= needed_space:
                self.logger.debug(f"Selected RAMDisk Node 0 for {file_size_bytes/(1024**2):.1f}MB file. Free: {free_node0/(1024**2):.1f}MB")
                return self.ramdisk_node0_mgr, self.output_queue_node0
        else:
            self.logger.debug(f"RAMDisk Node 0 ({self.ramdisk_node0_mgr.get_path()}) not verified mounted for selection.")

        if self.ramdisk_node1_mgr.is_verified_mounted():
            free_node1, total_node1 = self.ramdisk_node1_mgr.get_usage()
            if total_node1 > 0 and free_node1 >= needed_space:
                self.logger.debug(f"Selected RAMDisk Node 1 for {file_size_bytes/(1024**2):.1f}MB file. Free: {free_node1/(1024**2):.1f}MB")
                return self.ramdisk_node1_mgr, self.output_queue_node1
        else:
            self.logger.debug(f"RAMDisk Node 1 ({self.ramdisk_node1_mgr.get_path()}) not verified mounted for selection.")
        return None, None

    def start(self):
        self.logger.info(f"Downloader starting with {self.config.DOWNLOAD_THREADS_CONFIG} threads, targeting Node 0 cores: {self.core_allocator.download_cores_node0}")
        
        if self.core_allocator.download_cores_node0 and os.name != 'nt':
             try:
                 SystemUtils.set_affinity(self.core_allocator.download_cores_node0)
                 self.logger.info(f"Affinitized main Downloader process (PID {os.getpid()}) to Node 0 cores.")
             except Exception as e:
                 self.logger.warning(f"Could not affinitize main Downloader process: {e}")

        warc_paths_to_download = []
        try:
            if not self.config.WARC_LIST_FILE.exists():
                 self.logger.error(f"WARC paths file not found: {self.config.WARC_LIST_FILE}. No downloads.")
                 self._is_all_tasks_submitted = True
                 return

            with open(self.config.WARC_LIST_FILE, 'r', encoding='utf-8') as f:
                warc_paths_to_download = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            
            if not warc_paths_to_download:
                self.logger.warning(f"WARC list file {self.config.WARC_LIST_FILE} is empty. No downloads.")
                self._is_all_tasks_submitted = True
                return
            self.logger.info(f"Loaded {len(warc_paths_to_download)} WARC paths from {self.config.WARC_LIST_FILE}.")

        except Exception as e:
            self.logger.error(f"Error loading WARC list file {self.config.WARC_LIST_FILE}: {e}", exc_info=True)
            self._is_all_tasks_submitted = True
            return

        for rel_path in warc_paths_to_download:
            if self.stop_event.is_set():
                self.logger.info("Downloader stop signal received. No more tasks submitted.")
                break
            
            task = DownloadTask(
                relative_warc_path=rel_path,
                app_config=self.config,
                ram_disk_selector_fn=self._select_ramdisk_and_queue,
                stats_controller_proxy=self.stats_controller_proxy,
                stop_event=self.stop_event,
                logger=self.logger,
                session=self.shared_session,
                download_slot_semaphore=self.download_slot_semaphore
            )
            future = self.thread_pool.submit(task.run)
            self.download_futures.append(future)
        
        self._is_all_tasks_submitted = True
        self.logger.info(f"All {len(self.download_futures)} download tasks submitted.")

        self._monitor_thread = threading.Thread(target=self._monitor_completion, daemon=True, name="DownloaderFutureMonitor")
        self._monitor_thread.start()

    def _monitor_completion(self):
        for future in as_completed(self.download_futures):
            try:
                warc_basename_result = future.result(timeout=0.1)
                if warc_basename_result:
                    self.logger.debug(f"Download task for '{warc_basename_result}' completed and queued.")
            except TimeoutError:
                pass 
            except Exception as e:
                self.logger.error(f"A download task raised an unhandled exception: {e}", exc_info=True)
        
        self.logger.info("All submitted download tasks have finished processing.")

    def is_finished(self) -> bool:
        if not self._is_all_tasks_submitted:
            return False
        return all(f.done() for f in self.download_futures)

    def shutdown(self):
        self.logger.info("Downloader shutdown requested.")
        self.thread_pool.shutdown(wait=True) 
        self.logger.info("Downloader thread pool has been shut down.")
        if self.shared_session:
            self.shared_session.close()
            self.logger.debug("Shared requests session closed.")
