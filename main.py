#!/usr/bin/env python3
"""
main.py: Orchestrator for the NUMA-Optimized WARC Downloader & Cleaner.
"""
from __future__ import annotations
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
import warnings
from multiprocessing import (Queue as MPQueue, RLock as MPRLock, Lock as MPLock,
                             Event as MPEvent, BoundedSemaphore as MPBoundedSemaphore)
from multiprocessing.managers import SyncManager
from config import AppConfig
from utils import SystemUtils, RamDiskManager, CoreAllocator
from storage import StatsController
from extract import TextExtractor
from download import Downloader
from clean import CleanerProcess
from dashboard import RichDashboard
from writer import WriterProcess 
from bs4 import MarkupResemblesLocatorWarning, XMLParsedAsHTMLWarning

# Constants for main app
LOG_FORMAT_VERBOSE = '[%(asctime)s] %(levelname)s: %(threadName)s %(processName)s %(filename)s:%(lineno)d - %(message)s'
LOG_FORMAT_SIMPLE = '[%(asctime)s] %(levelname)s: %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
THREAD_JOIN_TIMEOUT_S = 10
PROCESS_JOIN_TIMEOUT_S = 10
PROCESS_FORCE_TERMINATE_TIMEOUT_S = 5

initial_stdout_handler_global: logging.Handler | None = None

def setup_main_logging(config_obj: AppConfig) -> logging.Handler | None:
    global initial_stdout_handler_global
    config_obj.LOG_DIR.mkdir(parents=True, exist_ok=True)

    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

    root_logger = logging.getLogger()
    root_logger.setLevel(config_obj.LOG_LEVEL)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    file_handler = logging.FileHandler(config_obj.ERROR_LOG_FILE, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT_VERBOSE, datefmt=DATE_FORMAT))
    file_handler.setLevel(logging.WARNING)
    root_logger.addHandler(file_handler)

    console_stdout_handler = logging.StreamHandler(sys.stdout)
    console_stdout_handler.setFormatter(logging.Formatter(LOG_FORMAT_SIMPLE, datefmt=DATE_FORMAT))
    console_stdout_handler.setLevel(config_obj.LOG_LEVEL)
    root_logger.addHandler(console_stdout_handler)

    initial_stdout_handler_global = console_stdout_handler
    return console_stdout_handler

class WARCPipelineManager(SyncManager):
    pass

WARCPipelineManager.register('StatsController', StatsController)

class PipelineApp:
    """Orchestrates the entire NUMA-aware download and cleaning pipeline."""
    def __init__(self):
        self.config = AppConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.core_allocator = CoreAllocator(self.config)

        self.mp_manager: "WARCPipelineManager | None" = None
        self.stats_controller_proxy: StatsController | None = None
        self.mp_stop_event_periodic_save: "multiprocessing.Event | None" = None
        self.ramdisk_node0_queue: "multiprocessing.Queue | None" = None
        self.ramdisk_node1_queue: "multiprocessing.Queue | None" = None
        self.writer_queue: "multiprocessing.Queue | None" = None 
        self.active_cleaner_tasks_view: dict[int, str] | None = None
        self.active_cleaner_tasks_lock: "multiprocessing.RLock | None" = None
        self.cleaner_feedback_queue: "multiprocessing.Queue | None" = None
        
        self.download_slot_semaphore: "multiprocessing.BoundedSemaphore | None" = None

        self.ramdisk_node0_mgr: RamDiskManager | None = None
        self.ramdisk_node1_mgr: RamDiskManager | None = None
        self.downloader_component: Downloader | None = None
        self.cleaner_processes_node0: list[CleanerProcess] = []
        self.cleaner_processes_node1: list[CleanerProcess] = []
        self.writer_process: WriterProcess | None = None 
        self.dashboard_ui: RichDashboard | None = None

        self.stats_saver_thread: threading.Thread | None = None
        self.active_task_monitor_thread: threading.Thread | None = None

        self.stop_event_main_loop = threading.Event()
        self.stop_event_downloader_pool = threading.Event()
        self.stop_event_cleaners_and_writer = multiprocessing.Event()
        self.stop_event_dashboard_thread = threading.Event()
        self.stop_event_stats_saver_thread = threading.Event()
        self.stop_event_task_monitor_thread = threading.Event()

    def _setup_multiprocessing_resources(self):
        self.logger.info("Setting up multiprocessing manager and shared IPC resources...")
        try:
            # Setup multiprocessing start method
            start_method = os.environ.get("MP_START_METHOD", "spawn" if sys.platform in ["darwin", "win32"] else "fork")
            current_method = multiprocessing.get_start_method(allow_none=True)
            if current_method != start_method:
                multiprocessing.set_start_method(start_method, force=True)
            self.logger.info(f"Using multiprocessing start method: '{multiprocessing.get_start_method()}'.")

            self.mp_manager = WARCPipelineManager()
            self.mp_manager.start()
            self.logger.info("Custom WARCPipelineManager started.")

        except Exception as e:
            self.logger.critical(f"Failed to initialize or start multiprocessing manager: {e}", exc_info=True)
            raise

        q_size_base = max(1, self.config.CORES_PER_NODE) if self.config.CORES_PER_NODE > 0 else 2
        clean_cores_total = self.config.CLEAN_CPUS_NODE0 + self.config.CLEAN_CPUS_NODE1

        self.ramdisk_node0_queue = self.mp_manager.Queue(maxsize=q_size_base * 2)
        self.ramdisk_node1_queue = self.mp_manager.Queue(maxsize=q_size_base * 2)
        self.writer_queue = self.mp_manager.Queue(maxsize=clean_cores_total * 2) 

        self.cleaner_feedback_queue = self.mp_manager.Queue()
        self.active_cleaner_tasks_lock = self.mp_manager.RLock()
        self.active_cleaner_tasks_view = self.mp_manager.dict()

        download_slots = self.config.DOWNLOAD_THREADS_CONFIG * self.config.PIPELINE_DEPTH_MULTIPLIER
        self.download_slot_semaphore = self.mp_manager.BoundedSemaphore(download_slots)
        self.logger.info(f"Initialized download slot semaphore with {download_slots} slots (Threads: {self.config.DOWNLOAD_THREADS_CONFIG} * Depth: {self.config.PIPELINE_DEPTH_MULTIPLIER}).")

        self.stop_event_cleaners_and_writer = self.mp_manager.Event()
        self.mp_stop_event_periodic_save = self.mp_manager.Event()

        self.stats_controller_proxy = self.mp_manager.StatsController(
            self.config, self.mp_manager.RLock(), self.mp_manager.Lock()
        )
        self.logger.info("StatsController proxy created via custom manager.")

    def _initialize_pipeline_components(self):
        self.logger.info("Initializing pipeline components...")
        
        self.ramdisk_node0_mgr = RamDiskManager(self.config.RAMDISK_NODE0_PATH, self.config.RAMDISK_SIZE_STR, 0, self.logger)
        self.ramdisk_node1_mgr = RamDiskManager(self.config.RAMDISK_NODE1_PATH, self.config.RAMDISK_SIZE_STR, 1, self.logger)
        
        try:
            self.ramdisk_node0_mgr.mount()
            if self.core_allocator.is_dual_node_system: self.ramdisk_node1_mgr.mount()
        except Exception as e_mount:
            self.logger.critical(f"Essential RAM disk mounting failed: {e_mount}. Cannot continue.", exc_info=True)
            raise SystemExit(f"RAM Disk mounting failed: {e_mount}")

        self.downloader_component = Downloader(
            app_config=self.config, core_allocator=self.core_allocator,
            ramdisk_managers=[self.ramdisk_node0_mgr, self.ramdisk_node1_mgr],
            output_queues=[self.ramdisk_node0_queue, self.ramdisk_node1_queue],
            stats_controller_proxy=self.stats_controller_proxy,
            stop_event=self.stop_event_downloader_pool,
            download_slot_semaphore=self.download_slot_semaphore
        )

        self.writer_process = WriterProcess(
            writer_queue=self.writer_queue,
            stop_event=self.stop_event_cleaners_and_writer
        )

        # Pass the writer_queue to the cleaners
        for core_id_n0 in self.core_allocator.clean_cores_node0:
            cleaner_n0 = CleanerProcess(
                process_id=core_id_n0, numa_node_idx=0, assigned_core_id=core_id_n0,
                input_queue=self.ramdisk_node0_queue, writer_queue=self.writer_queue,
                app_config=self.config, stats_controller_proxy=self.stats_controller_proxy,
                active_task_feedback_queue=self.cleaner_feedback_queue,
                stop_event=self.stop_event_cleaners_and_writer,
                text_extractor_cls=TextExtractor,
                download_slot_semaphore=self.download_slot_semaphore
            )
            self.cleaner_processes_node0.append(cleaner_n0)
        
        if self.core_allocator.is_dual_node_system and self.config.CLEAN_CPUS_NODE1 > 0:
            for core_id_n1 in self.core_allocator.clean_cores_node1:
                cleaner_n1 = CleanerProcess(
                    process_id=core_id_n1, numa_node_idx=1, assigned_core_id=core_id_n1,
                    input_queue=self.ramdisk_node1_queue, writer_queue=self.writer_queue,
                    app_config=self.config, stats_controller_proxy=self.stats_controller_proxy,
                    active_task_feedback_queue=self.cleaner_feedback_queue,
                    stop_event=self.stop_event_cleaners_and_writer,
                    text_extractor_cls=TextExtractor,
                    download_slot_semaphore=self.download_slot_semaphore
                )
                self.cleaner_processes_node1.append(cleaner_n1)
        
        self.logger.info(f"Initialized {len(self.cleaner_processes_node0) + len(self.cleaner_processes_node1)} cleaners and 1 writer.")

        global initial_stdout_handler_global
        self.dashboard_ui = RichDashboard(
            app_config=self.config, stats_controller_proxy=self.stats_controller_proxy,
            ramdisk_managers=[self.ramdisk_node0_mgr, self.ramdisk_node1_mgr],
            active_cleaner_tasks_view=self.active_cleaner_tasks_view, active_cleaner_tasks_lock=self.active_cleaner_tasks_lock,
            downloader_ref=self.downloader_component,
            cleaner_queues=[self.ramdisk_node0_queue, self.ramdisk_node1_queue],
            stop_event=self.stop_event_dashboard_thread,
            initial_stdout_handler=initial_stdout_handler_global,
            system_utils_class=SystemUtils
        )
        
        self.stats_saver_thread = threading.Thread(
            target=self.stats_controller_proxy.periodic_save_all,
            args=(self.mp_stop_event_periodic_save, self.config.STATS_SAVE_INTERVAL_S),
            daemon=True, name="StatsSaverThread"
        )
        self.active_task_monitor_thread = threading.Thread(
            target=self._monitor_cleaner_feedback_for_dashboard,
            daemon=True, name="CleanerFeedbackMonitorThread"
        )

    def _monitor_cleaner_feedback_for_dashboard(self):
        self.logger.info("Cleaner feedback monitor for dashboard started.")
        while not self.stop_event_task_monitor_thread.is_set():
            try:
                core_id, filename, status = self.cleaner_feedback_queue.get(timeout=1.0)
                with self.active_cleaner_tasks_lock:
                    if status == 'start':
                        self.active_cleaner_tasks_view[core_id] = filename
                    elif status == 'end' and core_id in self.active_cleaner_tasks_view:
                        del self.active_cleaner_tasks_view[core_id]
            except multiprocessing.queues.Empty:
                continue
            except (EOFError, BrokenPipeError):
                self.logger.warning("Cleaner feedback queue closed. Monitor stopping.")
                break
        self.logger.info("Cleaner feedback monitor for dashboard stopped.")

    def _start_all_components(self):
        self.logger.info("Starting all pipeline components...")
        self.stats_saver_thread.start()
        self.active_task_monitor_thread.start()
        self.dashboard_ui.start()
        # Start the new writer process
        if self.writer_process: self.writer_process.start()
        all_cleaners = self.cleaner_processes_node0 + self.cleaner_processes_node1
        for cleaner_proc in all_cleaners: cleaner_proc.start()
        self.downloader_component.start()

    def _signal_stop_to_all_components(self):
        self.logger.info("Broadcasting stop signal to all components...")
        self.stop_event_main_loop.set()
        self.stop_event_downloader_pool.set()
        self.stop_event_cleaners_and_writer.set() 
        self.stop_event_dashboard_thread.set()
        self.mp_stop_event_periodic_save.set()
        self.stop_event_task_monitor_thread.set()

    def _join_threads_and_processes(self, final_shutdown: bool = False):
        self.logger.info("Joining threads and processes...")
        self.downloader_component.shutdown()

        aux_threads = [self.dashboard_ui, self.stats_saver_thread, self.active_task_monitor_thread]
        for t in aux_threads:
            if t: t.join(THREAD_JOIN_TIMEOUT_S)
        
        all_procs = self.cleaner_processes_node0 + self.cleaner_processes_node1
        # Add writer process to the list to be joined
        if self.writer_process: all_procs.append(self.writer_process)
        for proc in all_procs:
            if proc.is_alive():
                proc.join(PROCESS_JOIN_TIMEOUT_S)
                if proc.is_alive():
                    self.logger.warning(f"Process {proc.name} did not terminate gracefully. Forcing.")
                    proc.terminate()
                    proc.join(PROCESS_FORCE_TERMINATE_TIMEOUT_S)

    def _perform_final_cleanup(self):
        self.logger.info("Performing final resource cleanup...")
        if self.stats_controller_proxy: self.stats_controller_proxy.final_save_all()
        if self.ramdisk_node0_mgr: self.ramdisk_node0_mgr.unmount()
        if self.ramdisk_node1_mgr: self.ramdisk_node1_mgr.unmount()
        if self.mp_manager: self.mp_manager.shutdown()
        self.logger.info("Pipeline application final cleanup complete.")

    def _setup_signal_handlers(self):
        def graceful_signal_handler(signum, frame):
            signal_name = signal.Signals(signum).name
            self.logger.warning(f"\n>>> OS Signal Received: {signal_name}. Initiating Graceful Shutdown... <<<")
            if not self.stop_event_main_loop.is_set():
                self._signal_stop_to_all_components()
        signal.signal(signal.SIGINT, graceful_signal_handler)
        signal.signal(signal.SIGTERM, graceful_signal_handler)

    def run(self):
        self._setup_signal_handlers()
        try:
            self.logger.info("========= WARC Processing Pipeline Starting Up =========")
            self._setup_multiprocessing_resources()
            self._initialize_pipeline_components() 
            self._start_all_components()
            self.logger.info("Pipeline running. Monitoring for completion or shutdown signal...")

            while not self.stop_event_main_loop.is_set():
    # Check if a critical process has died
                if not self.writer_process.is_alive():
                    self.logger.critical("WriterProcess has died unexpectedly! Initiating shutdown.")
                    self.stop_event_main_loop.set()
                    break # Exit the loop immediately

                # Check if any cleaner process has died
                if any(not p.is_alive() for p in self.cleaner_processes_node0 + self.cleaner_processes_node1):
                    self.logger.critical("One or more CleanerProcesses have died! Initiating shutdown.")
                    self.stop_event_main_loop.set()
                    break # Exit the loop immediately

                downloader_finished = self.downloader_component.is_finished()
                if downloader_finished:
                    queues_empty = self.ramdisk_node0_queue.empty() and \
                                   self.ramdisk_node1_queue.empty() and \
                                   self.writer_queue.empty()
                    with self.active_cleaner_tasks_lock:
                        no_cleaners_active = len(self.active_cleaner_tasks_view) == 0
                    
                    if queues_empty and no_cleaners_active:
                        self.logger.info("All tasks complete. Initiating shutdown.")
                        time.sleep(2) # Give writer a moment to finish last items
                        self.stop_event_main_loop.set()
                        break
                time.sleep(2.0)
        except (KeyboardInterrupt, SystemExit) as e:
            self.logger.info(f"Shutdown triggered by {type(e).__name__}.")
        except Exception as e_main_run:
            self.logger.critical(f"CRITICAL Unhandled exception in main run loop: {e_main_run}", exc_info=True)
        finally:
            self.logger.info("========= Pipeline Shutting Down =========")
            if not self.stop_event_main_loop.is_set():
                self._signal_stop_to_all_components() 
            self._join_threads_and_processes(final_shutdown=True)
            self._perform_final_cleanup()
            self.logger.info("========= Pipeline Shutdown Complete =========")
            logging.shutdown()
            sys.exit(0)

if __name__ == "__main__":
    try:
        app_config = AppConfig()
        setup_main_logging(app_config)
    except Exception as e:
        logging.basicConfig(level="ERROR")
        logging.critical(f"Failed to initialize config or logging: {e}", exc_info=True)
        sys.exit(1)

    if os.geteuid() != 0:
        logging.warning("This script may require root for RAM disk operations and NUMA control.")

    try:
        app = PipelineApp()
        app.run() 
    except Exception as top_level_exception:
        logging.critical(f"TOP LEVEL UNHANDLED EXCEPTION: {top_level_exception}", exc_info=True)
        sys.exit(2)