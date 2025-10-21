#!/usr/bin/env python3
"""
dashboard.py: Rich live dashboard component.
"""
from __future__ import annotations

import logging
import os
import sys
import time
import threading
from datetime import timedelta
from rich.console import Console
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.live import Live
from rich.text import Text

from config import AppConfig
from storage import StatsController
from utils import RamDiskManager, SystemUtils
from download import Downloader

class RichDashboard:
    """Manages the Rich live dashboard display."""
    def __init__(self, app_config: AppConfig,
                 stats_controller_proxy: StatsController, 
                 ramdisk_managers: list[RamDiskManager], 
                 active_cleaner_tasks_view: dict, 
                 active_cleaner_tasks_lock: "multiprocessing.RLock",
                 downloader_ref: Downloader, 
                 cleaner_queues: list["multiprocessing.Queue"],
                 stop_event: "threading.Event", 
                 initial_stdout_handler: "logging.Handler | None", 
                 system_utils_class: type[SystemUtils]): 
        self.config = app_config
        self.stats_controller_proxy = stats_controller_proxy
        self.ramdisk_node0_mgr, self.ramdisk_node1_mgr = ramdisk_managers[0], ramdisk_managers[1]
        self.active_cleaner_tasks_view = active_cleaner_tasks_view
        self.active_cleaner_tasks_lock = active_cleaner_tasks_lock
        self.downloader = downloader_ref
        self.cleaner_queue_node0, self.cleaner_queue_node1 = cleaner_queues[0], cleaner_queues[1]
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.__class__.__name__)
        self.initial_stdout_handler = initial_stdout_handler
        self.dashboard_thread: threading.Thread | None = None
        self.live_display_object: Live | None = None
        self.system_utils = system_utils_class 

    def _generate_layout_content(self, layout_obj: Layout):
        cpu0_usage, cpu1_usage = self.system_utils.get_dual_cpu_usage(self.config.NUM_LOGICAL_CPUS)
        temp_cpu0, temp_cpu1 = self.system_utils.get_cpu_temperatures()
        
        rd0_free, rd0_total = self.ramdisk_node0_mgr.get_usage()
        rd1_free, rd1_total = self.ramdisk_node1_mgr.get_usage()
        rd0_usage_str = f"{rd0_free/(1024**2):.1f} / {rd0_total/(1024**2):.1f} MB free" if rd0_total > 0 else "N/A"
        rd1_usage_str = f"{rd1_free/(1024**2):.1f} / {rd1_total/(1024**2):.1f} MB free" if rd1_total > 0 else "N/A"

        stats_summary = self.stats_controller_proxy.get_summary()
        try:
            start_ts = self.stats_controller_proxy.get_start_timestamp()
            elapsed_seconds = time.time() - start_ts if start_ts else 0
        except Exception: 
            elapsed_seconds = 0


        stats_table = Table(title="Pipeline Overview", show_edge=True, expand=True, border_style="bright_blue", padding=(0,1))
        stats_table.add_column("Metric", style="cyan", no_wrap=True, min_width=20)
        stats_table.add_column("Value", style="green", min_width=25)
        stats_table.add_row("Elapsed Time", str(timedelta(seconds=int(elapsed_seconds))))
        stats_table.add_row("Data Downloaded", f"{stats_summary.get('total_data_downloaded_bytes', 0) / (1024**3):.2f} GB")
        stats_table.add_row("Data Removed (Est.)", f"{stats_summary.get('total_data_removed_bytes', 0) / (1024**3):.2f} GB")
        stats_table.add_row("WARCs Downloaded", str(stats_summary.get('total_warc_processed_for_download_count', 0)))
        stats_table.add_row("WARCs Cleaned", str(stats_summary.get('total_warc_successfully_cleaned_count', 0)))
        stats_table.add_row("CPU Usage (N0 | N1)", f"{cpu0_usage} | {cpu1_usage}")
        stats_table.add_row("CPU Temp (N0 | N1)", f"{temp_cpu0} | {temp_cpu1}")
        stats_table.add_row(f"RAMDisk Node 0 ({self.ramdisk_node0_mgr.numa_node})", rd0_usage_str)
        stats_table.add_row(f"RAMDisk Node 1 ({self.ramdisk_node1_mgr.numa_node})", rd1_usage_str)
        stats_table.add_row("DL Threads (Node 0)", str(self.config.DOWNLOAD_THREADS_CONFIG))
        stats_table.add_row("Cleaner Cores (N0|N1)", f"{self.config.CLEAN_CPUS_NODE0} | {self.config.CLEAN_CPUS_NODE1}")
        
        try:
            download_slots = self.download_slot_semaphore.get_value()
            stats_table.add_row("Download Slots Free", str(download_slots))
        except Exception:
            pass
        q0_size_str = 'N/A'
        if self.cleaner_queue_node0 and hasattr(self.cleaner_queue_node0, 'qsize'):
            try: q0_size_str = str(self.cleaner_queue_node0.qsize()) 
            except (NotImplementedError, FileNotFoundError, BrokenPipeError): q0_size_str = "Err" 
        
        q1_size_str = 'N/A'
        if self.cleaner_queue_node1 and hasattr(self.cleaner_queue_node1, 'qsize'):
            try: q1_size_str = str(self.cleaner_queue_node1.qsize())
            except (NotImplementedError, FileNotFoundError, BrokenPipeError): q1_size_str = "Err"

        stats_table.add_row("Cleaner Q (N0|N1)", f"{q0_size_str} | {q1_size_str}")
        
        cleaners_table = Table(title="Active Cleaning Workers", show_header=True, header_style="bold magenta", expand=True, border_style="magenta", padding=(0,1))
        cleaners_table.add_column("Core (Node)", style="dim cyan", width=12)
        cleaners_table.add_column("Cleaning WARC File", style="yellow", overflow="fold", min_width=30)
        
        active_tasks_snapshot = {}
        try:
            with self.active_cleaner_tasks_lock:
                active_tasks_snapshot = dict(self.active_cleaner_tasks_view) 
        except Exception: # Handle if lock or view becomes invalid (e.g. manager shutdown)
            self.logger.debug("Could not get active cleaner tasks snapshot for dashboard.")


        if not active_tasks_snapshot:
            cleaners_table.add_row("...", "All cleaner cores idle or no tasks reported.")
        else:
            for core_id, warc_filename in sorted(active_tasks_snapshot.items()):
                node_display = "N0" if core_id < self.config.CORES_PER_NODE else "N1"
                cleaners_table.add_row(f"{core_id} ({node_display})", warc_filename)
        
        layout_obj["left_panel"].update(Panel(stats_table, title="📊 Statistics", border_style="blue", expand=True))
        layout_obj["right_panel"].update(Panel(cleaners_table, title="🛠️ Active Cleaners", border_style="magenta", expand=True))

    def _run_dashboard_loop(self):
        root_logger = logging.getLogger()
        handler_removed_for_dashboard = False
        if self.initial_stdout_handler and self.initial_stdout_handler in root_logger.handlers:
            root_logger.removeHandler(self.initial_stdout_handler)
            handler_removed_for_dashboard = True
            self.logger.info("Temporarily removed initial stdout handler for Rich dashboard.")

       
        console = Console(file=sys.stderr, force_terminal=True if os.isatty(sys.stderr.fileno()) else False)
        
        layout = Layout(name="root_layout")
        layout.split_column(
            Layout(name="header_layout", size=3),
            Layout(name="main_content_area", ratio=1)
        )
        layout["main_content_area"].split_row(
            Layout(name="left_panel", ratio=1),
            Layout(name="right_panel", ratio=1)
        )
        title_text = Text("WARC Download & Clean Pipeline Dashboard (NUMA Optimized)", style="bold white on blue", justify="center")
        layout["header_layout"].update(Panel(title_text, style="blue"))

        try:
            with Live(layout, console=console, screen=False, vertical_overflow="visible", 
                      transient=False, auto_refresh=False, 
                      refresh_per_second=self.config.DASHBOARD_REFRESH_RATE_HZ) as live:
                self.live_display_object = live 
                while not self.stop_event.is_set():
                    self._generate_layout_content(layout)
                    live.refresh()
                    time.sleep(max(0.1, 1.0 / self.config.DASHBOARD_REFRESH_RATE_HZ)) 
        except KeyboardInterrupt:
            self.logger.info("Dashboard: KeyboardInterrupt signal received.")
        except Exception as e:
            self.logger.critical(f"Dashboard CRITICAL ERROR: {e}", exc_info=True)
            
            if not self.stop_event.is_set():
                 self.stop_event.set()
        finally:
            self.live_display_object = None

            if handler_removed_for_dashboard and self.initial_stdout_handler:
                if self.initial_stdout_handler not in root_logger.handlers:
                    root_logger.addHandler(self.initial_stdout_handler)
                    self.logger.info("Restored initial stdout handler after Rich dashboard.")
            self.logger.info("Dashboard thread stopped.")

    def start(self):

        if not sys.stderr.isatty():
            self.logger.warning("stderr is not a TTY. Rich dashboard may not display correctly or might be disabled.")

        
        self.logger.info("Starting Rich dashboard thread.")
        self.dashboard_thread = threading.Thread(target=self._run_dashboard_loop, daemon=True, name="RichDashboardThread")
        self.dashboard_thread.start()

    def stop(self): 
        self.logger.info("Rich dashboard stop signal processed.")

    def join(self, timeout=10):
        if self.dashboard_thread and self.dashboard_thread.is_alive():
            self.dashboard_thread.join(timeout)
            if self.dashboard_thread.is_alive():
                self.logger.warning("Dashboard thread did not terminate gracefully within timeout.")
    
    def is_alive(self) -> bool:
        return self.dashboard_thread is not None and self.dashboard_thread.is_alive()