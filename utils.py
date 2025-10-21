#!/usr/bin/env python3
"""
utils.py: System utilities, RAM Disk management, and Core allocation logic.
"""
from __future__ import annotations 

import logging
import os
import re
import shutil
import subprocess
from pathlib import Path
from statistics import mean
import psutil


class SystemUtils:
    """Provides static methods for system-level operations."""
    @staticmethod
    def set_affinity(cores: list[int], pid: int | None = None):
        if not psutil: return
        if not cores:
            logging.debug(f"No cores specified for affinity setting (PID: {pid or os.getpid()}).")
            return
        try:
            p = psutil.Process(pid or os.getpid())
            current_affinity = p.cpu_affinity()
            if set(current_affinity) == set(cores):
                logging.debug(f"CPU affinity for PID {p.pid} already set to {cores}.")
                return
            p.cpu_affinity(cores)
            logging.debug(f"Set CPU affinity for PID {p.pid} to cores: {cores}")
        except (AttributeError, psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logging.warning(f"Failed to set CPU affinity for PID {pid or os.getpid()} to {cores}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error setting affinity for PID {pid or os.getpid()} to {cores}: {e}")

    @staticmethod
    def get_cpu_temperatures() -> tuple[str, str]:
        temps = {"cpu0": None, "cpu1": None}
        if not hasattr(psutil, "sensors_temperatures"): return "N/A", "N/A"
        try:
            all_temps = psutil.sensors_temperatures()
            if not all_temps: return "N/A", "N/A"

            if 'coretemp' in all_temps: 
                for reading in all_temps['coretemp']:
                    if 'Package id 0' in reading.label: temps['cpu0'] = reading.current
                    elif 'Package id 1' in reading.label: temps['cpu1'] = reading.current

            for amd_sensor_key in ['k10temp', 'zenpower']:
                if amd_sensor_key in all_temps:
                    tdie, tctl = None, None
                    for reading in all_temps[amd_sensor_key]:
                        if 'tdie' in reading.label.lower(): tdie = reading.current
                        if 'tctl' in reading.label.lower(): tctl = reading.current
                    if temps['cpu0'] is None: temps['cpu0'] = tdie if tdie is not None else tctl
                    break
        except Exception as e:
            logging.debug(f"Could not retrieve CPU temperatures: {e}")

        return (f"{temps['cpu0']:.1f}°C" if temps['cpu0'] is not None else "N/A",
                f"{temps['cpu1']:.1f}°C" if temps['cpu1'] is not None else "N/A")

    @staticmethod
    def get_dual_cpu_usage(num_logical_cpus: int) -> tuple[str, str]:
        if num_logical_cpus == 0: return "N/A", "N/A"

        per_core_percentages = psutil.cpu_percent(percpu=True, interval=0.1)

        if not per_core_percentages or len(per_core_percentages) != num_logical_cpus:
            overall_usage = psutil.cpu_percent(interval=0.1)
            return f"{overall_usage:.1f}% (Overall)", "N/A"

        midpoint_idx = num_logical_cpus // 2 if num_logical_cpus >= 2 else num_logical_cpus

        node0_cores_usage = per_core_percentages[:midpoint_idx]
        node1_cores_usage = per_core_percentages[midpoint_idx:] if num_logical_cpus >=2 else []

        cpu0_usage = mean(node0_cores_usage) if node0_cores_usage else 0.0
        cpu1_usage = mean(node1_cores_usage) if node1_cores_usage else 0.0

        if num_logical_cpus < 2:
            return f"{cpu0_usage:.1f}%", "N/A (Single Node)"
        return f"{cpu0_usage:.1f}%", f"{cpu1_usage:.1f}%"

class RamDiskManager:
    def __init__(self, path: Path, size_str: str, numa_node: int, logger: logging.Logger):
        self.path = path
        self.size_str = size_str
        self.numa_node = numa_node
        self.logger = logger
        self._is_mounted_verified = False

    def _check_numactl(self):
        if not shutil.which("numactl"):
            self.logger.critical("`numactl` is required but not found. Please install it (e.g., 'sudo apt install numactl').")
            raise FileNotFoundError("numactl not found. Cannot proceed with NUMA-aware RAM disks.")
        return True

    def _is_path_mounted(self) -> bool:
        try:
            with open('/proc/mounts', 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3 and parts[1] == str(self.path) and parts[2] == 'tmpfs':
                        return True
        except FileNotFoundError:
            self.logger.warning("/proc/mounts not found. Cannot verify existing tmpfs mounts accurately.")
        return False

    def mount(self):
        self._check_numactl()
        self.path.mkdir(parents=True, exist_ok=True)

        if self._is_path_mounted():
            self.logger.info(f"RAM disk {self.path} (tmpfs) already mounted.")
            self._is_mounted_verified = True
            return

        try:
            total_ram_bytes = psutil.virtual_memory().total
            size_gb_match = re.match(r"(\d+)[Gg]", self.size_str)
            if not size_gb_match:
                self.logger.critical(f"Invalid tmpfs size format: {self.size_str}. Expected e.g., '25G'.")
                raise ValueError(f"Invalid tmpfs size format: {self.size_str}")

            size_gb = int(size_gb_match.group(1))
            size_bytes = size_gb * (1024 ** 3)

            if size_bytes > total_ram_bytes * 0.85:
                self.logger.warning(f"Requested tmpfs size {self.size_str} for one disk is >85% of total system RAM. Ensure total allocated for all RAM disks is safe.")

            cmd = ['sudo', 'numactl', f'--membind={self.numa_node}',
                   'mount', '-t', 'tmpfs', '-o', f'size={self.size_str}', 'tmpfs', str(self.path)]
            self.logger.info(f"Attempting to mount RAM disk: {' '.join(cmd)}")

            result = subprocess.run(cmd, check=False, capture_output=True, text=True)

            if result.returncode != 0:
                self.logger.critical(f"Failed to mount RAM disk on {self.path} (Node {self.numa_node}): {result.stderr or result.stdout or 'No output'}.")
                self.logger.critical("Ensure you have sudo permissions or run this script as root, and that the NUMA node is valid.")
                raise ChildProcessError(f"tmpfs mount failed for {self.path}: {result.stderr or result.stdout}")

            self.logger.info(f"Successfully mounted RAM disk {self.path} ({self.size_str}) on NUMA node {self.numa_node}")
            self._is_mounted_verified = True
        except FileNotFoundError:
             raise
        except Exception as e:
            self.logger.critical(f"Unexpected error during RAM disk mount for {self.path} on Node {self.numa_node}: {e}", exc_info=True)
            raise

    def unmount(self):
        if not self._is_mounted_verified and not self._is_path_mounted():
            self.logger.info(f"RAM disk {self.path} does not appear to be mounted or was not managed by this session. Skipping unmount.")
            if self.path.exists() and not any(self.path.iterdir()):
                try:
                    self.path.rmdir()
                    self.logger.info(f"Removed empty RAM disk directory {self.path}")
                except OSError as e_rmdir:
                    self.logger.warning(f"Could not remove RAM disk directory {self.path}: {e_rmdir}")
            return

        self.logger.info(f"Attempting to unmount RAM disk {self.path}...")
        try:
            umount_cmd = ['sudo', 'umount', '-l', str(self.path)]
            result = subprocess.run(umount_cmd, check=False, capture_output=True, text=True)

            if result.returncode == 0:
                self.logger.info(f"Successfully unmounted {self.path}")
                if self.path.exists() and not any(self.path.iterdir()):
                    try:
                        self.path.rmdir()
                        self.logger.info(f"Removed empty RAM disk directory {self.path}")
                    except OSError as e_rmdir:
                        self.logger.warning(f"Could not remove RAM disk directory {self.path} after unmount: {e_rmdir}")
            else:
                self.logger.warning(f"Failed to unmount {self.path}: {result.stderr or result.stdout}. Manual unmount may be required ('sudo umount -l {self.path}').")
        except Exception as e:
            self.logger.error(f"Error during unmount process for {self.path}: {e}", exc_info=True)
        finally:
            self._is_mounted_verified = False

    def get_usage(self) -> tuple[int, int]:
        if not self.path.exists():
            self._is_mounted_verified = False
            return 0,0

        if not self._is_mounted_verified:
             if not self._is_path_mounted():
                return 0, 0
             else:
                self._is_mounted_verified = True

        try:
            usage = shutil.disk_usage(self.path)
            return usage.free, usage.total
        except FileNotFoundError:
            self.logger.warning(f"Path not found for disk usage check: {self.path} (might have been unmounted externally).")
            self._is_mounted_verified = False
            return 0, 0
        except Exception as e:
            self.logger.debug(f"Error getting disk usage for {self.path}: {e}")
            return 0, 0

    def get_path(self) -> Path:
        return self.path

    def is_verified_mounted(self) -> bool:
        return self._is_mounted_verified

class CoreAllocator:
    def __init__(self, app_config: "AppConfig"): 
        self.config = app_config
        self.num_logical_cpus = self.config.NUM_LOGICAL_CPUS

        if self.num_logical_cpus < 2:
            self.cores_node0_primary = list(range(self.num_logical_cpus))
            self.cores_node1_secondary = []
            self.is_dual_node_system = False
        else:
            self.cores_node0_primary = list(range(self.config.CORES_PER_NODE))
            self.cores_node1_secondary = list(range(self.config.CORES_PER_NODE, self.num_logical_cpus))
            self.is_dual_node_system = True

        self.download_cores_node0: list[int] = []
        self.clean_cores_node0: list[int] = []
        self.clean_cores_node1: list[int] = []
        self.reserved_cores_list: list[int] = []

        self._allocate()

    def _allocate(self):
        self.download_cores_node0 = self.cores_node0_primary[:self.config.DOWNLOAD_THREADS_CONFIG]
        available_node0 = [c for c in self.cores_node0_primary if c not in self.download_cores_node0]
        available_node1 = list(self.cores_node1_secondary)

        num_reserved_node0 = self.config.RESERVED_THREADS_TOTAL_CONFIG // 2 if self.is_dual_node_system else self.config.RESERVED_THREADS_TOTAL_CONFIG
        num_reserved_node1 = self.config.RESERVED_THREADS_TOTAL_CONFIG - num_reserved_node0 if self.is_dual_node_system else 0

        actual_reserved_node0 = available_node0[-num_reserved_node0:] if num_reserved_node0 > 0 and len(available_node0) >= num_reserved_node0 else []
        self.reserved_cores_list.extend(actual_reserved_node0)

        if self.is_dual_node_system:
            actual_reserved_node1 = available_node1[-num_reserved_node1:] if num_reserved_node1 > 0 and len(available_node1) >= num_reserved_node1 else []
            self.reserved_cores_list.extend(actual_reserved_node1)

        potential_clean_node0 = [c for c in available_node0 if c not in actual_reserved_node0]
        self.clean_cores_node0 = potential_clean_node0[:self.config.CLEAN_CPUS_NODE0]

        if self.is_dual_node_system:
            potential_clean_node1 = [c for c in available_node1 if c not in actual_reserved_node1]
            self.clean_cores_node1 = potential_clean_node1[:self.config.CLEAN_CPUS_NODE1]
        else:
            self.clean_cores_node1 = []

        logging.info(f"Core Allocation Plan:")
        logging.info(f"  Node 0/Primary Cores: {self.cores_node0_primary}")
        if self.is_dual_node_system:
            logging.info(f"  Node 1/Secondary Cores: {self.cores_node1_secondary}")
        logging.info(f"  Download Cores (Node 0/Primary): {self.download_cores_node0}")
        logging.info(f"  Clean Cores (Node 0/Primary): {self.clean_cores_node0} (Max requested: {self.config.CLEAN_CPUS_NODE0})")
        if self.is_dual_node_system:
            logging.info(f"  Clean Cores (Node 1/Secondary): {self.clean_cores_node1} (Max requested: {self.config.CLEAN_CPUS_NODE1})")
        logging.info(f"  Reserved Cores (Distributed): {sorted(list(set(self.reserved_cores_list)))}")