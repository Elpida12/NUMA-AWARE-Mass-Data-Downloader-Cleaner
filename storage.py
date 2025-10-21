#!/usr/bin/env python3
"""
storage.py: Persistent JSON storage and Statistics Controller.
"""
from __future__ import annotations

import json
import logging
import threading 
from pathlib import Path
from datetime import datetime
from typing import Callable
import time



class PersistentJsonStore:
    """Helper class for loading/saving JSON data with locking."""
    def __init__(self, file_path: Path, default_factory: Callable[[], dict],
                 lock: "threading.RLock | multiprocessing.RLock | multiprocessing.Lock", 
                 logger: logging.Logger):
        self.file_path = file_path
        self.default_factory = default_factory
        self.lock = lock 
        self.logger = logger
        self.data = self._load()

    def _load(self) -> dict:
        self.lock.acquire()
        try:
            if self.file_path.exists():
                try:
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except json.JSONDecodeError:
                    self.logger.warning(f"Could not decode JSON from {self.file_path}, using default.")
                except Exception as e:
                    self.logger.error(f"Error loading {self.file_path}: {e}", exc_info=True)
            return self.default_factory()
        finally:
            self.lock.release()

    def save(self):
        self.lock.acquire()
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"Saved data to {self.file_path}")
        except Exception as e:
            self.logger.error(f"Failed to save data to {self.file_path}: {e}", exc_info=True)
        finally:
            self.lock.release()

    def get(self, key, default=None):
        self.lock.acquire()
        try:
            return self.data.get(key, default)
        finally:
            self.lock.release()

    def get_all_copy(self) -> dict:
        self.lock.acquire()
        try:
            return self.data.copy()
        finally:
            self.lock.release()

    def update(self, key, value_or_updater_fn):
        self.lock.acquire()
        try:
            if callable(value_or_updater_fn):
                current_val = self.data.get(key)
                self.data[key] = value_or_updater_fn(current_val)
            else:
                self.data[key] = value_or_updater_fn
        finally:
            self.lock.release()

    def set_default(self, key, default_value):
        self.lock.acquire()
        try:
            return self.data.setdefault(key, default_value)
        finally:
            self.lock.release()

    def add_to_list_if_not_exists(self, key: str, item_to_add):
        self.lock.acquire()
        try:
            current_list = self.data.get(key, [])
            if not isinstance(current_list, list):
                self.logger.warning(f"Expected list for key '{key}' in {self.file_path}, found {type(current_list)}. Reinitializing as list.")
                current_list = []
            if item_to_add not in current_list:
                current_list.append(item_to_add)
            self.data[key] = current_list
        finally:
            self.lock.release()

    def remove_from_list(self, key: str, item_to_remove):
        self.lock.acquire()
        try:
            current_list = self.data.get(key, [])
            if not isinstance(current_list, list): return
            try:
                current_list.remove(item_to_remove)
                self.data[key] = current_list
            except ValueError:
                self.logger.debug(f"Item {item_to_remove} not found in list for key {key} during removal.")
        finally:
            self.lock.release()

class StatsController:
    """Manages application statistics and tracking of cleaned files. (Designed to be proxied by SyncManager)"""
  
    def __init__(self, app_config: "AppConfig",
                 stats_rlock: "multiprocessing.RLock",
                 cleaned_files_lock: "multiprocessing.Lock"):
        self.config = app_config
        self.logger = logging.getLogger(self.__class__.__name__)


        self.stats_lock = stats_rlock
        self.cleaned_files_lock = cleaned_files_lock

        self.stats_store = PersistentJsonStore(
            self.config.STATS_FILE,
            lambda: {
                'total_data_downloaded_bytes': 0,
                'total_data_removed_bytes': 0,
                'total_warc_processed_for_download_count': 0,
                'total_warc_successfully_cleaned_count': 0,
                'start_time_iso': datetime.now().isoformat(),
                'last_update_iso': datetime.now().isoformat(),
                'downloaded_warc_filenames': []
            },
            self.stats_lock,
            self.logger
        )
        self.cleaned_files_store = PersistentJsonStore(
            self.config.CLEANED_FILES_TRACKER_FILE,
            lambda: {'cleaned_warc_basenames': []},
            self.cleaned_files_lock,
            self.logger
        )
        self._in_memory_cleaned_set = set(self.cleaned_files_store.get('cleaned_warc_basenames', []))

        if self.config.INITIAL_CLEANED_SCAN and not self.config.CLEANED_FILES_TRACKER_FILE.exists():
            self._scan_output_for_cleaned()

       
        start_time_iso_str = self.stats_store.get('start_time_iso')
        if start_time_iso_str is None:
            start_time_iso_str = datetime.now().isoformat()
            self.stats_store.update('start_time_iso', start_time_iso_str)

        self.start_datetime = datetime.fromisoformat(start_time_iso_str)


    def _scan_output_for_cleaned(self):
        self.logger.info(f"Scanning {self.config.OUTPUT_DIR} for existing cleaned files (tracker not found)...")
        if not self.config.OUTPUT_DIR.is_dir():
            self.logger.warning(f"Output directory {self.config.OUTPUT_DIR} does not exist for initial scan.")
            return

        found_count = 0

        self.cleaned_files_lock.acquire()
        try:
            current_cleaned_list = self.cleaned_files_store.get('cleaned_warc_basenames', [])
            changed_in_memory = False
            for f_path in self.config.OUTPUT_DIR.glob('*.txt'):
                original_warc_basename = f_path.name.removesuffix('.txt')
                if original_warc_basename not in self._in_memory_cleaned_set:
                    self._in_memory_cleaned_set.add(original_warc_basename)
                    changed_in_memory = True
                    if original_warc_basename not in current_cleaned_list:
                        current_cleaned_list.append(original_warc_basename)
                    found_count += 1
            
            if found_count > 0 or changed_in_memory:
                updated_list_for_store = sorted(list(self._in_memory_cleaned_set))
                self.cleaned_files_store.update('cleaned_warc_basenames', updated_list_for_store)
                self.cleaned_files_store.save()
        finally:
            self.cleaned_files_lock.release()

        if found_count > 0:
            self.logger.info(f"Initial scan found and added {found_count} unique cleaned file basenames to tracker.")
        elif changed_in_memory :
             self.logger.info(f"Initial scan synchronized _in_memory_cleaned_set with existing tracker file (if any).")
        else:
            self.logger.info(f"Initial scan found no new cleaned file basenames to add to tracker.")


    def record_warc_downloaded(self, warc_basename: str, downloaded_bytes: int):

        if warc_basename not in self.stats_store.get('downloaded_warc_filenames', []):
            self.stats_store.update('total_data_downloaded_bytes', lambda current: (current or 0) + downloaded_bytes)
            self.stats_store.add_to_list_if_not_exists('downloaded_warc_filenames', warc_basename)
            self.stats_store.update('total_warc_processed_for_download_count', lambda current: (current or 0) + 1)
        self.stats_store.update('last_update_iso', datetime.now().isoformat())

    def revert_warc_download_stats(self, warc_basename: str, original_bytes: int):

        self.stats_lock.acquire()
        try:
            downloaded_files = self.stats_store.get('downloaded_warc_filenames', [])
            if warc_basename in downloaded_files:
                self.stats_store.update('total_data_downloaded_bytes', lambda current: (current or 0) - original_bytes)

                self.stats_store.remove_from_list('downloaded_warc_filenames', warc_basename)
                self.stats_store.update('total_warc_processed_for_download_count', lambda current: (current or 0) - 1)
                self.logger.info(f"Reverted download stats for {warc_basename} ({original_bytes} bytes).")
            self.stats_store.update('last_update_iso', datetime.now().isoformat())
        finally:
            self.stats_lock.release()


    def record_cleaned_file(self, warc_basename: str, data_removed_bytes: int):

        self.stats_store.update('total_warc_successfully_cleaned_count', lambda current: (current or 0) + 1)
        self.stats_store.update('total_data_removed_bytes', lambda current: (current or 0) + data_removed_bytes)
        self.stats_store.update('last_update_iso', datetime.now().isoformat())

        self.cleaned_files_lock.acquire()
        try:
            if warc_basename not in self._in_memory_cleaned_set:
                self._in_memory_cleaned_set.add(warc_basename)

                self.cleaned_files_store.add_to_list_if_not_exists('cleaned_warc_basenames', warc_basename)

        finally:
            self.cleaned_files_lock.release()


    def is_already_cleaned(self, warc_basename: str) -> bool:

        return warc_basename in self._in_memory_cleaned_set

    def get_summary(self) -> dict:
        return self.stats_store.get_all_copy()

    def get_start_timestamp(self) -> float:
        return self.start_datetime.timestamp()

    def periodic_save_all(self, stop_event: "threading.Event | multiprocessing.Event", interval_s: int):
        self.logger.info(f"Periodic saver thread/process started. Interval: {interval_s}s")
        next_save_time = time.time() + interval_s
        while not stop_event.is_set():
            wait_time = max(0, next_save_time - time.time())
            if stop_event.wait(wait_time):
                break 

            self.logger.debug("Periodic save: saving stats and cleaned files tracker.")
            try:
                self.stats_store.save() 
            except Exception as e:
                self.logger.error(f"Periodic save failed for stats_store: {e}", exc_info=True)
            try:
                self.cleaned_files_store.save()
            except Exception as e:
                self.logger.error(f"Periodic save failed for cleaned_files_store: {e}", exc_info=True)
            
            next_save_time = time.time() + interval_s
            
        self.logger.info("Periodic saver stop signal received or loop exited.")
        self.final_save_all() 

    def final_save_all(self):
        self.logger.info("Performing final save of stats and cleaned files tracker.")
        try:
            self.stats_store.save()
        except Exception as e:
            self.logger.error(f"Final save failed for stats_store: {e}", exc_info=True)
        try:
            self.cleaned_files_store.save()
            self.logger.error(f"Final save failed for cleaned_files_store: {e}", exc_info=True)
