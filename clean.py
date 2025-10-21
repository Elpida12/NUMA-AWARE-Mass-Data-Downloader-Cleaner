#!/usr/bin/env python3
"""
clean.py: CleanerProcess for processing WARC files.
"""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path
import multiprocessing
import io
from warcio.archiveiterator import ArchiveIterator

from config import AppConfig
from storage import StatsController
from extract import TextExtractor
from utils import SystemUtils

try:
    from multiprocessing.queues import Full as FullQueueException
except ImportError:
    FullQueueException = Exception

class CleanerProcess(multiprocessing.Process):
    """A process dedicated to cleaning WARC files from a specific NUMA node's queue."""
    def __init__(self,
                 process_id: int,
                 numa_node_idx: int,
                 assigned_core_id: int,
                 input_queue: "multiprocessing.Queue",
                 writer_queue: "multiprocessing.Queue",
                 app_config: "AppConfig",
                 stats_controller_proxy: "StatsController",
                 active_task_feedback_queue: "multiprocessing.Queue",
                 stop_event: "multiprocessing.Event",
                 text_extractor_cls: type[TextExtractor],
                 download_slot_semaphore: "multiprocessing.BoundedSemaphore"):
        super().__init__(daemon=True, name=f"Cleaner-Node{numa_node_idx}-Core{assigned_core_id}")
        self.process_id = process_id
        self.numa_node_idx = numa_node_idx
        self.assigned_core_id = assigned_core_id
        self.input_queue = input_queue
        self.writer_queue = writer_queue
        self.config = app_config
        self.stats_controller_proxy = stats_controller_proxy
        self.active_task_feedback_queue = active_task_feedback_queue
        self.stop_event = stop_event
        self.text_extractor_cls = text_extractor_cls
        self.logger: logging.Logger | None = None
        self.download_slot_semaphore = download_slot_semaphore

    def _setup_local_logging(self):
        self.logger = logging.getLogger(self.name)
        if not self.logger.handlers:
            pass

    def run(self):
        self._setup_local_logging()
        if self.logger is None:
            self.logger = logging.getLogger(self.name)
            if not self.logger.handlers and not logging.getLogger().handlers:
                logging.basicConfig(level=logging.INFO, format=f'[%(asctime)s] %(levelname)s: {self.name} %(message)s')
                self.logger = logging.getLogger(self.name)

        self.logger.info(f"Started. Target NUMA Node: {self.numa_node_idx}. Assigned Core: {self.assigned_core_id}.")

        try:
            SystemUtils.set_affinity([self.assigned_core_id], pid=os.getpid())
        except Exception as e:
            self.logger.error(f"Error setting affinity in CleanerProcess for core {self.assigned_core_id}: {e}")

        min_text_len = self.config.MIN_CLEAN_TEXT_LENGTH

        while not self.stop_event.is_set():
            warc_file_path_str = None
            try:
                warc_file_path_str = self.input_queue.get(timeout=1.0)
            except multiprocessing.queues.Empty:
                if self.stop_event.is_set(): break
                continue
            except (EOFError, BrokenPipeError):
                 self.logger.warning(f"Queue communication error (EOF/BrokenPipe), cleaner process {self.name} exiting.")
                 break
            except Exception as e:
                self.logger.error(f"Unexpected error getting from input queue for {self.name}: {e}", exc_info=True)
                if self.stop_event.is_set(): break
                time.sleep(0.1)
                continue

            if warc_file_path_str is None:
                if self.stop_event.is_set(): break
                continue

            warc_file_on_ramdisk = Path(warc_file_path_str)
            warc_basename = warc_file_on_ramdisk.name

            self.active_task_feedback_queue.put((self.assigned_core_id, warc_basename, 'start'))
            self.logger.info(f"Processing: {warc_basename} from {warc_file_on_ramdisk.parent}")

            task_is_complete_or_dropped = False

            try:
                if not warc_file_on_ramdisk.exists():
                    self.logger.warning(f"WARC file {warc_basename} not found at {warc_file_on_ramdisk}. Dropping task.")
                    task_is_complete_or_dropped = True
                else:
                    final_output_path = self.config.OUTPUT_DIR / (warc_basename + '.txt')

                    if final_output_path.exists() and final_output_path.stat().st_size > min_text_len:
                        self.logger.info(f"Output file {final_output_path.name} already exists. Skipping.")
                        if not self.stats_controller_proxy.is_already_cleaned(warc_basename):
                            self.stats_controller_proxy.record_cleaned_file(warc_basename, 0)
                        task_is_complete_or_dropped = True
                    else:
                        # --- MODIFICATION: Perform cleaning in-memory instead of to a temp file ---
                        extracted_record_count = 0
                        original_size_bytes = warc_file_on_ramdisk.stat().st_size
                        
                        # Use an in-memory text stream (StringIO) to buffer the cleaned text
                        in_memory_buffer = io.StringIO()

                        with open(warc_file_on_ramdisk, 'rb') as stream:
                            for record in ArchiveIterator(stream, arc2warc=True):
                                if self.stop_event.is_set(): break
                                if record.rec_type in ('response', 'resource') and \
                                   record.http_headers and 'html' in record.http_headers.get_header('Content-Type', ''):
                                    content_bytes = record.content_stream().read()
                                    clean_text = self.text_extractor_cls.extract_clean_text(content_bytes, self.logger, min_text_len)
                                    if len(clean_text) >= min_text_len:
                                        in_memory_buffer.write(clean_text + "\n\n---EndOfRecord---\n\n")
                                        extracted_record_count += 1

                        if extracted_record_count > 0:
                            # Get the complete text content from the in-memory buffer
                            final_cleaned_text = in_memory_buffer.getvalue()
                            final_output_size = len(final_cleaned_text.encode('utf-8'))
                            data_removed = original_size_bytes - final_output_size
                            self.stats_controller_proxy.record_cleaned_file(warc_basename, data_removed)

                            # MODIFICATION: Queue the final path and the actual text content for the writer
                            self.writer_queue.put((str(final_output_path), final_cleaned_text))
                            self.logger.info(f"Finished cleaning {warc_basename}. Queued content for writing.")
                        else:
                            # If no text was extracted, just record it as cleaned with max data removal
                            self.stats_controller_proxy.record_cleaned_file(warc_basename, original_size_bytes)
                        
                        # Close the buffer
                        in_memory_buffer.close()
                        task_is_complete_or_dropped = True

            except Exception as e:
                self.logger.error(f"Unhandled exception during cleaning of {warc_basename}: {e}", exc_info=True)
                task_is_complete_or_dropped = True

            finally:
                if task_is_complete_or_dropped:
                    # The original WARC can now be safely deleted, freeing up RAM disk space immediately.
                    if warc_file_on_ramdisk and warc_file_on_ramdisk.exists():
                        try:
                            warc_file_on_ramdisk.unlink()
                            self.logger.debug(f"Removed {warc_basename} from RAM disk after processing.")
                        except OSError as e_del:
                            self.logger.error(f"Failed to remove {warc_basename} from RAM disk: {e_del}")

                    # Release the download slot so a new download can begin.
                    try:
                        self.download_slot_semaphore.release()
                    except Exception as e:
                        self.logger.error(f"Error releasing download slot: {e}")
                    self.logger.debug(f"Released download slot for: {warc_basename}")

                self.active_task_feedback_queue.put((self.assigned_core_id, warc_basename, 'end'))
        self.logger.info(f"Cleaner process on Core {self.assigned_core_id} (Node {self.numa_node_idx}) stopping.")
