#!/usr/bin/env python3
"""
writer.py: A dedicated process for writing cleaned data to final storage.
"""
from __future__ import annotations

import logging
import multiprocessing
import time
from pathlib import Path

class WriterProcess(multiprocessing.Process):
    """
    A process that listens on a queue for cleaned file content and writes it
    to the final storage destination.
    """
    def __init__(self,
                 writer_queue: "multiprocessing.Queue",
                 stop_event: "multiprocessing.Event"):
        super().__init__(daemon=True, name="WriterProcess")
        self.writer_queue = writer_queue
        self.stop_event = stop_event
        self.logger: logging.Logger | None = None

    def run(self):
        """The main loop for the writer process."""
        self.logger = logging.getLogger(self.name)
        if not self.logger.handlers:
                logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(name)s %(message)s')
                self.logger = logging.getLogger(self.name)
        self.logger.info("Writer process started.")

        while not self.stop_event.is_set():
            try:
               
                dest_path_str, content_to_write = self.writer_queue.get(timeout=1.0)

                dest_path = Path(dest_path_str)

                try:
                    # Ensure the final destination directory exists
                    dest_path.parent.mkdir(parents=True, exist_ok=True)

                   
                    with open(dest_path, 'w', encoding='utf-8', errors='replace') as f_out:
                        f_out.write(content_to_write)

                    self.logger.info(f"Wrote cleaned content to final storage at {dest_path}.")

                except Exception as e:
                    self.logger.error(f"Failed to write content to {dest_path}: {e}", exc_info=True)

            except multiprocessing.queues.Empty:
             
                if self.stop_event.is_set():
                    break
                continue
            except (EOFError, BrokenPipeError):
                self.logger.warning("Writer queue has been closed or broken. Exiting.")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in writer process loop: {e}", exc_info=True)
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)

        self.logger.info("Writer process stopping.")
