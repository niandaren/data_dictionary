#!/usr/bin/env python3
# coding: utf-8

import sys
import os
import queue
import time
from multiprocessing.managers import BaseManager
import threading
import signal
import logging
from logging.handlers import RotatingFileHandler

current_path = os.path.realpath(__file__)
module_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_path))))
sys.path.append(module_path)

from modules.config_reader.parse_config import ConfigReader



class QueueManager(object):
    __period_seconds = 600

    def __init__(self):
        config_obj = ConfigReader()

        config_key = "audio_queue"
        config_info = config_obj.get_section_config(config_key)
        self.queue_ip = config_info.get("host")
        self.queue_port = int(config_info.get("port"))
        self.queue_key = config_info.get("key").encode()

        self.stop_flag = threading.Event()

        self.stop_flag.clear()

    @classmethod
    def change_period_value(cls, period_seconds):
        cls.__period_seconds = period_seconds

    def init_task_queue(self):
        audio_queue_v1 = queue.Queue()
        BaseManager.register("audio_queue_v1", callable=lambda: audio_queue_v1)

        audio_queue_v2 = queue.Queue()
        BaseManager.register("audio_queue_v2", callable=lambda: audio_queue_v2)

        manager = BaseManager(address=(self.queue_ip, self.queue_port), authkey=self.queue_key)

        manager.start()

        while not self.stop_flag.is_set():
            logging.info("task queue is running......")
            time.sleep(QueueManager.__period_seconds)

        manager.shutdown()

    def get_audio_queue_v1(self):
        BaseManager.register("audio_queue_v1")

        manager = BaseManager(address=(self.queue_ip, self.queue_port), authkey=self.queue_key)
        manager.connect()

        return manager.audio_queue_v1()

    def get_audio_queue_v2(self):
        BaseManager.register("audio_queue_v2")

        manager = BaseManager(address=(self.queue_ip, self.queue_port), authkey=self.queue_key)
        manager.connect()

        return manager.audio_queue_v2()

    def stop(self):
        logging.warning("try to stop the process")

        self.stop_flag.set()

    def signal_term_handler(self, signal_value, frame):
        logging.info("the process got {0}".format(signal_value))

        if signal_value == signal.SIGTERM or signal_value == signal.SIGINT:
            self.stop()



if __name__ == "__main__":
    output_logger = logging.getLogger()
    output_logger.setLevel(logging.INFO)
    output_rthandler = RotatingFileHandler("***", maxBytes=10 * 1024 * 1024, backupCount=10)
    output_formatter = logging.Formatter("%(asctime)s - %(name)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    output_rthandler.setFormatter(output_formatter)
    output_logger.addHandler(output_rthandler)

    run_job = QueueManager()

    signal.signal(signal.SIGTERM, run_job.signal_term_handler)
    signal.signal(signal.SIGINT, run_job.signal_term_handler)

    run_job.init_task_queue()