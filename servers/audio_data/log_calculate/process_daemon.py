#!/usr/bin/env python3
# coding: utf-8

import os
import sys
import signal
import time
import threading
from multiprocessing import Process, Manager

current_path = os.path.realpath(__file__)
module_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_path))))
sys.path.append(module_path)

from modules.log_print.print_log import LogPrint
from process_start import CalculateAudioDataV1, CalculateAudioDataV2



class ProcessDaemon(object):
    def __init__(self, delay_v1, delay_v2):
        self.delay_v1 = delay_v1
        self.delay_v2 = delay_v2

        self.sub_process_status_v1 = Manager().Value('d', 0)
        self.sub_process_status_v2 = Manager().Value('d', 0)

        self.data_record_v1 = Manager().dict()
        self.data_record_v2 = Manager().dict()

        self.sub_process_stop_flag_v1 = Manager().Value('d', 0)
        self.sub_process_stop_flag_v2 = Manager().Value('d', 0)

        self.stop_flag = threading.Event()
        self.stop_flag.clear()

    def run(self):
        thread_v1 = threading.Thread(target=self.start_audio_v1)
        thread_v2 = threading.Thread(target=self.start_audio_v2)

        thread_v1.start()
        thread_v2.start()

        thread_v1.join()
        thread_v2.join()

    def start_audio_v1(self):
        while not self.stop_flag.is_set():
            if self.sub_process_status_v1.get() == 0:
                new_job = CalculateAudioDataV1(self.sub_process_status_v1, self.delay_v1, self.data_record_v1, self.sub_process_stop_flag_v1)
                sub_process = Process(target=new_job.run)
                sub_process.start()
                pid = sub_process.pid
                logger.info("process daemon start a subprocess for audio data v1, the pid is: {0}".format(pid))
            elif self.sub_process_status_v1.get() == -1 and not sub_process.is_alive():
                logger.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v1.get()))
                self.sub_process_status_v1.set(0)
                self.sub_process_stop_flag_v1.set(0)
            elif not sub_process.is_alive():
                logger.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v1.get()))

            time.sleep(1)

    def start_audio_v2(self):
        while not self.stop_flag.is_set():
            if self.sub_process_status_v2.get() == 0:
                new_job = CalculateAudioDataV2(self.sub_process_status_v2, self.delay_v2, self.data_record_v2, self.sub_process_stop_flag_v2)
                sub_process = Process(target=new_job.run)
                sub_process.start()
                pid = sub_process.pid
                logger.info("process daemon start a subprocess for audio data v2, the pid is: {0}".format(pid))
            elif self.sub_process_status_v2.get() == -1 and not sub_process.is_alive():
                logger.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v2.get()))
                self.sub_process_status_v2.set(0)
                self.sub_process_stop_flag_v2.set(0)
            elif not sub_process.is_alive():
                logger.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v2.get()))

            time.sleep(1)

    def stop(self):
        logger.warning("try to stop the daemon process")

        self.sub_process_stop_flag_v1.set(1)
        self.sub_process_stop_flag_v2.set(1)
        self.stop_flag.set()

    def signal_term_handler(self, signal_value, frame):
        logger.info("main process got {0}".format(signal_value))

        if signal_value == signal.SIGTERM or signal_value == signal.SIGINT:
            self.stop()



if __name__ == "__main__":
    log_path = "audio_logs/process_daemon.log"
    log_formatter = "%(asctime)s - %(name)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"

    log_print_obj = LogPrint()
    logger = log_print_obj.init_logger(log_path, log_formatter)

    log_print_obj.set_log_level("cassandra.cluster", "critical")
    log_print_obj.set_log_level("cassandra.policies", "critical")
    log_print_obj.set_log_level("cassandra.pool", "critical")

    delay_v1 = 60
    delay_v2 = 60

    run_obj = ProcessDaemon(delay_v1, delay_v2)

    signal.signal(signal.SIGTERM, run_obj.signal_term_handler)
    signal.signal(signal.SIGINT, run_obj.signal_term_handler)

    run_obj.run()