#!/usr/bin/env python3
# coding: utf-8

import signal
import time
import threading
from multiprocessing import Process, Manager
import logging
from logging.handlers import RotatingFileHandler

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
                logging.info("process daemon start a subprocess for audio data v1, the pid is: {0}".format(pid))
            elif self.sub_process_status_v1.get() == -1 and not sub_process.is_alive():
                logging.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v1.get()))
                self.sub_process_status_v1.set(0)
                self.sub_process_stop_flag_v1.set(0)
            else:
                logging.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v1.get()))

            time.sleep(1)

    def start_audio_v2(self):
        while not self.stop_flag.is_set():
            if self.sub_process_status_v2.get() == 0:
                new_job = CalculateAudioDataV2(self.sub_process_status_v2, self.delay_v2, self.data_record_v2, self.sub_process_stop_flag_v2)
                sub_process = Process(target=new_job.run)
                sub_process.start()
                pid = sub_process.pid
                logging.info("process daemon start a subprocess for audio data v2, the pid is: {0}".format(pid))
            elif self.sub_process_status_v2.get() == -1 and not sub_process.is_alive():
                logging.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v2.get()))
                self.sub_process_status_v2.set(0)
                self.sub_process_stop_flag_v2.set(0)
            else:
                logging.info("subprocess: {0} over, exit code: {1}".format(pid, self.sub_process_status_v2.get()))

            time.sleep(1)

    def stop(self):
        logging.warning("try to stop the daemon process")

        self.sub_process_stop_flag_v1.set(1)
        self.sub_process_stop_flag_v2.set(1)
        self.stop_flag.set()

    def signal_term_handler(self, signal_value, frame):
        logging.info("main process got {0}".format(signal_value))

        if signal_value == signal.SIGTERM or signal_value == signal.SIGINT:
            self.stop()



if __name__ == "__main__":
    output_logger = logging.getLogger()
    output_logger.setLevel(logging.INFO)
    output_rthandler = RotatingFileHandler("process_daemon.log", maxBytes=10 * 1024 * 1024, backupCount=10)
    output_formatter = logging.Formatter("%(asctime)s - %(name)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    output_rthandler.setFormatter(output_formatter)
    output_logger.addHandler(output_rthandler)

    logging.getLogger("cassandra.cluster").setLevel(logging.CRITICAL)
    logging.getLogger("cassandra.policies").setLevel(logging.CRITICAL)
    logging.getLogger("cassandra.pool").setLevel(logging.CRITICAL)

    delay_v1 = 60
    delay_v2 = 60

    run_obj = ProcessDaemon(delay_v1, delay_v2)

    signal.signal(signal.SIGTERM, run_obj.signal_term_handler)
    signal.signal(signal.SIGINT, run_obj.signal_term_handler)

    run_obj.run()