#!/usr/bin/env python3
# coding: utf-8

import sys
import os
import traceback
import re
import json
import time
from datetime import datetime, timedelta, timezone
from multiprocessing.managers import BaseManager
import threading
import signal
import functools
import logging
from logging.handlers import RotatingFileHandler

current_path = os.path.realpath(__file__)
module_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_path))))
sys.path.append(module_path)

from modules.config_reader.parse_config import ConfigReader
from modules.connector.kafka_connector import KafkaConnector

RETRY_TIMES = 5
PERIOD_SECONDS = 60



def function_retry(retry_times, period_seconds):
    def decorator(func):
        functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(retry_times):
                try:
                    return func(*args, **kwargs)
                except:
                    error_data = traceback.format_exc()
                    logging.error(error_data)
                    time.sleep(period_seconds)
                    logging.info(f"retry {func.__name__} {i + 1} times")

            return None

        return wrapper

    return decorator


class ReceiveLog(object):
    def __init__(self):
        config_obj = ConfigReader()

        config_key = "audio_queue"
        audio_queue_config = config_obj.get_section_config(config_key)
        self.queue_ip = audio_queue_config.get("host")
        self.queue_port = int(audio_queue_config.get("port"))
        self.queue_key = audio_queue_config.get("key").encode()

        config_key = "kafka"
        kafka_config = config_obj.get_section_config(config_key)

        self.kafka_connector = KafkaConnector(kafka_config)

        self.audio_queue_v1 = self.get_audio_queue_v1()
        self.audio_queue_v2 = self.get_audio_queue_v2()

        if self.audio_queue_v1 is None or self.get_audio_queue_v2 is None:
            logging.error("init audio queue error")
            sys.exit(1)

        self.stop_flag = threading.Event()
        self.stop_flag.clear()

    @function_retry(retry_times=RETRY_TIMES, period_seconds=PERIOD_SECONDS)
    def get_audio_queue_v1(self):
        BaseManager.register("audio_queue_v1")

        manager = BaseManager(address=(self.queue_ip, self.queue_port), authkey=self.queue_key)
        manager.connect()

        return manager.audio_queue_v1()

    @function_retry(retry_times=RETRY_TIMES, period_seconds=PERIOD_SECONDS)
    def get_audio_queue_v2(self):
        BaseManager.register("audio_queue_v2")

        manager = BaseManager(address=(self.queue_ip, self.queue_port), authkey=self.queue_key)
        manager.connect()

        return manager.audio_queue_v2()

    def run(self):
        thread_v1 = threading.Thread(target=self.receive_audio_logs_v1)
        thread_v2 = threading.Thread(target=self.receive_audio_logs_v2)

        thread_v1.start()
        thread_v2.start()

        thread_v1.join()
        thread_v2.join()

    def receive_audio_logs_v1(self):
        logging.info("receive audio logs v1, status: start")

        try:
            kafka_consumer = self.kafka_connector.balanced_consumer("audio-log-v1", True)
        except:
            error_data = traceback.format_exc()
            logging.error("topic: {0}, error_message: {1}".format("audio-log-v1", error_data))
            return False

        log_count = 0
        log_info = {}

        for message in kafka_consumer:
            if self.stop_flag.is_set():
                break

            if message is None:
                break

            message_decode = message.value.decode("utf-8")
            origin_log = json.loads(message_decode).get("origin_log")

            now_ts = datetime.now().replace(microsecond=0, second=0)

            if now_ts not in log_info:
                if len(log_info) > 0:
                    log_info_key_list = list(log_info.keys())
                    log_info_key = log_info_key_list[0]
                    log_info = {log_info_key.strftime("%Y-%m-%d %H:%M:%S"): [log_item.strftime("%Y-%m-%d %H:%M:%S") for log_item in sorted(list(log_info[log_info_key]))]}
                    logging.info("receive audio logs v1, log info: {0}".format(log_info))

                log_info = {now_ts: set()}

            try:
                log_time = self.parse_audio_logs_v1(origin_log)
            except:
                log_time = False
                logging.error("parse audio logs v1 error, the origin_log: {0}".format(origin_log))

            if log_time is not False:
                self.audio_queue_v1.put(origin_log)
                log_minute = log_time.replace(second=0)
                log_info[now_ts].add(log_minute)

            log_count += 1

            if log_count >= 10000:
                logging.info("receive audio logs v1, status: running")
                log_count = 0

        kafka_consumer.stop()

        logging.info("receive audio logs v1, status: over")

    def parse_audio_logs_v1(self, log):
        log_time_format = '%d/%b/%Y:%X +0800'
        pattern = re.compile(r'(?P<api_ip>[\d\.]+) (?P<slb_ip>[\d\.]+) - (.*) \[(?P<log_time>[\da-zA-Z/:]{20} \+\d{4})\] "(?P<method>\w+) (?P<request_url>\S+) \S+" ([\d\.]+) (\d+) ([\d\.\-]+) (\d+) (\d+) "(?P<referer>[^"]+)" (?:"(?P<context_raw>uk:[^"]+)" ){0,1}"(?P<ajmd_app_name>.*)\/(?P<ajmd_version>.*) \((?P<os>[a-zA-Z]+) (?P<os_version>[^;]+); (?P<device_type>[^";]+); (?P<device_id>[0-9a-zA-Z\-]{40}|[0-9a-zA-Z\-]{36})(?:; (?P<channel>[^";]+)){0,1}[^\)].*" "(?:[\d\.]+, )*(?P<ip>[\d\.]+)"')

        m = pattern.match(log)

        if not m:
            return False

        log_info = m.groupdict()

        device_id = log_info.get("device_id")

        if device_id is None:
            return False

        device_length = len(device_id)

        if device_length != 36 and device_length != 40:
            return False

        log_time = log_info.get("log_time")

        if log_time is None:
            return False

        log_time_ts = datetime.strptime(log_time, log_time_format).replace(tzinfo=timezone(timedelta(hours=8)))
        log_time = datetime.strptime(log_time_ts.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        return log_time

    def receive_audio_logs_v2(self):
        logging.info("receive audio logs v2, status: start")

        try:
            kafka_consumer = self.kafka_connector.balanced_consumer("audio-log-v2", True)
        except:
            error_data = traceback.format_exc()
            logging.error("topic: {0}, error_message: {1}".format("audio-log-v2", error_data))
            return False

        log_count = 0
        log_info = {}

        for message in kafka_consumer:
            if self.stop_flag.is_set():
                break

            if message is None:
                break

            message_decode = message.value.decode("utf-8")
            origin_log = json.loads(message_decode).get("origin_log")

            now_ts = datetime.now().replace(microsecond=0, second=0)

            if now_ts not in log_info:
                if len(log_info) > 0:
                    log_info_key_list = list(log_info.keys())
                    log_info_key = log_info_key_list[0]
                    log_info = {log_info_key.strftime("%Y-%m-%d %H:%M:%S"): [log_item.strftime("%Y-%m-%d %H:%M:%S") for log_item in sorted(list(log_info[log_info_key]))]}
                    logging.info("receive audio logs v1, log info: {0}".format(log_info))

                log_info = {now_ts: set()}

            try:
                log_time = self.parse_audio_logs_v2(origin_log)
            except:
                log_time = False
                logging.error("parse audio logs v2 error, the origin_log: {0}".format(origin_log))

            if log_time is not False:
                self.audio_queue_v2.put(origin_log)
                log_minute = log_time.replace(second=0)
                log_info[now_ts].add(log_minute)

            log_count += 1

            if log_count >= 10000:
                logging.info("receive audio logs v2, status: running")
                log_count = 0

        kafka_consumer.stop()

        logging.info("receive audio logs v2, status: over")

    def parse_audio_logs_v2(self, log):
        log_time_format = '%d/%b/%Y:%X +0800'
        pattern = re.compile(r'(?P<api_ip>[\d\.]+) (?P<slb_ip>[\d\.]+) - (.*) \[(?P<log_time>[\da-zA-Z/:]{20} \+\d{4})\] "(?P<method>\w+) (?P<request_url>\S+) \S+" ([\d\.]+) (\d+) ([\d\.\-]+) (\d+) (\d+) "(?P<referer>[^"]+)" (?:"(?P<context_raw>uk:[^"]+)" ){0,1}"(?P<ajmd_app_name>.*)\/(?P<ajmd_version>.*) \((?P<os>[a-zA-Z]+) (?P<os_version>[^;]+); (?P<device_type>[^";]+); (?P<device_id>[0-9a-zA-Z\-]{40}|[0-9a-zA-Z\-]{36})(?:; (?P<channel>[^";]+)){0,1}[^\)].*" "(?:[\d\.]+, )*(?P<ip>[\d\.]+)"')

        m = pattern.match(log)

        if not m:
            return False

        log_info = m.groupdict()

        device_id = log_info.get("device_id")

        if device_id is None:
            return False

        device_length = len(device_id)

        if device_length != 36 and device_length != 40:
            return False

        log_time = log_info.get("log_time")

        if log_time is None:
            return False

        log_time_ts = datetime.strptime(log_time, log_time_format).replace(tzinfo=timezone(timedelta(hours=8)))
        log_time = datetime.strptime(log_time_ts.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

        return log_time

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

    run_job = ReceiveLog()

    signal.signal(signal.SIGTERM, run_job.signal_term_handler)
    signal.signal(signal.SIGINT, run_job.signal_term_handler)

    run_job.run()