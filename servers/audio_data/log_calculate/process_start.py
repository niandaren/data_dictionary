#!/usr/bin/env python3
# coding: utf-8

import sys
import os
import traceback
import json
import time
from datetime import datetime, timedelta
import threading
from multiprocessing import Process, Manager
from multiprocessing.managers import BaseManager
import functools
import logging

current_path = os.path.realpath(__file__)
module_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_path))))
sys.path.append(module_path)

from modules.config_reader.parse_config import ConfigReader
from modules.connector.cassandra_connector import CassandraConnector
from modules.time_handle.handle_time import convert_into_utc_time
from modules.time_handle.handle_time import DateDecoder

from calculate_one_minute_logs_v1 import CalculateOneMinuteLogs as CalculateOneMinuteLogsV1
from calculate_one_minute_logs_v2 import CalculateOneMinuteLogs as CalculateOneMinuteLogsV2

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

            return False

        return wrapper

    return decorator


class CalculateAudioDataV1(object):
    def __init__(self, status_value, delay_value, data_record, stop_flag):
        self.status_value = status_value
        self.status_value.set(1)
        self.delay_value = delay_value
        self.data_record = data_record
        self.stop_flag = stop_flag

        config_obj = ConfigReader()

        config_key = "audio_queue"
        audio_queue_config = config_obj.get_section_config(config_key)
        self.queue_ip = audio_queue_config.get("host")
        self.queue_port = int(audio_queue_config.get("port"))
        self.queue_key = audio_queue_config.get("key").encode()

        config_key = "cassandra"
        cassandra_config = config_obj.get_section_config(config_key)
        cassandra_host = [host.strip() for host in cassandra_config.get("hosts").split(',')]
        cassandra_user = cassandra_config.get("username")
        cassandra_password = cassandra_config.get("password")

        self.cassandra_connector = CassandraConnector()
        self.cassandra_connector.db = cassandra_host
        self.cassandra_connector.username = cassandra_user
        self.cassandra_connector.password = cassandra_password

        self.audio_queue = self.get_audio_queue()

        if self.audio_queue is False:
            logging.error("get audio queue error")
            sys.exit(1)

        self.period_logs_info = {}
        self.device_audio_record = Manager().dict()

    @function_retry(retry_times=RETRY_TIMES, period_seconds=PERIOD_SECONDS)
    def get_audio_queue(self):
        BaseManager.register("audio_queue_v1")

        manager = BaseManager(address=(self.queue_ip, self.queue_port), authkey=self.queue_key)
        manager.connect()

        return manager.audio_queue_v1()

    def init_cassandra(self):
        if not self.cassandra_connector.prepare_stmt("update_device_audio_data", "ks_ajmd_dw", "update device_audio_daily set start_type=?,end_type=?,end_time=?,live_status=?,pid=?,tid=?,netstat=?,start_pos=?,end_pos=? where date=? and device_id=? and start_time=? and phid=?"):
            logging.error("init cassandra db error")
            sys.exit(2)

    def run(self):
        self.status_value.set(2)

        self.init_cassandra()

        self.status_value.set(3)

        thread_get_logs = threading.Thread(target=self.get_logs_from_queue)
        thread_calculate_logs = threading.Thread(target=self.calculate_logs)

        thread_get_logs.start()
        thread_calculate_logs.start()

        thread_get_logs.join()
        thread_calculate_logs.join()

        self.status_value.set(4)

        for period_key in self.period_logs_info:
            self.data_record[period_key] = self.period_logs_info[period_key]

        self.status_value.set(-1)

    def calculate_logs(self):
        while not self.stop_flag.get() == 1:
            now_ts = datetime.now().replace(microsecond=0)
            period_key_list = sorted(self.period_logs_info.keys())
            calculate_logs_info = {}

            for period_key in period_key_list:
                if self.period_logs_info[period_key].get("end_ts") is None:
                    continue

                if now_ts > self.period_logs_info[period_key].get("end_ts"):
                    if period_key.hour == 23 and period_key.minute == 59:
                        self.stop_flag.set(1)
                        calculate_logs_info[period_key] = self.period_logs_info[period_key].get("logs_list")
                        del self.period_logs_info[period_key]
                        break
                    else:
                        calculate_logs_info[period_key] = self.period_logs_info[period_key].get("logs_list")
                        del self.period_logs_info[period_key]

            calculate_minute_list = sorted(calculate_logs_info.keys())

            for calculate_minute_item in calculate_minute_list:
                logging.info("start calculate minute v1: {0}".format(calculate_minute_item.strftime("%Y-%m-%d %H:%M:00")))

                start_ts = time.time()

                calculate_one_minute_logs_obj = CalculateOneMinuteLogsV1(self.device_audio_record)
                calculate_one_minute_logs_process = Process(target=calculate_one_minute_logs_obj.run, args=(calculate_logs_info[calculate_minute_item],))
                calculate_one_minute_logs_process.start()
                calculate_one_minute_logs_process.join()

                try:
                    self.check_device_audio_record(calculate_minute_item + timedelta(seconds=59))
                except:
                    error_data = traceback.format_exc()
                    logging.error("check device audio record error: {0}".format(error_data))

                if calculate_minute_item.hour == 23 and calculate_minute_item.minute == 59:
                    try:
                        self.end_device_audio_record(calculate_minute_item + timedelta(seconds=59))
                    except:
                        error_data = traceback.format_exc()
                        logging.error("end device audio record error: {0}".format(error_data))

                end_ts = time.time()

                logging.info("calculate minute v1: {0} over, it takes: {1} seconds".format(calculate_minute_item, int(end_ts - start_ts)))

    def end_device_audio_record(self, end_ts):
        device_id_list = list(self.device_audio_record.keys())
        jobs = {}
        job_index = 0

        for device_id in device_id_list:
            device_info = self.device_audio_record[device_id]

            last_action = device_info.get("action")
            last_update_ts = device_info.get("update_ts")
            last_start_type = device_info.get("start_type")
            last_start_ts = device_info.get("start_ts")
            last_phid = device_info.get("phid")
            last_live = device_info.get("live")
            last_pid = device_info.get("pid")
            last_tid = device_info.get("tid")
            last_netstat = device_info.get("netstat")
            last_start_pos = device_info.get("start_pos")
            last_end_pos = device_info.get("end_pos")

            diff_ts = int((end_ts - last_update_ts).total_seconds())

            if last_action == "pause":
                last_end_ts = last_update_ts
                last_end_type = "pause"
            else:
                if diff_ts >= 40:
                    last_end_ts = last_update_ts + timedelta(seconds=30)
                else:
                    last_end_ts = end_ts

                last_end_type = "stop"

            if last_end_ts is not None and last_end_ts < last_start_ts:
                last_end_ts = last_start_ts + timedelta(seconds=30)

            jobs[job_index] = [last_start_type, last_end_type,
                               convert_into_utc_time(last_end_ts) if last_end_ts is not None else None, last_live,
                               last_pid, last_tid, last_netstat, last_start_pos, last_end_pos,
                               convert_into_utc_time(last_start_ts.replace(second=0, minute=0, hour=0)), device_id,
                               convert_into_utc_time(last_start_ts), last_phid]
            job_index += 1

            del self.device_audio_record[device_id]

        if len(jobs) > 0:
            self.cassandra_connector.do_futures("update_device_audio_data", jobs)

    def check_device_audio_record(self, check_ts):
        device_id_list = list(self.device_audio_record.keys())
        jobs = {}
        job_index = 0

        for device_id in device_id_list:
            device_info = self.device_audio_record[device_id]

            last_action = device_info.get("action")
            last_update_ts = device_info.get("update_ts")
            last_start_type = device_info.get("start_type")
            last_start_ts = device_info.get("start_ts")
            last_phid = device_info.get("phid")
            last_live = device_info.get("live")
            last_pid = device_info.get("pid")
            last_tid = device_info.get("tid")
            last_netstat = device_info.get("netstat")
            last_start_pos = device_info.get("start_pos")
            last_end_pos = device_info.get("end_pos")

            diff_ts = int((check_ts - last_update_ts).total_seconds())

            if diff_ts >= 40:
                if last_action == "pause":
                    last_end_ts = last_update_ts
                    last_end_type = "pause"
                else:
                    last_end_ts = last_update_ts + timedelta(seconds=30)
                    last_end_type = "stop"

                if last_end_ts is not None and last_end_ts < last_start_ts:
                    last_end_ts = last_start_ts + timedelta(seconds=30)

                jobs[job_index] = [last_start_type, last_end_type,
                                   convert_into_utc_time(last_end_ts) if last_end_ts is not None else None, last_live,
                                   last_pid, last_tid, last_netstat, last_start_pos, last_end_pos,
                                   convert_into_utc_time(last_start_ts.replace(second=0, minute=0, hour=0)), device_id,
                                   convert_into_utc_time(last_start_ts), last_phid]
                job_index += 1

                del self.device_audio_record[device_id]

        if len(jobs) > 0:
            self.cassandra_connector.do_futures("update_device_audio_data", jobs)

    def get_logs_from_queue(self):
        for data_key in self.data_record.keys():
            self.period_logs_info[data_key] = self.data_record.get(data_key)

        while not self.stop_flag.get() == 1:
            try:
                if not self.audio_queue.empty():
                    now_ts = datetime.now().replace(microsecond=0)

                    message = self.audio_queue.get()
                    log_info = json.loads(message.decode("utf-8"), cls=DateDecoder)

                    log_time = log_info.get("log_time")
                    log_minute = log_time.replace(second=0)
                    log_end_ts = log_minute + timedelta(minutes=1) + timedelta(seconds=self.delay_value)

                    if now_ts <= log_end_ts:
                        if log_minute not in self.period_logs_info:
                            self.period_logs_info[log_minute] = {"log_list": [], "end_ts": log_end_ts}

                        self.period_logs_info[log_minute]["log_list"].append(log_info)
                else:
                    logging.info("audio queue is empty...")
                    time.sleep(1)
            except:
                logging.error("audio queue is broken!!!")
                break


class CalculateAudioDataV2(object):
    def __init__(self, status_value, delay_value, data_record, stop_flag):
        self.status_value = status_value
        self.status_value.set(1)
        self.delay_value = delay_value
        self.data_record = data_record
        self.stop_flag = stop_flag

        config_obj = ConfigReader()

        config_key = "audio_queue"
        audio_queue_config = config_obj.get_section_config(config_key)
        self.queue_ip = audio_queue_config.get("host")
        self.queue_port = int(audio_queue_config.get("port"))
        self.queue_key = audio_queue_config.get("key").encode()

        config_key = "cassandra"
        cassandra_config = config_obj.get_section_config(config_key)
        cassandra_host = [host.strip() for host in cassandra_config.get("hosts").split(',')]
        cassandra_user = cassandra_config.get("username")
        cassandra_password = cassandra_config.get("password")

        self.cassandra_connector = CassandraConnector()
        self.cassandra_connector.db = cassandra_host
        self.cassandra_connector.username = cassandra_user
        self.cassandra_connector.password = cassandra_password

        self.audio_queue = self.get_audio_queue()

        if self.audio_queue is False:
            logging.error("get audio queue error")
            sys.exit(1)

        self.period_logs_info = {}
        self.device_audio_record = Manager().dict()

    @function_retry(retry_times=RETRY_TIMES, period_seconds=PERIOD_SECONDS)
    def get_audio_queue(self):
        BaseManager.register("audio_queue_v2")

        manager = BaseManager(address=(self.queue_ip, self.queue_port), authkey=self.queue_key)
        manager.connect()

        return manager.audio_queue_v2()

    def init_cassandra(self):
        if not self.cassandra_connector.prepare_stmt("update_device_audio_data", "ks_ajmd_dw", "update device_audio_daily set start_type=?,end_type=?,end_time=?,live_status=?,pid=?,tid=?,netstat=?,start_pos=?,end_pos=?,album_id=?,controller=?,audio_id=?,key_id=?,user_id=? where date=? and device_id=? and start_time=? and phid=?"):
            logging.error("init cassandra db error")
            sys.exit(2)

    def run(self):
        self.status_value.set(2)

        self.init_cassandra()

        self.status_value.set(3)

        thread_get_logs = threading.Thread(target=self.get_logs_from_queue)
        thread_calculate_logs = threading.Thread(target=self.calculate_logs)

        thread_get_logs.start()
        thread_calculate_logs.start()

        thread_get_logs.join()
        thread_calculate_logs.join()

        self.status_value.set(4)

        for period_key in self.period_logs_info:
            self.data_record[period_key] = self.period_logs_info[period_key]

        self.status_value.set(-1)

    def calculate_logs(self):
        while not self.stop_flag.get() == 1:
            now_ts = datetime.now().replace(microsecond=0)
            period_key_list = sorted(self.period_logs_info.keys())
            calculate_logs_info = {}

            for period_key in period_key_list:
                if self.period_logs_info[period_key].get("end_ts") is None:
                    continue

                if now_ts > self.period_logs_info[period_key].get("end_ts"):
                    if period_key.hour == 23 and period_key.minute == 59:
                        self.stop_flag.set(1)
                        calculate_logs_info[period_key] = self.period_logs_info[period_key].get("logs_list")
                        del self.period_logs_info[period_key]
                        break
                    else:
                        calculate_logs_info[period_key] = self.period_logs_info[period_key].get("logs_list")
                        del self.period_logs_info[period_key]

            calculate_minute_list = sorted(calculate_logs_info.keys())

            for calculate_minute_item in calculate_minute_list:
                logging.info("start calculate minute v2: {0}".format(calculate_minute_item.strftime("%Y-%m-%d %H:%M:00")))

                start_ts = time.time()

                calculate_one_minute_logs_obj = CalculateOneMinuteLogsV2(self.device_audio_record)
                calculate_one_minute_logs_process = Process(target=calculate_one_minute_logs_obj.run, args=(calculate_logs_info[calculate_minute_item],))
                calculate_one_minute_logs_process.start()
                calculate_one_minute_logs_process.join()

                try:
                    self.check_device_audio_record(calculate_minute_item + timedelta(seconds=59))
                except:
                    error_data = traceback.format_exc()
                    logging.error("check device audio record error: {0}".format(error_data))

                if calculate_minute_item.hour == 23 and calculate_minute_item.minute == 59:
                    try:
                        self.end_device_audio_record(calculate_minute_item + timedelta(seconds=59))
                    except:
                        error_data = traceback.format_exc()
                        logging.error("end device audio record error: {0}".format(error_data))

                end_ts = time.time()

                logging.info("calculate minute v2: {0} over, it takes: {1} seconds".format(calculate_minute_item, int(end_ts - start_ts)))

    def check_device_audio_record(self, check_ts):
        device_id_list = list(self.device_audio_record.keys())
        jobs = {}
        job_index = 0

        for device_id in device_id_list:
            device_info = self.device_audio_record[device_id]

            last_action = device_info.get("action")
            last_update_ts = device_info.get("update_ts")
            last_start_type = device_info.get("start_type")
            last_start_ts = device_info.get("start_ts")
            last_phid = device_info.get("phid")
            last_live = device_info.get("live")
            last_pid = device_info.get("pid")
            last_tid = device_info.get("tid")
            last_album_id = device_info.get("album_id")
            last_netstat = device_info.get("netstat")
            last_controller = device_info.get("controller")
            last_start_pos = device_info.get("start_pos")
            last_end_pos = device_info.get("end_pos")
            last_audio_id = device_info.get("audio_id")
            last_key_id = device_info.get("key_id")
            last_user_id = device_info.get("user_id")

            diff_ts = int((check_ts - last_update_ts).total_seconds())

            if diff_ts >= 40:
                last_end_ts = last_update_ts + timedelta(seconds=30)

                if last_end_ts is not None and last_end_ts < last_start_ts:
                    last_end_ts = last_start_ts + timedelta(seconds=30)

                last_end_type = "stop"
                jobs[job_index] = [last_start_type, last_end_type,
                                   convert_into_utc_time(last_end_ts) if last_end_ts is not None else None, last_live,
                                   last_pid, last_tid, last_netstat, last_start_pos, last_end_pos, last_album_id,
                                   last_controller, last_audio_id, last_key_id, last_user_id,
                                   convert_into_utc_time(last_start_ts.replace(second=0, minute=0, hour=0)), device_id,
                                   convert_into_utc_time(last_start_ts), last_phid]
                job_index += 1

                del self.device_audio_record[device_id]

        if len(jobs) > 0:
            self.cassandra_connector.do_futures("update_device_audio_data", jobs)

    def end_device_audio_record(self, end_ts):
        device_id_list = list(self.device_audio_record.keys())
        jobs = {}
        job_index = 0

        for device_id in device_id_list:
            device_info = self.device_audio_record[device_id]

            last_action = device_info.get("action")
            last_update_ts = device_info.get("update_ts")
            last_start_type = device_info.get("start_type")
            last_start_ts = device_info.get("start_ts")
            last_phid = device_info.get("phid")
            last_live = device_info.get("live")
            last_pid = device_info.get("pid")
            last_tid = device_info.get("tid")
            last_album_id = device_info.get("album_id")
            last_netstat = device_info.get("netstat")
            last_controller = device_info.get("controller")
            last_start_pos = device_info.get("start_pos")
            last_end_pos = device_info.get("end_pos")
            last_audio_id = device_info.get("audio_id")
            last_key_id = device_info.get("key_id")
            last_user_id = device_info.get("user_id")

            diff_ts = int((end_ts - last_update_ts).total_seconds())

            if diff_ts >= 40:
                last_end_ts = last_update_ts + timedelta(seconds=30)
            else:
                last_end_ts = end_ts

            if last_end_ts is not None and last_end_ts < last_start_ts:
                last_end_ts = last_start_ts + timedelta(seconds=30)

            last_end_type = "stop"
            jobs[job_index] = [last_start_type, last_end_type,
                               convert_into_utc_time(last_end_ts) if last_end_ts is not None else None, last_live,
                               last_pid, last_tid, last_netstat, last_start_pos, last_end_pos, last_album_id,
                               last_controller, last_audio_id, last_key_id, last_user_id,
                               convert_into_utc_time(last_start_ts.replace(second=0, minute=0, hour=0)), device_id,
                               convert_into_utc_time(last_start_ts), last_phid]
            job_index += 1

            del self.device_audio_record[device_id]

        if len(jobs) > 0:
            self.cassandra_connector.do_futures("update_device_audio_data", jobs)

    def get_logs_from_queue(self):
        for data_key in self.data_record.keys():
            self.period_logs_info[data_key] = self.data_record.get(data_key)

        while not self.stop_flag.get() == 1:
            try:
                if not self.audio_queue.empty():
                    now_ts = datetime.now().replace(microsecond=0)

                    message = self.audio_queue.get()
                    log_info = json.loads(message.decode("utf-8"), cls=DateDecoder)

                    log_time = log_info.get("log_time")
                    log_minute = log_time.replace(second=0)
                    log_end_ts = log_minute + timedelta(minutes=1) + timedelta(seconds=self.delay_value)

                    if now_ts <= log_end_ts:
                        if log_minute not in self.period_logs_info:
                            self.period_logs_info[log_minute] = {"log_list": [], "end_ts": log_end_ts}

                        self.period_logs_info[log_minute]["log_list"].append(log_info)
                else:
                    logging.info("audio queue is empty...")
                    time.sleep(1)
            except:
                logging.error("audio queue is broken!!!")
                break