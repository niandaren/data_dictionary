#!/usr/bin/env python3
# coding: utf-8

import sys
import os
import traceback
from datetime import datetime, timedelta
import logging

current_path = os.path.realpath(__file__)
module_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_path))))
sys.path.append(module_path)

from modules.config_reader.parse_config import ConfigReader
from modules.connector.cassandra_connector import CassandraConnector
from modules.time_handle.handle_time import convert_into_utc_time



class CalculateOneMinuteLogs(object):
    def __init__(self, device_audio_record):
        self.device_audio_record = device_audio_record

        config_obj = ConfigReader()

        config_key = "cassandra"
        cassandra_config = config_obj.get_section_config(config_key)
        cassandra_host = [host.strip() for host in cassandra_config.get("hosts").split(',')]
        cassandra_user = cassandra_config.get("username")
        cassandra_password = cassandra_config.get("password")

        self.cassandra_connector = CassandraConnector()
        self.cassandra_connector.db = cassandra_host
        self.cassandra_connector.username = cassandra_user
        self.cassandra_connector.password = cassandra_password

    def init_cassandra(self):
        if not self.cassandra_connector.prepare_stmt("update_device_audio_data", "data_dictionary", "update device_audio_daily set start_type=?,end_type=?,end_time=?,live_status=?,pid=?,tid=?,netstat=? where date=? and device_id=? and start_time=? and phid=?"):
            logging.error("init cassandra db error")
            sys.exit(1)

    def run(self, calculate_logs):
        self.init_cassandra()

        result = self.calculate_audio_logs(calculate_logs)

        jobs = {}
        job_index = 0

        if result is not None:
            for device_id in result:
                for phid in result[device_id]:
                    for start_time in result[device_id][phid]:
                        end_type = result[device_id][phid][start_time]["end_type"]
                        end_time = result[device_id][phid][start_time]["end_time"]
                        start_type = result[device_id][phid][start_time]["start_type"]
                        live_status = result[device_id][phid][start_time]["live_status"]
                        pid = result[device_id][phid][start_time]["pid"]
                        tid = result[device_id][phid][start_time]["tid"]
                        netstat = None

                        if end_time is not None and end_time < start_time:
                            end_time = start_time + timedelta(seconds=30)

                        jobs[job_index] = [start_type, end_type,
                                           convert_into_utc_time(end_time) if end_time is not None else None, live_status,
                                           pid, tid, netstat,
                                           convert_into_utc_time(start_time.replace(second=0, minute=0, hour=0)), device_id,
                                           convert_into_utc_time(start_time), phid]
                        job_index += 1

        if len(jobs) > 0:
            try:
                self.cassandra_connector.do_futures("update_device_audio_data", jobs)
            except:
                error_data = traceback.format_exc()
                logging.error("update device audio data error: {0}".format(error_data))

    def calculate_audio_logs(self, audio_logs):
        devices_logs = self.get_devices_logs(audio_logs)

        if len(devices_logs) == 0:
            return None

        devices_audio_info = self.get_devices_audio_info(devices_logs)

        return devices_audio_info

    def get_devices_audio_info(self, devices_logs):
        devices_audio_info = {}

        for device_id in devices_logs:
            device_logs = self.adjust_logs(devices_logs[device_id])

            for log_info in device_logs:
                action = log_info.get("action")
                ts = log_info.get("ts")
                phid = log_info.get("phid")

                if action is None or phid is None:
                    continue

                try:
                    phid = int(phid)
                except:
                    logging.error("the wrong phid: {0}".format(phid))
                    continue

                try:
                    pid = int(log_info.get("pid"))
                except:
                    # logging.warning("the wrong pid: {0}".format(log_info.get("pid")))
                    pid = 0

                try:
                    tid = int(log_info.get("tid"))
                except:
                    # logging.warning("the wrong tid: {0}".format(log_info.get("tid")))
                    tid = 0

                try:
                    live = int(log_info.get("live"))
                except:
                    # logging.warning("the wroing live status: {0}".format(log_info.get("live")))
                    live = 0

                if live < 0:
                    live = 0

                adjust_log_info = {"phid": phid, "pid": pid, "tid": tid, "live": live}

                if device_id not in self.device_audio_record:
                    if action not in ["stop", "pause", "complete"]:
                        start_type = "start"
                        self.device_audio_record[device_id] = {"action": action, "start_type": start_type, "start_ts": ts, "update_ts": ts, "phid": phid, "pid": pid, "tid": tid, "live": None if action in ["start", "buffer"] else live}

                        if device_id not in devices_audio_info:
                            devices_audio_info[device_id] = {}

                        if phid not in devices_audio_info[device_id]:
                            devices_audio_info[device_id][phid] = {}

                        devices_audio_info[device_id][phid][ts] = {"start_type": start_type, "end_type": None, "end_time": None, "live_status": None if action in ["start", "buffer"] else live, "pid": pid, "tid": tid}
                else:
                    device_last_log = self.device_audio_record[device_id]
                    last_action = device_last_log.get("action")
                    last_update_ts = device_last_log.get("update_ts")
                    last_start_type = device_last_log.get("start_type")
                    last_start_ts = device_last_log.get("start_ts")
                    last_phid = device_last_log.get("phid")
                    last_live = device_last_log.get("live")
                    last_pid = device_last_log.get("pid")
                    last_tid = device_last_log.get("tid")

                    if action in ["start", "stop", "complete"]:
                        if last_action == "pause":
                            last_end_ts = last_update_ts
                            last_end_type = "pause"
                        else:
                            if action == "start":
                                diff_ts = int(ts.timestamp() - last_update_ts.timestamp())
                                if diff_ts >= 40:
                                    last_end_ts = last_update_ts + timedelta(seconds=30)
                                else:
                                    last_end_ts = ts
                                last_end_type = "stop"
                            else:
                                last_end_ts = ts
                                if action == "complete":
                                    last_end_type = "complete"
                                else:
                                    last_end_type = "stop"

                        if device_id not in devices_audio_info:
                            devices_audio_info[device_id] = {}

                        if last_phid not in devices_audio_info[device_id]:
                            devices_audio_info[device_id][last_phid] = {}

                        devices_audio_info[device_id][last_phid][last_start_ts] = {"start_type": last_start_type, "end_type": last_end_type, "end_time": last_end_ts, "live_status": last_live, "pid": last_pid, "tid": last_tid}

                        del self.device_audio_record[device_id]

                        if action == "start":
                            self.device_audio_record[device_id] = {"action": action, "start_type": "start", "start_ts": ts, "update_ts": ts, "phid": phid, "pid": pid, "tid": tid, "live": None if action in ["start", "buffer"] else live}

                            if device_id not in devices_audio_info:
                                devices_audio_info[device_id] = {}

                            if phid not in devices_audio_info[device_id]:
                                devices_audio_info[device_id][phid] = {}

                            devices_audio_info[device_id][phid][ts] = {"start_type": "start", "end_type": None, "end_time": None, "live_status": None if action in ["start", "buffer"] else live, "pid": pid, "tid": tid}
                    elif action == "pause":
                        last_end_ts = ts
                        last_end_type = "pause"
                        self.device_audio_record[device_id] = {"action": action, "start_type": last_start_type, "start_ts": last_start_ts, "update_ts": ts, "phid": last_phid, "pid": last_pid, "tid": last_tid, "live": live}

                        if device_id not in devices_audio_info:
                            devices_audio_info[device_id] = {}

                        if last_phid not in devices_audio_info[device_id]:
                            devices_audio_info[device_id][last_phid] = {}

                        devices_audio_info[device_id][last_phid][last_start_ts] = {"start_type":last_start_type, "end_type":last_end_type, "end_time":last_end_ts, "live_status":live, "pid":last_pid, "tid":last_tid}
                    else:
                        if not self.is_same_target(adjust_log_info, device_last_log):
                            if action == "buffer":
                                pass
                            else:
                                if last_action == "pause":
                                    last_end_ts = last_update_ts
                                    last_end_type = "pause"
                                else:
                                    diff_ts = int((ts - last_update_ts).total_seconds())

                                    if diff_ts >= 40:
                                        last_end_ts = last_update_ts + timedelta(seconds=30)
                                    else:
                                        last_end_ts = ts

                                    last_end_type = "stop"

                                if device_id not in devices_audio_info:
                                    devices_audio_info[device_id] = {}

                                if last_phid not in devices_audio_info[device_id]:
                                    devices_audio_info[device_id][last_phid] = {}

                                devices_audio_info[device_id][last_phid][last_start_ts] = {"start_type": last_start_type, "end_type": last_end_type, "end_time": last_end_ts, "live_status": last_live, "pid": last_pid, "tid": last_tid}
                                self.device_audio_record[device_id] = {"action": action, "start_type": "start", "start_ts": ts, "update_ts": ts, "phid": phid, "pid": pid, "tid": tid, "live": live}

                                if device_id not in devices_audio_info:
                                    devices_audio_info[device_id] = {}

                                if phid not in devices_audio_info[device_id]:
                                    devices_audio_info[device_id][phid] = {}

                                devices_audio_info[device_id][phid][ts] = {"start_type": "start", "end_type": None, "end_time": None, "live_status": live, "pid": pid, "tid": tid}
                        else:
                            if action == "buffer":
                                self.device_audio_record[device_id] = {"action": action, "start_type": last_start_type, "start_ts": last_start_ts, "update_ts": ts, "phid": last_phid, "pid": last_pid, "tid": last_tid, "live": last_live}
                            else:
                                diff_ts = int((ts - last_update_ts).total_seconds())

                                if diff_ts >= 40:
                                    if last_action == "pause":
                                        last_end_ts = last_update_ts
                                        last_end_type = "pause"
                                    else:
                                        last_end_ts = last_update_ts + timedelta(seconds=30)
                                        last_end_type = "stop"

                                    if device_id not in devices_audio_info:
                                        devices_audio_info[device_id] = {}

                                    if last_phid not in devices_audio_info[device_id]:
                                        devices_audio_info[device_id][last_phid] = {}

                                    devices_audio_info[device_id][last_phid][last_start_ts] = {"start_type": last_start_type, "end_type": last_end_type, "end_time": last_end_ts, "live_status": last_live, "pid": last_pid, "tid": last_tid}
                                    self.device_audio_record[device_id] = {"action": action, "start_type": "start", "start_ts": ts, "update_ts": ts, "phid": phid, "pid": pid, "tid": tid, "live": live}

                                    if device_id not in devices_audio_info:
                                        devices_audio_info[device_id] = {}

                                    if phid not in devices_audio_info[device_id]:
                                        devices_audio_info[device_id][phid] = {}

                                    devices_audio_info[device_id][phid][ts] = {"start_type": "start", "end_type": None, "end_time": None, "live_status": live, "pid": pid, "tid": tid}
                                else:
                                    if last_action == "pause":
                                        start_type = "resume"
                                        self.device_audio_record[device_id] = {"action": action, "start_type": start_type, "start_ts": ts, "update_ts": ts, "phid": phid, "pid": pid, "tid": tid, "live": live}

                                        if device_id not in devices_audio_info:
                                            devices_audio_info[device_id] = {}

                                        if phid not in devices_audio_info[device_id]:
                                            devices_audio_info[device_id][phid] = {}

                                        devices_audio_info[device_id][phid][ts] = {"start_type": start_type, "end_type": None, "end_time": None, "live_status": live, "pid": pid, "tid": tid}
                                    else:
                                        self.device_audio_record[device_id] = {"action": action, "start_type": last_start_type, "start_ts": last_start_ts, "update_ts": ts, "phid": last_phid, "pid": last_pid, "tid": last_tid, "live": live}
                                        if device_id in devices_audio_info:
                                            if last_phid in devices_audio_info[device_id]:
                                                if last_start_ts in devices_audio_info[device_id][last_phid]:
                                                    if devices_audio_info[device_id][last_phid][last_start_ts]["live_status"] is None:
                                                        devices_audio_info[device_id][last_phid][last_start_ts]["live_status"] = live

        return devices_audio_info

    def is_same_target(self, a, b):
        for k in ["phid", "pid", "tid", "live"]:
            if k == "live" and b.get(k) is None:
                continue

            if a.get(k) != b.get(k):
                return False

        return True

    def adjust_logs(self, lines):
        lines = [self.radio_extract(line) for line in lines]

        for line in lines:
            if line.get("client_ts") is None:
                line["client_ts"] = line.get("ts")

        min_server_ts = min([l.get("ts") for l in lines])
        lines = sorted(lines, key=lambda l: self.action_sort(l.get("action")))
        lines = sorted(lines, key=lambda l: l.get("client_ts"))
        min_client_ts = min([l.get("client_ts") for l in lines])

        for line in lines:
            line["server_ts"] = line.get("ts")
            line["ts"] = min_server_ts + (line.get("client_ts") - min_client_ts)

        return lines

    def action_sort(self, action):
        weight = {"start": 1, "stop": 0}

        return 2 if weight.get(action) is None else weight.get(action)

    def radio_extract(self, line):
        params = line["params_info"]
        ret = {"action": params.get("t2"), "ts": line.get("log_time")}

        for k in ["pid", "phid", "live", "tid"]:
            ret[k] = params.get(k)

        if params.get("ts") is None:
            ret["client_ts"] = line.get("log_time")
        elif len(str(params.get("ts"))) >= 10:
            try:
                if len(str(params.get("ts"))) > 10:
                    ts = int(params.get("ts")) / 1000
                else:
                    ts = int(params.get("ts"))
                # ts = int(params.get("ts")) / (10 ** (len(str(params.get("ts"))) - 10))
                ret["client_ts"] = datetime.fromtimestamp(ts)
            except:
                logging.error("got client ts {0}".format(params.get("ts")))
                ret["client_ts"] = line.get("log_time")
        else:
            ret["client_ts"] = line.get("log_time")

        return ret

    def get_devices_last_log(self, devices_logs):
        devices_last_log = {}

        for device in devices_logs:
            device_logs = sorted(devices_logs[device], key=lambda l: l["log_time"], reverse=True)
            log_info = device_logs[0]
            params_info = log_info.get("params_info")
            action = params_info.get("t2")
            phid = params_info.get("phid")

            if action is None or phid is None:
                continue

            phid = int(phid)
            devices_last_log[device] = {"action":action, "ts":log_info.get("log_time"), "phid":phid}

            for key in ["pid", "tid", "live"]:
                devices_last_log[device][key] = int(params_info.get(key)) if params_info.get(key) is not None else 0

        return devices_last_log

    def get_devices_logs(self, logs_info):
        devices_logs = {}

        for log_info in logs_info:
            device = log_info.get("device")

            if device not in devices_logs:
                devices_logs[device] = []

            devices_logs[device].append(log_info)

        return devices_logs