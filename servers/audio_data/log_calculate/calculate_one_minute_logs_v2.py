#!/usr/bin/env python3
# coding: utf-8

import sys
import os
import traceback
from datetime import timedelta
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
        if not self.cassandra_connector.prepare_stmt("update_device_audio_data", "ks_ajmd_dw", "update device_audio_daily set start_type=?,end_type=?,end_time=?,live_status=?,pid=?,tid=?,netstat=?,start_pos=?,end_pos=?,album_id=?,controller=?,audio_id=?,key_id=?,user_id=? where date=? and device_id=? and start_time=? and phid=?"):
            logging.error("init cassandra db error")
            sys.exit(1)

    def run(self, calculate_logs):
        self.init_cassandra()

        self.action_adjust_list = [("stop", "start"), ("complete", "start"), ("start", "play"), ("play", "complete"),
                                   ("play", "stop"), ("play", "pause"), ("resume", "play"), ("pause", "resume"),
                                   ("seek_start", "start")]

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
                        album_id = result[device_id][phid][start_time]["album_id"]
                        netstat = result[device_id][phid][start_time]["netstat"]
                        controller = result[device_id][phid][start_time]["controller"]
                        start_pos = result[device_id][phid][start_time]["start_pos"]
                        end_pos = result[device_id][phid][start_time]["end_pos"]
                        audio_id = result[device_id][phid][start_time]["audio_id"]
                        key_id = result[device_id][phid][start_time]["key_id"]
                        user_id = result[device_id][phid][start_time]["user_id"]

                        if end_time is not None and end_time < start_time:
                            end_time = start_time + timedelta(seconds=30)

                        jobs[job_index] = [start_type, end_type,
                                           convert_into_utc_time(end_time) if end_time is not None else None, live_status,
                                           pid, tid, netstat, start_pos, end_pos, album_id, controller, audio_id,
                                           key_id, user_id,
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
                action = log_info.get("params_info").get("t2")
                ts = log_info.get("log_time")
                phid = log_info.get("params_info").get("phid")

                if action is None or phid is None:
                    continue

                try:
                    phid = int(phid)
                except:
                    logging.error("the wrong phid: {0}".format(phid))
                    continue

                action = action.lower()

                try:
                    pid = int(log_info.get("params_info").get("pid"))
                except:
                    # logging.warning("the wrong pid: {0}".format(log_info.get("params_info").get("pid")))
                    pid = 0

                try:
                    tid = int(log_info.get("params_info").get("tid"))
                except:
                    # logging.warning("the wrong tid: {0}".format(log_info.get("params_info").get("tid")))
                    tid = 0

                try:
                    live = int(log_info.get("params_info").get("islive"))
                except:
                    # logging.warning("the wrong live status: {0}".format(log_info.get("params_info").get("islive")))
                    live = 0

                try:
                    album_id = int(log_info.get("params_info").get("albumId"))
                except:
                    # logging.warning("the wrong album id: {0}".format(log_info.get("params_info").get("albumId")))
                    album_id = 0

                try:
                    audio_id = int(log_info.get("params_info").get("audioId"))
                except:
                    audio_id = None

                try:
                    key_id = int(log_info.get("params_info").get("keyId"))
                except:
                    key_id = None

                try:
                    user_id = int(log_info.get("params_info").get("userId"))
                except:
                    user_id = 0

                play_time = int(round(float(log_info.get("params_info").get("playtime")), 0)) if log_info.get("params_info").get("playtime") is not None else None
                start_time = int(round(float(log_info.get("params_info").get("startTime")), 0)) if log_info.get("params_info").get("startTime") is not None else None
                target_time = int(round(float(log_info.get("params_info").get("targetTime")), 0)) if log_info.get("params_info").get("targetTime") is not None else None
                total_time = int(round(float(log_info.get("params_info").get("totalTime")), 0)) if log_info.get("params_info").get("totalTime") is not None else None
                netstat = log_info.get("params_info").get("netstat")
                controller = log_info.get("params_info").get("controller")

                if live < 0:
                    live = 0

                adjust_log_info = {"phid": phid, "pid": pid, "user_id": user_id, "tid": tid, "live": live, "album_id": album_id}

                if device_id not in self.device_audio_record:
                    if action not in ["stop", "pause", "complete", "buffer", "buffer_full"]:
                        if action == "resume":
                            start_type = "resume"
                            start_pos = play_time
                        elif action == "seek_start":
                            start_type = "seek_start"
                            start_pos = target_time
                        elif action == "play":
                            start_type = "play"
                            start_pos = play_time
                        else:
                            start_type = "start"
                            start_pos = None

                        if device_id not in devices_audio_info:
                            devices_audio_info[device_id] = {}

                        if phid not in devices_audio_info[device_id]:
                            devices_audio_info[device_id][phid] = {}

                        devices_audio_info[device_id][phid][ts] = {"start_type": start_type, "end_type": None,
                                                                "end_time": None,
                                                                "live_status": None if action in ["start"] else live,
                                                                "pid": pid, "tid": tid, "album_id": album_id,
                                                                "start_pos": start_pos, "end_pos": None,
                                                                "netstat": netstat, "controller": controller,
                                                                "audio_id": audio_id, "key_id": key_id, "user_id": user_id}

                        self.device_audio_record[device_id] = {"action": action, "start_type": start_type, "start_ts": ts,
                                                            "update_ts": ts, "phid": phid, "pid": pid, "tid": tid,
                                                            "album_id": album_id, "audio_id": audio_id, "key_id": key_id, "user_id": user_id,
                                                            "live": None if action in ["start"] else live,
                                                            "start_pos": start_pos, "end_pos": None, "netstat": netstat, "controller": controller}
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
                    last_album_id = device_last_log.get("album_id")
                    last_start_pos = device_last_log.get("start_pos")
                    last_end_pos = device_last_log.get("end_pos")
                    last_netstat = device_last_log.get("netstat")
                    last_controller = device_last_log.get("controller")
                    last_audio_id = device_last_log.get("audio_id")
                    last_key_id = device_last_log.get("key_id")
                    last_user_id = device_last_log.get("user_id")

                    if action in ["start", "seek_start", "pause", "stop", "complete"]:
                        if action == "start":
                            diff_ts = int(ts.timestamp() - last_update_ts.timestamp())

                            if diff_ts >= 40:
                                last_end_ts = last_update_ts + timedelta(seconds=30)
                            else:
                                last_end_ts = ts

                            last_end_type = "stop"

                            if device_id not in devices_audio_info:
                                devices_audio_info[device_id] = {}

                            if last_phid not in devices_audio_info[device_id]:
                                devices_audio_info[device_id][last_phid] = {}

                            devices_audio_info[device_id][last_phid][last_start_ts] = {"start_type": last_start_type,
                                                                                    "end_type": last_end_type,
                                                                                    "end_time": last_end_ts,
                                                                                    "live_status": last_live,
                                                                                    "pid": last_pid, "tid": last_tid,
                                                                                    "album_id": last_album_id,
                                                                                    "start_pos": last_start_pos,
                                                                                    "end_pos": last_end_pos,
                                                                                    "netstat": last_netstat,
                                                                                    "controller": last_controller,
                                                                                    "audio_id": last_audio_id,
                                                                                    "key_id": last_key_id,
                                                                                    "user_id": last_user_id}

                            del self.device_audio_record[device_id]

                            start_type = "start"

                            if device_id not in devices_audio_info:
                                devices_audio_info[device_id] = {}

                            if phid not in devices_audio_info[device_id]:
                                devices_audio_info[device_id][phid] = {}

                            devices_audio_info[device_id][phid][ts] = {"start_type": start_type, "end_type": None,
                                                                    "end_time": None, "live_status": None, "pid": pid,
                                                                    "tid": tid, "album_id": album_id, "start_pos": None,
                                                                    "end_pos": None, "netstat": netstat, "controller": controller,
                                                                    "audio_id": audio_id, "key_id": key_id, "user_id": user_id}

                            self.device_audio_record[device_id] = {"action": action, "start_type": start_type,
                                                                "start_ts": ts, "update_ts": ts, "phid": phid,
                                                                "pid": pid, "tid": tid, "album_id": album_id,
                                                                "live": None, "start_pos": None, "end_pos": None,
                                                                "netstat": netstat, "controller": controller,
                                                                "audio_id": audio_id, "key_id": key_id, "user_id": user_id}
                        else:
                            if not self.is_same_target(adjust_log_info, device_last_log):
                                continue

                            if action == "seek_start":
                                last_end_ts = ts
                                last_end_type = "seek_stop"
                                last_end_pos = start_time
                            elif action == "pause":
                                last_end_ts = ts
                                last_end_type = "pause"
                                last_end_pos = play_time
                            elif action == "stop":
                                last_end_ts = ts
                                last_end_type = "stop"
                                last_end_pos = last_end_pos if play_time == 0 else play_time
                            elif action == "complete":
                                last_end_ts = ts
                                last_end_type = "complete"
                                last_end_pos = total_time

                            if device_id not in devices_audio_info:
                                devices_audio_info[device_id] = {}

                            if last_phid not in devices_audio_info[device_id]:
                                devices_audio_info[device_id][last_phid] = {}

                            devices_audio_info[device_id][last_phid][last_start_ts] = {"start_type": last_start_type,
                                                                                    "end_type": last_end_type,
                                                                                    "end_time": last_end_ts,
                                                                                    "live_status": live,
                                                                                    "pid": last_pid, "tid": last_tid,
                                                                                    "album_id": last_album_id,
                                                                                    "start_pos": last_start_pos if last_start_pos is not None else last_end_pos,
                                                                                    "end_pos": last_end_pos,
                                                                                    "netstat": netstat if last_netstat is None else last_netstat,
                                                                                    "controller": controller if last_controller is None else last_controller,
                                                                                    "audio_id": last_audio_id, "key_id": last_key_id, "user_id": last_user_id}

                            del self.device_audio_record[device_id]

                            if action == "seek_start":
                                start_type = "seek_start"

                                if device_id not in devices_audio_info:
                                    devices_audio_info[device_id] = {}

                                if phid not in devices_audio_info[device_id]:
                                    devices_audio_info[device_id][phid] = {}

                                devices_audio_info[device_id][phid][ts] = {"start_type": start_type, "end_type": None,
                                                                        "end_time": None, "live_status": live,
                                                                        "pid": pid, "tid": tid, "album_id": album_id,
                                                                        "start_pos": target_time, "end_pos": None,
                                                                        "netstat": netstat, "controller": controller,
                                                                        "audio_id": audio_id, "key_id": key_id, "user_id": user_id}

                                self.device_audio_record[device_id] = {"action": action, "start_type": start_type,
                                                                    "start_ts": ts, "update_ts": ts, "phid": phid,
                                                                    "pid": pid, "tid": tid, "album_id": album_id,
                                                                    "live": live, "start_pos": target_time,
                                                                    "end_pos": None, "netstat": netstat, "controller": controller,
                                                                    "audio_id": audio_id, "key_id": key_id, "user_id": user_id}
                    else:
                        if action in ["buffer", "buffer_full"]:
                            pass
                        else:
                            diff_ts = int((ts - last_update_ts).total_seconds())

                            if diff_ts >= 40:
                                last_end_ts = last_update_ts + timedelta(seconds=30)
                                last_end_type = "stop"

                                if device_id not in devices_audio_info:
                                    devices_audio_info[device_id] = {}

                                if last_phid not in devices_audio_info[device_id]:
                                    devices_audio_info[device_id][last_phid] = {}

                                devices_audio_info[device_id][last_phid][last_start_ts] = {"start_type": last_start_type,
                                                                                        "end_type": last_end_type,
                                                                                        "end_time": last_end_ts,
                                                                                        "live_status": live,
                                                                                        "pid": last_pid,
                                                                                        "tid": last_tid,
                                                                                        "album_id": last_album_id,
                                                                                        "start_pos": last_start_pos if last_start_pos is not None else last_end_pos,
                                                                                        "end_pos": last_end_pos,
                                                                                        "netstat": netstat if last_netstat is None else last_netstat,
                                                                                        "controller": controller if last_controller is None else last_controller,
                                                                                        "audio_id": last_audio_id, "key_id": last_key_id, "user_id": last_user_id}

                                del self.device_audio_record[device_id]

                                start_type = "start"

                                if device_id not in devices_audio_info:
                                    devices_audio_info[device_id] = {}

                                if phid not in devices_audio_info[device_id]:
                                    devices_audio_info[device_id][phid] = {}

                                devices_audio_info[device_id][phid][ts] = {"start_type": start_type, "end_type": None,
                                                                        "end_time": None, "live_status": live,
                                                                        "pid": pid, "tid": tid, "album_id": album_id,
                                                                        "start_pos": play_time, "end_pos": None,
                                                                        "netstat": netstat, "controller": controller,
                                                                        "audio_id": audio_id, "key_id": key_id, "user_id": user_id}

                                self.device_audio_record[device_id] = {"action": action, "start_type": start_type,
                                                                    "start_ts": ts, "update_ts": ts, "phid": phid,
                                                                    "pid": pid, "tid": tid, "album_id": album_id,
                                                                    "live": live, "start_pos": play_time,
                                                                    "end_pos": None, "netstat": netstat, "controller": controller,
                                                                    "audio_id": audio_id, "key_id": key_id, "user_id": user_id}
                            else:
                                self.device_audio_record[device_id] = {"action": action, "start_type": last_start_type,
                                                                    "start_ts": last_start_ts, "update_ts": ts,
                                                                    "phid": last_phid, "pid": last_pid, "tid": last_tid,
                                                                    "album_id": last_album_id, "live": live,
                                                                    "start_pos": last_start_pos if last_start_pos is not None else play_time,
                                                                    "end_pos": play_time,
                                                                    "netstat": netstat if last_netstat is None else last_netstat,
                                                                    "controller": controller if last_controller is None else last_controller,
                                                                    "audio_id": last_audio_id, "key_id": last_key_id, "user_id": last_user_id}
                                if device_id in devices_audio_info:
                                    if last_phid in devices_audio_info[device_id]:
                                        if last_start_ts in devices_audio_info[device_id][last_phid]:
                                            if devices_audio_info[device_id][last_phid][last_start_ts]["live_status"] is None:
                                                devices_audio_info[device_id][last_phid][last_start_ts]["live_status"] = live
                                            if devices_audio_info[device_id][last_phid][last_start_ts]["start_pos"] is None:
                                                devices_audio_info[device_id][last_phid][last_start_ts]["start_pos"] = play_time
                                            if devices_audio_info[device_id][last_phid][last_start_ts]["controller"] is None:
                                                devices_audio_info[device_id][last_phid][last_start_ts]["controller"] = controller

        return devices_audio_info

    def is_same_target(self, a, b):
        for k in ["phid", "pid", "user_id", "tid", "live", "album_id"]:
            if k == "live" and b.get(k) is None:
                continue

            if a.get(k) != b.get(k):
                return False

        return True

    def adjust_logs(self, lines):
        lines = sorted(lines, key=lambda l: l.get("log_time"))

        adjust_lines = []

        while len(lines) > 1:
            a = lines[0]
            b = lines[1]
            result = self.action_sort(a, b)
            adjust_lines.append(result[0])
            lines = lines[1:]
            lines[0] = result[1]

        adjust_lines.append(lines[0])

        return adjust_lines

    def action_sort(self, a, b):
        if a.get("log_time") != b.get("log_time"):
            return (a, b)

        a_action = a.get("params_info").get("t2").lower() if a.get("params_info").get("t2") is not None else None
        b_action = b.get("params_info").get("t2").lower() if b.get("params_info").get("t2") is not None else None

        if (a_action, b_action) in self.action_adjust_list:
            return (a, b)

        if (b_action, a_action) in self.action_adjust_list:
            return (b, a)

        return (a, b)

    def get_devices_logs(self, logs_info):
        devices_logs = {}

        for log_info in logs_info:
            device = log_info.get("device")
            if device not in devices_logs:
                devices_logs[device] = []
            devices_logs[device].append(log_info)

        return devices_logs