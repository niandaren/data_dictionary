#!/usr/bin/env python3
# coding: utf-8

from datetime import datetime, timezone, tzinfo, timedelta
import logging
from logging.handlers import RotatingFileHandler
from threading import Lock



class LogPrint(object):
    __instance = None
    __thread_lock = Lock()
    __level_map = {"debug": logging.DEBUG, "info": logging.INFO, "waring": logging.WARNING, "critical": logging.CRITICAL}

    def __new__(cls, *args, **kwargs):
        if cls.__instance is not None:
            return cls.__instance

        with cls.__thread_lock:
            cls.__instance = super().__new__(cls)
            return cls.__instance

    def init_logger(self, log_path, log_formatter, log_level=logging.INFO, max_bytes=10*1024*1024, backup_count=10):
        logger = logging.getLogger()
        logger.setLevel(log_level)
        output_rthandler = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count)
        output_formatter = logging.Formatter(log_formatter)
        output_rthandler.setFormatter(output_formatter)
        logger.addHandler(output_rthandler)

        return logger

    def set_log_level(self, log_name, log_level):
        if log_level not in LogPrint.__level_map:
            raise Exception("log level error")

        logging.getLogger(log_name).setLevel(LogPrint.__level_map.get(log_level))


def print_log(*log):
    now = datetime.now(timezone(timedelta(hours=8)))
    now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    print(now_str, *log)