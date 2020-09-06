#!/usr/bin/env python3
# coding: utf-8

from datetime import datetime, timezone, tzinfo, timedelta



def print_log(*log):
    now = datetime.now(timezone(timedelta(hours=8)))
    now_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    print(now_str, *log)