#!/usr/bin/env python3
# coding: utf-8

import time
from datetime import datetime



def convert_into_utc_time(ts):
    time_struct = time.mktime(ts.timetuple())
    utc_st = datetime.utcfromtimestamp(time_struct)

    return utc_st

def convert_into_local_time(ts):
    now_timestamp = time.time()
    local_time = datetime.fromtimestamp(now_timestamp)
    utc_time = datetime.utcfromtimestamp(now_timestamp)
    offset = local_time - utc_time
    local_st = ts + offset

    return local_st