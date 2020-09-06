#!/usr/bin/env python3
# coding: utf-8

import io
import logging
import random
import threading
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.io.libevreactor import LibevConnection
from cassandra.query import dict_factory
import functools
from tqdm import tqdm

logger = logging.getLogger("CassandraConnector")
logging.getLogger("cassandra.cluster").setLevel(logging.WARNING)
DEFAULT_RETRY_TIMES = 5



def function_retry(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for retries in range(DEFAULT_RETRY_TIMES):
            try:
                return func(*args, **kwargs)
            except Exception as err:
                logger.error(err)
                logger.info("retry **{0}**: {1}".format(func.__name__, retries + 1))

        return None

    return wrapper


class TqdmToLogger(io.StringIO):
    """
    Output stream for TQDM which will output to logger module instead of the StdOut
    """

    logger = None
    level = None
    buf = ''

    def __init__(self, level=None):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO

    def write(self, buf):
        self.buf = buf.strip('\r\n\t ')

    def flush(self):
        self.logger.log(self.level, self.buf)


class PagedResultHandler(object):
    def __init__(self, future):
        super(PagedResultHandler, self).__init__()

        self.rows = []
        self.error = None
        self.error_message = None
        self.finished_event = threading.Event()
        self.future = future
        self.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        self.finished_event.clear()
        self.future.add_callbacks(callback=self.handle_page, errback=self.handle_error)

    def handle_page(self, rows):
        if rows is not None:
            self.rows.extend(rows)

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        self.error = exc
        self.error_message = "exception: {0}, query: {1}".format(exc, self.future.query)
        self.finished_event.set()


class CassandraConnector(object):
    def __init__(self, **kwargs):
        super(CassandraConnector, self).__init__()

        if "db" in kwargs and isinstance(kwargs.get("db"), list):
            self.db = kwargs.get("db")
        else:
            self.db = []

        if "username" in kwargs and isinstance(kwargs.get("username"), str):
            self.username = kwargs.get("username")
        else:
            self.username = None

        if "password" in kwargs and isinstance(kwargs.get("password"), str):
            self.password = kwargs.get("password")
        else:
            self.password = None

        self.row_factory = dict_factory
        self.cluster = None
        self.sessions = {}
        self.stmts = {}
        self.stmt_info = {}
        self.stop_flag = threading.Event()
        self.default_fetch_size = 1000
        self.default_timeout = 10
        self.tqdm_out = TqdmToLogger(level=logging.INFO)

        self.stop_flag.clear()

    @function_retry
    def prepare_stmt(self, name, keyspace, cql, consistency_level=ConsistencyLevel.LOCAL_QUORUM, row_factory=dict_factory):
        if keyspace not in self.sessions and not self.connect_keyspace(keyspace, row_factory):
            return False

        stmt = self.sessions[keyspace].prepare(cql)
        stmt.consistency_level = consistency_level

        self.stmt_info[name] = {"keyspace": keyspace, "cql": cql, "row_factory": row_factory, "consistency_level": consistency_level}
        self.stmts[name] = {"stmt": stmt, "keyspace": keyspace, "cql": cql, "row_factory": row_factory, "consistency_level": consistency_level}

        return True

    @function_retry
    def connect_keyspace(self, keyspace, row_factory=None):
        if keyspace in self.sessions:
            self.sessions[keyspace].shutdown()
            del self.sessions[keyspace]

        self.connect_db()

        self.sessions[keyspace] = self.cluster.connect(keyspace)
        self.sessions[keyspace].row_factory = self.row_factory if row_factory is None else row_factory
        self.sessions[keyspace].default_timeout = self.default_timeout
        self.sessions[keyspace].default_fetch_size = self.default_fetch_size

        return True

    @function_retry
    def connect_db(self, reconnect=False):
        if self.cluster is not None and not reconnect:
            return True

        self.stmts = {}

        if self.cluster is not None:
            self.cluster.shutdown()

        self.cluster = None

        for key in self.sessions:
            self.sessions[key].shutdown()

        self.sessions = {}

        random.shuffle(self.db)

        ap = PlainTextAuthProvider(self.username, self.password)

        if self.cluster is None:
            self.cluster = Cluster(self.db, port=9042, protocol_version=4, auth_provider=ap)
            self.cluster.connection_class = LibevConnection

        return True

    @function_retry
    def do_futures(self, name, jobs, concurrency=100):
        if name not in self.stmts:
            logger.error("{0} didn't be prepared".format(name))
            return None

        all_result = {}
        part_jobs = []
        i = 0

        for key in jobs:
            if i % concurrency == 0:
                part_jobs.append({})

            part_jobs[-1][key] = jobs[key]
            i += 1

        pbar = tqdm(total=len(jobs), file=self.tqdm_out, mininterval=1, desc=name, unit="req", unit_scale=True)

        for temp_jobs in part_jobs:
            if self.stop_flag.is_set():
                break

            if len(temp_jobs) > 0:
                futures = {}

                for key in temp_jobs.keys():
                    futures[key] = self.do_async_stmt(name, temp_jobs.get(key))

                part_result = self.fetch_futures(name, futures)

                pbar.update(len(part_result))

                if not part_result:
                    return None

                all_result.update(part_result)

        pbar.close()

        return all_result

    @function_retry
    def fetch_futures(self, name, futures):
        result = {}

        for key in futures.keys():
            handler = futures.get(key)

            if handler is None:
                result[key] = None
                continue

            handler.finished_event.wait()

            if handler.error:
                raise Exception("handler error at cql: {0}".format(name))
            else:
                result[key] = handler.rows

        return result

    @function_retry
    def do_async_stmt(self, name, param):
        if name not in self.stmts:
            logger.error("{0} didn't be prepared".format(name))
            return None

        future = self.sessions[self.stmts[name]["keyspace"]].execute_async(self.stmts[name]["stmt"], param)

        return PagedResultHandler(future)

    def close(self):
        self.stop()

        if self.cluster is not None:
            self.cluster.shutdown()

        for key in self.sessions:
            self.sessions[key].shutdown()

    def stop(self):
        self.stop_flag.is_set()