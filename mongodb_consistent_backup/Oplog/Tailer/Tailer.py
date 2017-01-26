import logging

from bson.timestamp import Timestamp
from multiprocessing import Manager
from time import time, sleep

from TailThread import TailThread
from mongodb_consistent_backup.Common import parse_method


class Tailer:
    def __init__(self, config, secondaries, base_dir):
        self.config      = config
        self.secondaries = secondaries
        self.base_dir    = base_dir
        self.backup_name = self.config.name
        self.user        = self.config.user
        self.password    = self.config.password
        self.authdb      = self.config.authdb

        self.threads        = []
        self._summary       = {}

        self._manager      = Manager()
        self.thread_states = {}

    def compression(self, method=None):
        if method:
            self.config.oplog.compression = parse_method(method)
            logging.info("Setting oplog compression method to: %s" % self.config.oplog.compression)
        return parse_method(self.config.oplog.compression)

    def do_gzip(self):
        if self.compression() == 'gzip':
            return True
        return False

    def summary(self):
        return self._summary

    def thread_state(self, shard):
        if not shard in self.thread_states:
            self.thread_states[shard] = self._manager.dict()
        return self.thread_states[shard]

    def run(self):
        for shard in self.secondaries:
            secondary  = self.secondaries[shard]
            shard_name = secondary['replSet']
            host, port = secondary['host'].split(":")
            thread = TailThread(
                self.thread_state(shard),
                shard_name,
                self.base_dir,
                host,
                port,
                self.do_gzip(),
                self.user,
                self.password,
                self.authdb
            )
            self.threads.append(thread)
        for thread in self.threads:
            thread.start()

    def stop(self, timestamp=None):
        if not timestamp:
            timestamp = Timestamp(int(time()), 0)
        logging.info("Stopping oplog tailing threads at >= %s" % timestamp)
        for shard in self.secondaries:
            thread_state = self.thread_state(shard)
            thread_state['stop_ts'] = timestamp
        for thread in self.threads:
            while thread.is_alive():
                sleep(1)
        logging.info("Stopped all oplog threads")

        for shard in self.secondaries:
            state = self.thread_state(shard)
            host  = state['host']
            port  = state['port']
            if host not in self._summary:
                self._summary[host] = {}
            self._summary[host][port] = state

        return self._summary

    def close(self):
        self.stop()
