import logging

from multiprocessing import Queue
from time import sleep

from TailerThread import TailerThread


class Tailer:
    def __init__(self, config, secondaries, base_dir):
        self.config      = config
        self.secondaries = secondaries
        self.base_dir    = base_dir
        self.backup_name = self.config.name
        self.user        = self.config.user
        self.password    = self.config.password
        self.authdb      = self.config.authdb

        self.response_queue = Queue()
        self.threads        = []
        self._summary       = {}

    def compression(self, method=None):
        if method:
	    self.config.oplog.compression = method.lower()
	return self.config.oplog.compression

    def do_gzip(self):
	if self.compression() == 'gzip':
	    return True
        return False

    def summary(self):
        return self._summary

    def run(self):
        for shard in self.secondaries:
            secondary  = self.secondaries[shard]
            shard_name = secondary['replSet']
            host, port = secondary['host'].split(":")
            thread = TailerThread(
                self.response_queue,
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

    def stop(self):
        logging.info("Stopping oplog tailing threads")
        for thread in self.threads:
            thread.stop()
        for thread in self.threads:
            while thread.is_alive():
                sleep(1)
        logging.info("Stopped all oplog threads")

        while not self.response_queue.empty():
            response = self.response_queue.get()
            host = response['host']
            port = response['port']
            if host not in self._summary:
                self._summary[host] = {}
            self._summary[host][port] = response

        return self._summary

    def close(self):
        self.stop()
