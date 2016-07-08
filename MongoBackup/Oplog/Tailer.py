import logging

from multiprocessing import Queue
from time import sleep

from MongoBackup.Oplog import OplogTail


class OplogTailer:
    def __init__(self, secondaries, backup_name, base_dir, dump_gzip, user=None, password=None, authdb='admin'):
        self.secondaries = secondaries
        self.backup_name = backup_name
        self.base_dir    = base_dir
        self.dump_gzip   = dump_gzip
        self.user        = user
        self.password    = password
        self.authdb      = authdb

        self.response_queue = Queue()
        self.threads        = []
        self._summary       = {}

    def summary(self):
        return self._summary

    def run(self):
        for shard in self.secondaries:
            secondary  = self.secondaries[shard]
            shard_name = secondary['replSet']
            host, port = secondary['host'].split(":")
            thread = OplogTail(
                self.response_queue,
                shard_name,
                self.base_dir,
                host,
                port,
                self.dump_gzip,
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
