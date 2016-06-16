import logging

from multiprocessing import Queue
from time import sleep

from MongoBackup.Oplog import OplogTail
from MongoBackup.ReplsetHandler import ReplsetHandlerSharded


class OplogTailer:
    def __init__(self, backup_name, base_dir, host, port, dump_gzip, max_repl_lag_secs, user=None, password=None,
                 authdb='admin'):
        self.backup_name       = backup_name
        self.base_dir          = base_dir
        self.host              = host
        self.port              = port
        self.dump_gzip         = dump_gzip
        self.max_repl_lag_secs = max_repl_lag_secs
        self.user              = user
        self.password          = password
        self.authdb            = authdb

        self.response_queue = Queue()
        self.threads        = []
        self._summary       = {}

    def summary(self):
        return self._summary

    def run(self):
        replset_sharded = ReplsetHandlerSharded(self.host, self.port, self.user, self.password, self.authdb,
                                                self.max_repl_lag_secs)
        secondaries = replset_sharded.find_desirable_secondaries()
        for shard in secondaries:
            secondary = secondaries[shard]
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
