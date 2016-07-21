import logging

from fabric.api import hide, settings, local
from multiprocessing import Process, Queue
from time import sleep

from MongoBackup.Methods import Dump


class Dumper:
    def __init__(self, secondaries, base_dir, binary, dump_gzip=False, user=None, password=None,
                 authdb='admin', config_server=None, verbose=False):
        self.secondaries   = secondaries
        self.base_dir      = base_dir
        self.binary        = binary
        self.dump_gzip     = dump_gzip
        self.user          = user
        self.password      = password
        self.authdb        = authdb
        self.config_server = config_server
        self.verbose       = verbose

        self.response_queue = Queue()
        self.threads        = []
        self._summary       = {}

        if not isinstance(self.secondaries, dict):
            raise Exception, "Field 'secondaries' must be a dictionary of secondary info (by shard)!", None

        with hide('running', 'warnings'), settings(warn_only=True):
            self.version = local("%s --version|awk 'NR >1 {exit}; /version/{print $NF}'" % self.binary, capture=True)

    def summary(self):
        return self._summary

    def run(self):
        # backup a secondary from each shard:
        for shard in self.secondaries:
            secondary = self.secondaries[shard]
            thread = Dump(
                self.response_queue,
                secondary['replSet'],
                secondary['host'],
                self.user,
                self.password,
                self.authdb,
                self.base_dir,
                self.binary,
                self.dump_gzip,
                self.verbose
            )
            self.threads.append(thread)

        # backup a single config server:
        if self.config_server:
            thread = Dump(
                self.response_queue,
                'config',
                self.config_server,
                self.user,
                self.password,
                self.authdb,
                self.base_dir,
                self.binary,
                self.dump_gzip,
                self.verbose
            )
            self.threads.append(thread)

        if not len(self.threads) > 0:
            raise Exception, 'No backup threads started!', None

        # start all threads
        logging.info(
            "Starting backups in threads using mongodump %s (inline gzip: %s)" % (self.version, str(self.dump_gzip)))
        for thread in self.threads:
            thread.start()

        # wait for all threads to finish
        for thread in self.threads:
            thread.join()

        # sleep for 3 sec to fix logging order
        sleep(3)

        # get oplog summaries from the queue
        completed = 0
        while not self.response_queue.empty():
            backup = self.response_queue.get()
            host = backup['host']
            port = backup['port']
            if host not in self._summary:
                self._summary[host] = {}
            self._summary[host][port] = backup
            if backup['completed']:
                completed += 1

        # check if all threads completed
        if completed == len(self.threads):
            logging.info("All mongodump backups completed")
        else:
            raise Exception, "Not all mongodump threads completed successfully!", None

        return self._summary

    def close(self):
        logging.info("Killing all mongodump threads...")
        if len(self.threads) > 0:
            for thread in self.threads:
                thread.terminate()
        logging.info("Killed all mongodump threads")
