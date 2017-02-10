import os, sys
import logging

from fabric.api import hide, settings, local
from math import floor
from multiprocessing import Queue, cpu_count
from time import sleep


from MongodumpThread import MongodumpThread


class Mongodump:
    def __init__(self, config, base_dir, replsets, config_server=None):
        self.config        = config
        self.base_dir      = base_dir
        self.replsets      = replsets
        self.config_server = config_server
        self.binary        = self.config.backup.mongodump.binary
        self.user          = self.config.user
        self.password      = self.config.password
        self.authdb        = self.config.authdb
        self.verbose       = self.config.verbose

        self.config_replset = False
        self.cpu_count      = cpu_count()
        self.response_queue = Queue()
        self.threads        = []
        self._summary       = {}

        with hide('running', 'warnings'), settings(warn_only=True):
            self.version = local("%s --version|awk 'NR >1 {exit}; /version/{print $NF}'" % self.binary, capture=True)
        self.do_gzip = self.can_gzip()

        if not self.do_gzip and self.config.backup.mongodump.compression == 'gzip':
            logging.warning("mongodump gzip compression requested on binary that does not support gzip!")

        if not isinstance(self.config_server, dict) and self.config_server in self.replsets:
            self.config_replset = True

        if not isinstance(self.replsets, dict):
            raise Exception, "Field 'replsets' must be a dictionary of mongodb_consistent_backup.Replication.Replset classes!", None

    def can_gzip(self):
        if os.path.isfile(self.binary) and os.access(self.binary, os.X_OK):
            if tuple("3.2.0".split(".")) <= tuple(self.version.split(".")):
                return True
            return False
        else:
            logging.fatal("Cannot find or execute the mongodump binary file %s!" % self.binary)
            sys.exit(1)

    def is_compressed(self):
        return self.can_gzip()

    def summary(self):
        return self._summary

    def wait(self):
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

    def threads_per_dump(self, threads=None):
        if threads:
            self.threads_per_dump = int(threads)
        elif not self.threads_per_dump:
            if tuple(self.version.split(".")) >= tuple("3.2.0".split(".")):
                self.threads_per_dump = 1
                if self.cpu_count > len(self.replsets):
                    self.threads_per_dump = int(floor(self.cpu_count / len(self.replsets)))
                    if self.threads_per_dump > self.threads_per_dump_max:
                        self.threads_per_dump = self.threads_per_dump_max

    def run(self):
        # decide how many parallel dump workers to use based on cpu count vs # of shards (if 3.2+), 8 max workers max to protect the db
        self.threads_per_dump     = 0
        self.threads_per_dump_max = 8
        if tuple(self.version.split(".")) >= tuple("3.2.0".split(".")): 
            self.threads_per_dump = 1
            if self.cpu_count > len(self.replsets):
                self.threads_per_dump = int(floor(self.cpu_count / len(self.replsets)))
                if self.threads_per_dump > self.threads_per_dump_max:
                    self.threads_per_dump = self.threads_per_dump_max
        else:
            logging.warn("Threading unsupported by mongodump version %s. Use mongodump 3.2.0 or greater to enable per-dump threading." % self.version)

        # backup a secondary from each shard:
        for shard in self.replsets:
            secondary = self.replsets[shard].find_secondary()
            thread = MongodumpThread(
                self.response_queue,
                secondary['replSet'],
                secondary['host'],
                self.user,
                self.password,
                self.authdb,
                self.base_dir,
                self.binary,
                self.threads_per_dump,
                self.do_gzip,
                self.verbose
            )
            self.threads.append(thread)

        if not len(self.threads) > 0:
            raise Exception, 'No backup threads started!', None

        # start all threads and wait
        logging.info(
              "Starting backups using mongodump %s (options: gzip=%s,threads_per_dump=%i)" % (self.version, str(self.do_gzip), self.threads_per_dump))
        for thread in self.threads:
            thread.start()
        self.wait()

        # backup a single non-replset config server, if exists:
        if not self.config_replset and isinstance(self.config_server, dict):
            logging.info("Using non-replset backup method for config server mongodump")
            config_server = self.config_server['host']
            if not ":" in config_server:
                config_server = config_server+":27019"
            self.threads = [MongodumpThread(
                self.response_queue,
                'configsvr',
                config_server,
                self.user,
                self.password,
                self.authdb,
                self.base_dir,
                self.binary,
                self.threads_per_dump,
                self.do_gzip,
                self.verbose
            )]
            self.threads[0].start()
            self.wait()

        return self._summary

    def close(self):
        logging.info("Killing all mongodump threads...")
        if len(self.threads) > 0:
            for thread in self.threads:
                thread.terminate()
        logging.info("Killed all mongodump threads")
