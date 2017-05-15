import os, sys
import logging
import signal

from fabric.api import hide, settings, local
from math import floor
from multiprocessing import cpu_count
from time import sleep

from mongodb_consistent_backup.Common import MongoUri
from mongodb_consistent_backup.Errors import Error, OperationError
from mongodb_consistent_backup.Oplog import OplogState
from mongodb_consistent_backup.Pipeline import Task

from MongodumpThread import MongodumpThread


class Mongodump(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, replsets, sharding=None):
        super(Mongodump, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
        self.compression_method = self.config.backup.mongodump.compression
        self.binary             = self.config.backup.mongodump.binary
        self.user               = self.config.username
        self.password           = self.config.password
        self.authdb             = self.config.authdb
        self.replsets           = replsets
        self.sharding           = sharding

        self.compression_supported = ['auto', 'none', 'gzip']
        self.version               = 'unknown'
        self.threads_max           = 16
        self.config_replset        = False
        self.dump_threads          = []
        self.states                = {}
        self._summary              = {}

        if self.config.backup.mongodump.threads and self.config.backup.mongodump.threads > 0:
            self.threads(self.config.backup.mongodump.threads)

        with hide('running', 'warnings'), settings(warn_only=True):
            self.version = local("%s --version|awk 'NR >1 {exit}; /version/{print $NF}'" % self.binary, capture=True)

        self.choose_compression()

    def choose_compression(self):
        if self.can_gzip():
            if self.compression() == 'auto':
                logging.info("Mongodump binary supports gzip compression, auto-enabling gzip compression")
                self.compression('gzip')
        elif self.compression() == 'gzip':
            raise OperationError("mongodump gzip compression requested on binary that does not support gzip!")

    def can_gzip(self):
        if os.path.isfile(self.binary) and os.access(self.binary, os.X_OK):
            logging.debug("Mongodump binary supports gzip")
            if tuple("3.2.0".split(".")) <= tuple(self.version.split(".")):
                return True
            return False
        else:
            logging.fatal("Cannot find or execute the mongodump binary file %s!" % self.binary)
            sys.exit(1)

    def summary(self):
        return self._summary

    # get oplog summaries from the queue
    def get_summaries(self):
        for shard in self.states:
            state = self.states[shard]
            host  = state.get('host')
            port  = state.get('port')
            self._summary[shard] = state.get().copy()

    def wait(self):
        completed = 0
        start_threads = len(self.dump_threads)
        # wait for all threads to finish
        while len(self.dump_threads) > 0:
            for thread in self.dump_threads:
                if not thread.is_alive():
                    if thread.exitcode == 0:
                        completed += 1
                    self.dump_threads.remove(thread)
            sleep(0.5)

        # sleep for 3 sec to fix logging order before gathering summaries
        sleep(3)
        self.get_summaries()

        # check if all threads completed
        if completed == start_threads:
            logging.info("All mongodump backups completed successfully")
            self.timer.stop(self.timer_name)
        else:
            raise OperationError("Not all mongodump threads completed successfully!")

    def threads(self, threads=None):
        if threads:
            self.thread_count = int(threads)
        elif not self.thread_count:
            if tuple(self.version.split(".")) >= tuple("3.2.0".split(".")):
                self.thread_count = 1
                if self.cpu_count > len(self.replsets):
                    self.thread_count = int(floor(self.cpu_count / len(self.replsets)))
                    if self.thread_count > self.threads_max:
                        self.thread_count = self.threads_max
            else:
                logging.warn("Threading unsupported by mongodump version %s. Use mongodump 3.2.0 or greater to enable per-dump threading." % self.version)
        return self.thread_count

    def run(self):
        self.timer.start(self.timer_name)

        # backup a secondary from each shard:
        for shard in self.replsets:
            secondary = self.replsets[shard].find_secondary()
            mongo_uri = secondary['uri']
            self.states[shard] = OplogState(self.manager, mongo_uri)
            thread = MongodumpThread(
                self.states[shard],
                mongo_uri,
                self.timer,
                self.user,
                self.password,
                self.authdb,
                self.backup_dir,
                self.binary,
                self.version,
                self.threads(),
                self.do_gzip(),
                self.verbose
            )
            self.dump_threads.append(thread)

        if not len(self.dump_threads) > 0:
            raise OperationError('No backup threads started!')

        logging.info(
            "Starting backups using mongodump %s (options: compression=%s, threads_per_dump=%i)" % (self.version, self.compression(), self.threads()))
        for thread in self.dump_threads:
            thread.start()
        self.wait()

        # backup a single sccc/non-replset config server, if exists:
        if self.sharding:
            config_server = self.sharding.get_config_server()
            if config_server and isinstance(config_server, dict):
                logging.info("Using non-replset backup method for config server mongodump")
                mongo_uri = MongoUri(config_server['host'], 27019, 'configsvr')
                self.states['configsvr'] = OplogState(self.manager, mongo_uri)
                self.dump_threads = [MongodumpThread(
                    self.states['configsvr'],
                    mongo_uri,
                    self.timer,
                    self.user,
                    self.password,
                    self.authdb,
                    self.backup_dir,
                    self.binary,
                    self.version,
                    self.threads(),
                    self.do_gzip(),
                    self.verbose
                )]
                self.dump_threads[0].start()
                self.dump_threads[0].join()

        self.completed = True
        return self._summary

    def close(self):
        if not self.stopped:
            logging.info("Stopping all mongodump threads")
            if len(self.dump_threads) > 0:
                for thread in self.dump_threads:
                    thread.terminate()
            try:
                self.timer.stop(self.timer_name)
            except:
                pass
            logging.info("Stopped all mongodump threads")
            self.stopped = True
