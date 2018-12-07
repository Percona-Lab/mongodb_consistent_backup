import os
import logging

from math import floor
from subprocess import check_output
from time import sleep

from mongodb_consistent_backup.Common import config_to_string
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Oplog import OplogState
from mongodb_consistent_backup.Pipeline import Task

from MongodumpThread import MongodumpThread


class Mongodump(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, replsets, backup_stop=None, sharding=None):
        super(Mongodump, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
        self.user               = self.config.username
        self.password           = self.config.password
        self.authdb             = self.config.authdb
        self.compression_method = self.config.backup.mongodump.compression
        self.binary             = self.config.backup.mongodump.binary
        self.replsets           = replsets
        self.backup_stop        = backup_stop
        self.sharding           = sharding

        self.compression_supported = ['auto', 'none', 'gzip']
        self.version               = 'unknown'
        self.version_extra         = {}
        self.threads_max           = 16
        self.config_replset        = False
        self.dump_threads          = []
        self.states                = {}
        self._summary              = {}

        self.parse_mongodump_version()
        self.choose_compression()

        if self.config.backup.mongodump.threads and self.config.backup.mongodump.threads > 0:
            self.threads(self.config.backup.mongodump.threads)

    def parse_mongodump_version(self):
        if os.path.isfile(self.binary):
            output = check_output([self.binary, "--version"])
            lines  = output.rstrip().split("\n")
            for line in lines:
                if "version:" in line:
                    name, version_num = line.split(" version: ")
                    if name == 'mongodump':
                        self.version = version_num
                        if '-' in version_num:
                            self.version = version_num.split("-")[0]
                    self.version_extra[name.lower()] = version_num
            return self.version, self.version_extra
        raise OperationError("Could not parse mongodump --version output!")

    def choose_compression(self):
        if self.can_compress():
            if self.compression() == 'auto':
                logging.info("Mongodump binary supports gzip compression, auto-enabling gzip compression")
                self.compression('gzip')
        elif self.compression() == 'gzip':
            raise OperationError("mongodump gzip compression requested on binary that does not support gzip!")

    def can_compress(self):
        if os.path.isfile(self.binary) and os.access(self.binary, os.X_OK):
            logging.debug("Mongodump binary supports gzip compression")
            if tuple("3.2.0".split(".")) <= tuple(self.version.split(".")):
                return True
            return False
        else:
            raise OperationError("Cannot find or execute the mongodump binary file %s!" % self.binary)

    def summary(self):
        return self._summary

    # get oplog summaries from the queue
    def get_summaries(self):
        for shard in self.states:
            state = self.states[shard]
            self._summary[shard] = state.get().copy()

    def wait(self):
        completed = 0
        start_threads = len(self.dump_threads)
        # wait for all threads to finish
        while len(self.dump_threads) > 0:
            if self.backup_stop and self.backup_stop.is_set():
                logging.error("Received backup stop event due to error(s), stopping backup!")
                raise OperationError("Received backup stop event due to error(s)")
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
        elif not self.thread_count and self.version is not 'unknown':
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
            try:
                secondary = self.replsets[shard].find_secondary()
                mongo_uri = secondary['uri']
                self.states[shard] = OplogState(self.manager, mongo_uri)
                thread = MongodumpThread(
                    self.states[shard],
                    mongo_uri,
                    self.timer,
                    self.config,
                    self.backup_dir,
                    self.version,
                    self.threads(),
                    self.do_gzip()
                )
                self.dump_threads.append(thread)
            except Exception, e:
                logging.error("Failed to get secondary for shard %s: %s" % (shard, e))
                raise e

        if not len(self.dump_threads) > 0:
            raise OperationError('No backup threads started!')

        options = {
            'compression':      self.compression(),
            'threads_per_dump': self.threads()
        }
        options.update(self.version_extra)
        logging.info(
            "Starting backups using mongodump %s (options: %s)" % (self.version, config_to_string(options)),
        )

        for thread in self.dump_threads:
            thread.start()
        self.wait()

        self.completed = True
        self.stopped   = True
        return self._summary

    def close(self):
        if not self.stopped:
            logging.info("Stopping all mongodump threads")
            if len(self.dump_threads) > 0:
                for thread in self.dump_threads:
                    thread.terminate()
            try:
                self.timer.stop(self.timer_name)
            except Exception:
                pass
            logging.info("Stopped all mongodump threads")
            self.stopped = True
