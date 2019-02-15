import logging
import os
from time import sleep

from mongodb_consistent_backup.Pipeline import Task


class OplogTask(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, replsets, backup_stop):
        super(OplogTask, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)

        self.backup_name = self.config.name
        self.user        = self.config.username
        self.password    = self.config.password
        self.authdb      = self.config.authdb
        self.status_secs = self.config.oplog.tailer.status_interval
        self.replsets    = replsets
        self.backup_stop = backup_stop
        self._enabled    = self.config.oplog.tailer.enabled

        self.compression_supported = ['none', 'gzip']
        self.shards                = {}
        self._summary              = {}

    def enabled(self):
        if isinstance(self._enabled, bool):
            return self._enabled
        elif isinstance(self._enabled, str) and self._enabled.strip().lower() != 'false':
            return True
        return False

    def summary(self):
        return self._summary

    def get_summaries(self):
        for shard in self.shards:
            state = self.shards[shard].get('state')
            self._summary[shard] = state.get().copy()

    def prepare_oplog_files(self, shard_name):
        oplog_dir = os.path.join(self.backup_dir, shard_name)
        if not os.path.isdir(oplog_dir):
            os.mkdir(oplog_dir)
        oplog_file = os.path.join(oplog_dir, "oplog-tailed.bson")
        return oplog_file

    def close(self):
        if not self.enabled():
            return
        for shard in self.shards:
            try:
                self.shards[shard]['stop'].set()
                thread = self.shards[shard]['thread']
                thread.terminate()
                while thread.is_alive():
                    sleep(0.5)
            except Exception, e:
                logging.error("Cannot stop oplog task thread: %s" % e)
