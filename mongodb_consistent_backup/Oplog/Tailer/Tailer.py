import os
import logging

from bson.timestamp import Timestamp
from multiprocessing import Event
from time import time, sleep

from TailThread import TailThread
from mongodb_consistent_backup.Common import MongoUri
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Oplog import OplogState
from mongodb_consistent_backup.Pipeline import Task


class Tailer(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, replsets, backup_stop):
        super(Tailer, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
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

    def prepare_oplog_files(self, shard_name):
        oplog_dir = os.path.join(self.backup_dir, shard_name)
        if not os.path.isdir(oplog_dir):
            os.mkdir(oplog_dir)
        oplog_file = os.path.join(oplog_dir, "oplog-tailed.bson")
        return oplog_file

    def run(self):
        if not self.enabled():
            logging.info("Oplog tailer is disabled, skipping")
            return
        logging.info("Starting oplog tailers on all replica sets (options: compression=%s, status_secs=%i)" % (self.compression(), self.status_secs))
        self.timer.start(self.timer_name)
        for shard in self.replsets:
            tail_stop   = Event()
            secondary   = self.replsets[shard].find_secondary()
            mongo_uri   = secondary['uri']
            shard_name  = mongo_uri.replset

            oplog_file  = self.prepare_oplog_files(shard_name)
            oplog_state = OplogState(self.manager, mongo_uri, oplog_file)
            thread = TailThread(
                self.backup_stop,
                tail_stop,
                mongo_uri,
                self.config,
                self.timer,
                oplog_file,
                oplog_state,
                self.do_gzip()
            )
            self.shards[shard] = {
                'stop':   tail_stop,
                'thread': thread,
                'state':  oplog_state
            }
            self.shards[shard]['thread'].start()
            while not oplog_state.get('running'):
                if self.shards[shard]['thread'].exitcode:
                    raise OperationError("Oplog tailer for %s failed with exit code %i!" % (mongo_uri, self.shards[shard]['thread'].exitcode))
                sleep(0.5)

    def stop(self, kill=False, sleep_secs=3):
        if not self.enabled():
            return
        logging.info("Stopping all oplog tailers")
        for shard in self.shards:
            replset = self.replsets[shard]
            state   = self.shards[shard]['state']
            thread  = self.shards[shard]['thread']

            try:
                uri = MongoUri(state.get('uri'))
            except Exception, e:
                raise OperationError(e)

            if not kill:
                # get current optime of replset primary to use a stop position
                try:
                    timestamp = replset.primary_optime(True, True)
                except Exception:
                    logging.warning("Could not get current optime from PRIMARY! Using now as a stop time")
                    timestamp = Timestamp(int(time()), 0)

                # wait for replication to get in sync
                while state.get('last_ts') and state.get('last_ts') < timestamp:
                    logging.info('Waiting for %s tailer to reach ts: %s, currrent: %s' % (uri, timestamp, state.get('last_ts')))
                    sleep(sleep_secs)

            # set thread stop event
            self.shards[shard]['stop'].set()
            if kill:
                thread.terminate()
            sleep(1)

            # wait for thread to stop
            while thread.is_alive():
                logging.info('Waiting for tailer %s to stop' % uri)
                sleep(sleep_secs)

            # gather state info
            self._summary[shard] = state.get().copy()

        self.timer.stop(self.timer_name)
        logging.info("Oplog tailing completed in %.2f seconds" % self.timer.duration(self.timer_name))

        return self._summary

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
                logging.error("Cannot stop tailer thread: %s" % e)
