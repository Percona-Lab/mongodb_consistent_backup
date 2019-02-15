import logging

from bson.timestamp import Timestamp
from multiprocessing import Event
from time import time, sleep

from TailThread import TailThread
from mongodb_consistent_backup.Common import MongoUri
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Oplog import OplogState
from mongodb_consistent_backup.Oplog.Common.OplogTask import OplogTask


class Tailer(OplogTask):
    def __init__(self, manager, config, timer, base_dir, backup_dir, replsets, backup_stop):
        super(Tailer, self).__init__(manager, config, timer, base_dir, backup_dir, replsets, backup_stop)

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
