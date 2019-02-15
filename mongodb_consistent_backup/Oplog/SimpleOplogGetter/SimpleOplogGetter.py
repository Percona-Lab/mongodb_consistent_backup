import logging

from multiprocessing import Event
from time import sleep

from SimpleOplogGetterThread import SimpleOplogGetterThread
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Oplog import OplogState
from mongodb_consistent_backup.Oplog.Common.OplogTask import OplogTask


class SimpleOplogGetter(OplogTask):
    def __init__(self, manager, config, timer, base_dir, backup_dir, replsets, backup_stop):
        super(SimpleOplogGetter, self).__init__(manager, config, timer, base_dir, backup_dir, replsets, backup_stop)
        self.worker_threads        = []
        self.backup_summary        = {}

    def run(self):
        if not self.enabled():
            logging.info("Oplog getter is disabled, skipping")
            return
        logging.info("Starting oplog getter for all replica sets (options: compression=%s, status_secs=%i)" % (self.compression(), self.status_secs))
        self.timer.start(self.timer_name)

        if len(self.backup_summary) == 0:
            raise OperationError("Oplogs cannot gathered without a successful backup first.")

        # Determine the time when the last shard completed its backup, because we need all changes
        # across all other shards since whenever they finished until then
        logging.debug("Finding latest finished backup timestamp")
        need_changes_until_ts = None
        for shard in self.replsets:
            ts = self.backup_summary[shard].get('last_ts')
            logging.debug("Shard %s's has changes up to %s" % (shard, ts))
            if need_changes_until_ts is None or ts > need_changes_until_ts:
                need_changes_until_ts = ts

        logging.info("Getting oplogs for all shards up to %s" % need_changes_until_ts)
        for shard in self.replsets:
            getter_stop   = Event()
            secondary   = self.replsets[shard].find_secondary()
            mongo_uri   = secondary['uri']
            shard_name  = mongo_uri.replset
            need_changes_since_ts = self.backup_summary[shard].get('last_ts')
            oplog_file  = self.prepare_oplog_files(shard_name)
            oplog_state = OplogState(self.manager, mongo_uri, oplog_file)
            thread = SimpleOplogGetterThread(
                self.backup_stop,
                getter_stop,
                mongo_uri,
                self.config,
                self.timer,
                oplog_file,
                oplog_state,
                self.do_gzip(),
                need_changes_since_ts,
                need_changes_until_ts
            )
            self.shards[shard] = {
                'stop':   getter_stop,
                'thread': thread,
                'state':  oplog_state
            }
            self.worker_threads.append(thread)
            logging.debug("Starting thread %s to write %s oplog to %s" % (thread.name, mongo_uri, oplog_file))
            thread.start()
        # Wait for all threads to complete
        self.wait()

        # Wait would have thrown an error is not all of them completed
        # normally.
        self.completed = True
        self.stopped   = True
        self.get_summaries()

        return self._summary

    def wait(self):
        completed = 0
        start_threads = len(self.worker_threads)
        # wait for all threads to finish
        logging.debug("Waiting for %d oplog threads to finish" % start_threads)
        while len(self.worker_threads) > 0:
            if self.backup_stop and self.backup_stop.is_set():
                logging.error("Received backup stop event due to error(s), stopping backup!")
                raise OperationError("Received backup stop event due to error(s)")
            for thread in self.worker_threads:
                if not thread.is_alive():
                    logging.debug("Thread %s exited with code %d" % (thread, thread.exitcode))
                    if thread.exitcode == 0:
                        completed += 1
                    self.worker_threads.remove(thread)
                else:
                    logging.debug("Waiting for %s to finish" % thread.name)
            sleep(1)

        # check if all threads completed
        if completed == start_threads:
            logging.info("All oplog threads completed successfully")
            self.timer.stop(self.timer_name)
        else:
            raise OperationError("%d oplog getter threads failed to complete successfully!" % (start_threads - completed))

    def stop(self, kill=False, sleep_secs=3):
        if not self.enabled():
            return
        logging.info("Stopping all oplog tailers")
        for shard in self.shards:
            state   = self.shards[shard]['state']
            thread  = self.shards[shard]['thread']

            # set thread stop event
            self.shards[shard]['stop'].set()
            if kill:
                thread.terminate()
            sleep(1)

            # wait for thread to stop
            while thread.is_alive():
                logging.info('Waiting for %s getter stop' % thread.name)
                sleep(sleep_secs)

            # gather state info
            self._summary[shard] = state.get().copy()

        self.timer.stop(self.timer_name)
        logging.info("Oplog getter completed in %.2f seconds" % self.timer.duration(self.timer_name))
        return self._summary
