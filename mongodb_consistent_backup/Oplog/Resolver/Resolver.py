import logging

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson.timestamp import Timestamp
from copy_reg import pickle
from multiprocessing import Pool, TimeoutError
from types import MethodType

from ResolverThread import ResolverThread
from mongodb_consistent_backup.Common import MongoUri
from mongodb_consistent_backup.Errors import Error, OperationError
from mongodb_consistent_backup.Oplog import OplogState
from mongodb_consistent_backup.Pipeline import Task


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


pickle(MethodType, _reduce_method)


class Resolver(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, tailed_oplogs, backup_oplogs):
        super(Resolver, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
        self.tailed_oplogs = tailed_oplogs
        self.backup_oplogs = backup_oplogs

        self.compression_supported = ['none', 'gzip']
        self.resolver_summary      = {}
        self.resolver_state        = {}

        self.running   = False
        self.stopped   = False
        self.completed = False
        self._pool     = None
        self._pooled   = []
        self._results  = {}

        self.threads(self.config.oplog.resolver.threads)

        try:
            self._pool = Pool(processes=self.threads())
        except Exception, e:
            logging.fatal("Could not start oplog resolver pool! Error: %s" % e)
            raise Error(e)

    def close(self):
        if self._pool and self.stopped:
            logging.debug("Stopping all oplog resolver threads")
            self._pool.terminate()
            logging.info("Stopped all oplog resolver threads")
            self.stopped = True

    def get_backup_end_max_ts(self):
        end_ts = None
        for shard in self.backup_oplogs:
            instance = self.backup_oplogs[shard]
            if 'last_ts' in instance and instance['last_ts'] is not None:
                last_ts = instance['last_ts']
                if end_ts is None or last_ts > end_ts:
                    end_ts = last_ts
        return end_ts

    def get_consistent_end_ts(self):
        end_ts     = None
        bkp_end_ts = self.get_backup_end_max_ts()
        for shard in self.tailed_oplogs:
            instance = self.tailed_oplogs[shard]
            if 'last_ts' in instance and instance['last_ts'] is not None:
                last_ts = instance['last_ts']
                if end_ts is None or last_ts < end_ts:
                    end_ts = last_ts
                    if last_ts < bkp_end_ts:
                        end_ts = bkp_end_ts
        return Timestamp(end_ts.time + 1, 0)

    def done(self, done_uri):
        if done_uri in self._pooled:
            logging.debug("Resolving completed for: %s" % done_uri)
            self._pooled.remove(done_uri)
        else:
            raise OperationError("Unexpected response from resolver thread: %s" % done_uri)

    def wait(self, max_wait_secs=6 * 3600, poll_secs=2):
        if len(self._pooled) > 0:
            waited_secs = 0
            self._pool.close()
            while len(self._pooled):
                logging.debug("Waiting for %i oplog resolver thread(s) to stop" % len(self._pooled))
                try:
                    for thread_name in self._pooled:
                        thread = self._results[thread_name]
                        thread.get(poll_secs)
                except TimeoutError:
                    if waited_secs < max_wait_secs:
                        waited_secs += poll_secs
                    else:
                        raise OperationError("Waited more than %i seconds for Oplog resolver! I will assume there is a problem and exit")

    def run(self):
        try:
            logging.info("Resolving oplogs (options: threads=%s, compression=%s)" % (self.threads(), self.compression()))
            self.timer.start(self.timer_name)
            self.running = True

            for shard in self.backup_oplogs:
                backup_oplog = self.backup_oplogs[shard]
                self.resolver_state[shard] = OplogState(self.manager, None, backup_oplog['file'])
                uri = MongoUri(backup_oplog['uri']).get()
                if shard in self.tailed_oplogs:
                    tailed_oplog = self.tailed_oplogs[shard]
                    if backup_oplog['last_ts'] is None and tailed_oplog['last_ts'] is None:
                        logging.info("No oplog changes to resolve for %s" % uri)
                    elif backup_oplog['last_ts'] > tailed_oplog['last_ts']:
                        logging.fatal(
                            "Backup oplog is newer than the tailed oplog! This situation is unsupported. Please retry backup")
                        raise OperationError("Backup oplog is newer than the tailed oplog!")
                    else:
                        thread_name = uri.str()
                        logging.debug("Starting ResolverThread: %s" % thread_name)
                        self._results[thread_name] = self._pool.apply_async(ResolverThread(
                            self.config.dump(),
                            self.resolver_state[shard],
                            uri,
                            tailed_oplog.copy(),
                            backup_oplog.copy(),
                            self.get_consistent_end_ts(),
                            self.compression()
                        ).run, callback=self.done)
                        self._pooled.append(thread_name)
                else:
                    logging.info("No tailed oplog for host %s" % uri)
            self.wait()
            self.completed = True
            logging.info("Oplog resolving completed in %.2f seconds" % self.timer.duration(self.timer_name))
        except Exception, e:
            logging.error("Resolver failed for %s: %s" % (uri, e))
            raise e
        finally:
            self.timer.stop(self.timer_name)
            self.running = False
            self.stopped = True

        for shard in self.resolver_state:
            self.resolver_summary[shard] = self.resolver_state[shard].get()
        return self.resolver_summary
