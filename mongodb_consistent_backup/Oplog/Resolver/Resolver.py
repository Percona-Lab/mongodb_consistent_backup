import logging

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson.timestamp import Timestamp
from copy_reg import pickle
from multiprocessing import Pool, TimeoutError
from time import sleep
from types import MethodType

from ResolverThread import ResolverThread
from mongodb_consistent_backup.Common import MongoUri, parse_method
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
        try:
            self._pool = Pool(processes=self.threads(None, 2))
        except Exception, e:
            logging.fatal("Could not start oplog resolver pool! Error: %s" % e)
            raise Error(e)

    def close(self, code=None, frame=None):
       if self._pool and not self.stopped:
           logging.debug("Stopping all oplog resolver threads")
           self._pool.terminate()
           logging.info("Stopped all oplog resolver threads")
           self.stopped = True

    def get_consistent_end_ts(self):
        ts = None
        for shard in self.tailed_oplogs:
            instance = self.tailed_oplogs[shard]
            if 'last_ts' in instance and instance['last_ts'] is not None:
                if ts is None or instance['last_ts'].time < ts.time:
                    ts = Timestamp(instance['last_ts'].time, 0)
        return ts

    def done(self, done_uri):
        if done_uri in self._pooled:
            logging.debug("Resolving completed for: %s" % done_uri)
            self._pooled.remove(done_uri)
        else:
            raise OperationError("Unexpected response from resolver thread: %s" % done_uri)

    def wait(self, max_wait_secs=6*3600, poll_secs=2):
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
            self._pool.terminate()
            logging.debug("Stopped all oplog resolver threads")
            self.stopped = True
            self.running = False

    def run(self):
        logging.info("Resolving oplogs (options: threads=%s, compression=%s)" % (self.threads(), self.compression()))
        self.timer.start(self.timer_name)
        self.running = True

        for shard in self.backup_oplogs:
            backup_oplog = self.backup_oplogs[shard]
            self.resolver_state[shard] = OplogState(self.manager, None, backup_oplog['file'])
            uri = MongoUri(backup_oplog['uri']).get()
            if shard in self.tailed_oplogs:
                tailed_oplog = self.tailed_oplogs[shard]
                tailed_oplog_file = tailed_oplog['file']
                if backup_oplog['last_ts'] is None and tailed_oplog['last_ts'] is None:
                    logging.info("No oplog changes to resolve for %s" % uri)
                elif backup_oplog['last_ts'] > tailed_oplog['last_ts']:
                    logging.fatal(
                        "Backup oplog is newer than the tailed oplog! This situation is unsupported. Please retry backup")
                    raise OperationError("Backup oplog is newer than the tailed oplog!")
                else:
                    try:
                        thread_name = uri.str()
                        self._results[thread_name] = self._pool.apply_async(ResolverThread(
                            self.resolver_state[shard],
                            uri,
                            tailed_oplog.copy(),
                            backup_oplog.copy(),
                            self.get_consistent_end_ts(),
                            self.compression()
                        ).run, callback=self.done)
                        self._pooled.append(thread_name)
                    except Exception, e:
                        logging.fatal("Resolve failed for %s! Error: %s" % (uri, e))
                        raise Error(e)
            else:
                logging.info("No tailed oplog for host %s" % uri)
        self.wait()
        self.running   = False
        self.stopped   = True
        self.completed = True

        self.timer.stop(self.timer_name)
        logging.info("Oplog resolving completed in %.2f seconds" % self.timer.duration(self.timer_name))

        for shard in self.resolver_state:
            self.resolver_summary[shard] = self.resolver_state[shard].get()
        return self.resolver_summary
