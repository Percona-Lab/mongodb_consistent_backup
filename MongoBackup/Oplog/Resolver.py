import os
import logging

from bson.timestamp import Timestamp
from copy_reg import pickle
from multiprocessing import Pool, cpu_count
from types import MethodType

from MongoBackup.Oplog import OplogResolve


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

pickle(MethodType, _reduce_method)


class OplogResolver:
    def __init__(self, tailed_oplogs_summary, backup_oplogs_summary, dump_gzip=False, thread_count=None):
        self.tailed_oplogs = tailed_oplogs_summary
        self.backup_oplogs = backup_oplogs_summary
        self.dump_gzip     = dump_gzip
        self.thread_count  = thread_count

        self.end_ts = None
        self.delete_oplogs = {}

        if self.thread_count is None:
            self.thread_count = cpu_count() * 2

        try:
            self._pool = Pool(processes=self.thread_count)
        except Exception, e:
            logging.fatal("Could not start pool! Error: %s" % e)
            raise e

    def get_consistent_end_ts(self):
        ts = None
        for host in self.tailed_oplogs:
            for port in self.tailed_oplogs[host]:
                instance = self.tailed_oplogs[host][port]
                if 'last_ts' in instance and instance['last_ts'] is not None:
                    if ts is None or instance['last_ts'].time < ts.time:
                        ts = Timestamp(instance['last_ts'].time, 0)
        return ts

    def run(self):
        logging.info("Resolving oplogs using %i threads max" % self.thread_count)

        self.end_ts   = self.get_consistent_end_ts()
        for host in self.backup_oplogs:
            for port in self.backup_oplogs[host]:
                backup_oplog = self.backup_oplogs[host][port]
                if host in self.tailed_oplogs and port in self.tailed_oplogs[host]:
                    tailed_oplog = self.tailed_oplogs[host][port]
                    tailed_oplog_file = tailed_oplog['file']
                    self.delete_oplogs[tailed_oplog_file] = {
                        'host': host,
                        'port': port
                    }

                    if backup_oplog['last_ts'] is None and tailed_oplog['last_ts'] is None:
                        logging.info("No oplog changes to resolve for %s:%s" % (host, port))
                    elif backup_oplog['last_ts'] > tailed_oplog['last_ts']:
                        logging.fatal(
                            "Backup oplog is newer than the tailed oplog! This situation is unsupported. Please retry backup")
                        raise Exception, "Backup oplog is newer than the tailed oplog!", None
                    else:
                        try:
                            self._pool.apply_async(OplogResolve(
                                host,
                                port,
                                tailed_oplog['file'],
                                backup_oplog['file'],
                                backup_oplog['last_ts'],
                                self.end_ts,
                                self.dump_gzip
                            ).run)
                        except Exception, e:
                            logging.fatal("Resolve failed for %s:%s! Error: %s" % (host, port, e))
                            raise e
                else:
                    logging.info("No tailed oplog for host %s:%s" % (host, port))
        self._pool.close()
        self._pool.join()

        for delete_oplog in self.delete_oplogs:
            try:
                logging.debug("Deleting tailed oplog file for %s:%i" % (
                    self.delete_oplogs[delete_oplog]['host'],
                    self.delete_oplogs[delete_oplog]['port']
                ))
                os.remove(delete_oplog)
            except Exception, e:
                logging.fatal("Deleting of tailed oplog file %s failed! Error: %s" % (oplog_file, e))
                raise e

        logging.info("Done resolving oplogs")
