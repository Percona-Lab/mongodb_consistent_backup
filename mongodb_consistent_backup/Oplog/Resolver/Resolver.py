import os
import logging

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson.timestamp import Timestamp
from copy_reg import pickle
from multiprocessing import Pool, cpu_count
from types import MethodType

from ResolverThread import ResolverThread
from mongodb_consistent_backup.Common import Timer, parse_method


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

pickle(MethodType, _reduce_method)


class Resolver:
    def __init__(self, config, tailed_oplogs_summary, backup_oplogs_summary):
        self.config        = config
        self.tailed_oplogs = tailed_oplogs_summary
        self.backup_oplogs = backup_oplogs_summary

        self.timer  = Timer()
        self.end_ts = None
        self.delete_oplogs = {}

        try:
            self._pool = Pool(processes=self.threads())
        except Exception, e:
            logging.fatal("Could not start oplog resolver pool! Error: %s" % e)
            raise e

    def compression(self, method=None):
        if method:
            logging.debug("Setting oplog resolver compression to: %s" % method)
            self.config.oplog.compression = parse_method(method)
        return parse_method(self.config.oplog.compression)

    def do_gzip(self):
        if self.compression() == 'gzip':
           return True
        return False

    def threads(self, threads=None):
        if threads:
            self.config.oplog.resolver.threads = int(threads)
        if self.config.oplog.resolver.threads is None or self.config.oplog.resolver.threads < 1:
            self.config.oplog.resolver.threads = int(cpu_count() * 2)
        return int(self.config.oplog.resolver.threads)

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
        logging.info("Resolving oplogs (options: threads=%s,compression=%s)" % (self.threads(), self.compression()))
        self.timer.start()

        self.end_ts = self.get_consistent_end_ts()
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
                            self._pool.apply_async(ResolverThread(
                                host,
                                port,
                                tailed_oplog['file'],
                                backup_oplog['file'],
                                backup_oplog['last_ts'],
                                self.end_ts,
                                self.do_gzip()
                            ).run)
                        except Exception, e:
                            logging.fatal("Resolve failed for %s:%s! Error: %s" % (host, port, e))
                            raise e
                else:
                    logging.info("No tailed oplog for host %s:%s" % (host, port))
        self._pool.close()
        self._pool.join()

        for oplog_file in self.delete_oplogs:
            try:
                logging.debug("Deleting tailed oplog file for %s:%i" % (
                    self.delete_oplogs[oplog_file]['host'],
                    self.delete_oplogs[oplog_file]['port']
                ))
                os.remove(oplog_file)
            except Exception, e:
                logging.fatal("Deleting of tailed oplog file %s failed! Error: %s" % (oplog_file, e))
                raise e

        self.timer.stop()
        logging.info("Oplog resolving completed in %s seconds" % self.timer.duration())
