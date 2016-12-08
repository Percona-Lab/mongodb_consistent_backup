import os
import logging

from gzip import GzipFile
# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson import decode_file_iter


class OplogFile:
    def __init__(self, oplog_file, dump_gzip=False):
        self.oplog_file = oplog_file
        self.dump_gzip  = dump_gzip

        self._count    = 0
        self._first_ts = None
        self._last_ts  = None

        self.read()

    def read(self):
        if os.path.isfile(self.oplog_file):
            try:
                logging.debug("Reading oplog file %s" % self.oplog_file)

                if self.dump_gzip:
                    oplog = GzipFile(self.oplog_file)
                else:
                    oplog = open(self.oplog_file)

                for change in decode_file_iter(oplog):
                    if 'ts' in change:
                        self._last_ts = change['ts']
                    if self._first_ts is None and self._last_ts is not None:
                        self._first_ts = self._last_ts
                    self._count += 1
                oplog.close()
            except Exception, e:
                logging.fatal("Error reading oplog file %s! Error: %s" % (self.oplog_file, e))
                raise e

    def count(self):
        return self._count

    def first_ts(self):
        return self._first_ts

    def last_ts(self):
        return self._last_ts
