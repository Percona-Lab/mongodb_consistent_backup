import os
import logging

from gzip import GzipFile
from bson import BSON, decode_file_iter


class Oplog:
    def __init__(self, oplog_file, do_gzip=False):
        self.oplog_file = oplog_file
        self.do_gzip    = do_gzip

        self._count    = 0
        self._first_ts = None
        self._last_ts  = None
        self._oplog    = None

        self.open()

    def open(self):
        if not self._oplog and os.path.isfile(self.oplog_file):
            try:
                logging.debug("Opening oplog file %s" % self.oplog_file)
                if self.do_gzip:
                    self._oplog = GzipFile(self.oplog_file)
                else:
                    self._oplog = open(self.oplog_file)
            except Exception, e:
                logging.fatal("Error opening oplog file %s! Error: %s" % (self.oplog_file, e))
                raise e
        return self._oplog

    def read(self):
        try:
            oplog = self.open()
            logging.debug("Reading oplog file %s" % self.oplog_file)
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

    def write(self, doc):
        if self._oplog:
            try:
                  self._oplog.write(BSON.encode(doc))
                self._count += 1
                if not self._first_ts:
                    self._first_ts = doc['ts']
                self._last_ts = doc['ts']
            except Exception, e:
                logging.fatal("Cannot write to oplog file %s! Error: %s" % (self.oplog_file, e))
                raise e

    def flush(self):
        if self._oplog:
            return self._oplog.flush()

    def close(self):
        if self._oplog:
            return self._oplog.close()

    def count(self):
        return self._count

    def first_ts(self):
        return self._first_ts

    def last_ts(self):
        return self._last_ts
