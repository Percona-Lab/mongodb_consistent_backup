import os
import logging

from gzip import GzipFile
from bson import BSON, decode_file_iter
from bson.codec_options import CodecOptions

from mongodb_consistent_backup.Errors import OperationError


class Oplog:
    def __init__(self, oplog_file, do_gzip=False, file_mode="r"):
        self.oplog_file = oplog_file
        self.do_gzip    = do_gzip
        self.file_mode  = file_mode

        self._count    = 0
        self._first_ts = None
        self._last_ts  = None
        self._oplog    = None

        self.open()

    def handle(self):
        return self._oplog

    def open(self):
        if not self._oplog:
            try:
                logging.debug("Opening oplog file %s" % self.oplog_file)
                if self.do_gzip:
                    self._oplog  = GzipFile(self.oplog_file, self.file_mode)
                else:
                    self._oplog = open(self.oplog_file, self.file_mode)
            except Exception, e:
                logging.fatal("Error opening oplog file %s! Error: %s" % (self.oplog_file, e))
                raise OperationError(e)
        return self._oplog

    def read(self, b):
        if self._oplog:
            return self._oplog.read(b)

    def load(self):
        try:
            oplog = self.open()
            logging.debug("Reading oplog file %s" % self.oplog_file)
            for change in decode_file_iter(oplog, CodecOptions(unicode_decode_error_handler="ignore")):
                if 'ts' in change:
                    self._last_ts = change['ts']
                if self._first_ts is None and self._last_ts is not None:
                    self._first_ts = self._last_ts
                self._count += 1
            oplog.close()
        except Exception, e:
            logging.fatal("Error reading oplog file %s! Error: %s" % (self.oplog_file, e))
            raise OperationError(e)

    def add(self, doc):
        try:
            self._oplog.write(BSON.encode(doc))
            self._count += 1
            if not self._first_ts:
                self._first_ts = doc['ts']
            self._last_ts = doc['ts']
        except Exception, e:
            logging.fatal("Cannot write to oplog file %s! Error: %s" % (self.oplog_file, e))
            raise OperationError(e)

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

    def stat(self):
       return {
           'file':     self.oplog_file,
           'count':    self.count(),
           'first_ts': self.first_ts(),
           'last_ts':  self.last_ts()
       }
