import os
import logging

from gzip import GzipFile
from bson import BSON, decode_file_iter
from bson.codec_options import CodecOptions
from time import time

from mongodb_consistent_backup.Errors import OperationError


class Oplog:
    def __init__(self, oplog_file, do_gzip=False, file_mode="r", flush_docs=100, flush_secs=1):
        self.oplog_file = oplog_file
        self.do_gzip    = do_gzip
        self.file_mode  = file_mode
        self.flush_docs = flush_docs
        self.flush_secs = flush_secs

        self._count    = 0
        self._first_ts = None
        self._last_ts  = None
        self._oplog    = None

        self._last_flush_time  = time()
        self._writes_unflushed = 0

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

    def add(self, doc, autoflush=True):
        try:
            self._oplog.write(BSON.encode(doc))
            self._writes_unflushed += 1
            self._count            += 1
            if not self._first_ts:
                self._first_ts = doc['ts']
            self._last_ts = doc['ts']
            if autoflush:
                self.autoflush()
        except Exception, e:
            logging.fatal("Cannot write to oplog file %s! Error: %s" % (self.oplog_file, e))
            raise OperationError(e)

    def secs_since_flush(self):
        return time() - self._last_flush_time

    def do_flush(self):
        if self._writes_unflushed > self.flush_docs:
            return True
        elif self.secs_since_flush() > self.flush_secs:
            return True
        return False

    def flush(self):
        if self._oplog:
            return self._oplog.flush()

    def fsync(self):
        if self._oplog:
            # https://docs.python.org/2/library/os.html#os.fsync
            self._oplog.flush()
            self._last_flush_time  = time()
            self._writes_unflushed = 0
            return os.fsync(self._oplog.fileno())

    def autoflush(self):
        if self._oplog and self.do_flush():
            logging.debug("Fsyncing %s (secs_since=%.2f, changes=%i, ts=%s)" % (
                self.oplog_file,
                self.secs_since_flush(),
                self._writes_unflushed,
                self.last_ts())
            )
            return self.fsync()

    def close(self):
        if self._oplog:
            self.fsync()
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
