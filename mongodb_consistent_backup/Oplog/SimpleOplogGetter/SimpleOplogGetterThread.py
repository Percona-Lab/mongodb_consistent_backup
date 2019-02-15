import logging
import sys

# noinspection PyPackageRequirements
from multiprocessing import Process
from pymongo.errors import AutoReconnect, ConnectionFailure, CursorNotFound, ExceededMaxWaiters
from pymongo.errors import ExecutionTimeout, NetworkTimeout, NotMasterError, ServerSelectionTimeoutError
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from time import time

from mongodb_consistent_backup.Common import DB
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Oplog import Oplog


class SimpleOplogGetterThread(Process):
    def __init__(self, backup_stop, tail_stop, uri, config, timer, oplog_file, state, dump_gzip=False, tail_from_ts=None, tail_to_ts=None):
        Process.__init__(self)
        self.backup_stop = backup_stop
        self.tail_stop   = tail_stop
        self.uri         = uri
        self.config      = config
        self.timer       = timer
        self.oplog_file  = oplog_file
        self.state       = state
        self.dump_gzip   = dump_gzip
        self.flush_docs  = self.config.oplog.flush.max_docs
        self.flush_secs  = self.config.oplog.flush.max_secs
        self.status_secs = self.config.oplog.tailer.status_interval
        self.status_last = time()

        self.cursor_name     = "mongodb_consistent_backup.Oplog.Tailer.TailThread"
        self.timer_name      = "%s-%s" % (self.__class__.__name__, self.uri.replset)
        self.db              = None
        self.conn            = None
        self.count           = 0
        self.first_ts        = None
        self.last_ts         = tail_from_ts
        self.tail_to_ts      = tail_to_ts
        self.stopped         = False
        self._oplog          = None
        self._cursor         = None
        self._cursor_addr    = None
        self.exit_code       = 0
        self._tail_retry     = 0
        self._tail_retry_max = 10

        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, self.close)

    def oplog(self):
        if not self._oplog:
            self._oplog = Oplog(
                self.oplog_file,
                self.dump_gzip,
                'w+',
                self.flush_docs,
                self.flush_secs
            )
        return self._oplog

    def close(self, exit_code=None, frame=None):
        del exit_code
        del frame
        self.tail_stop.set()
        if self.db:
            self.db.close()
        sys.exit(1)

    def check_cursor(self):
        if self.backup_stop.is_set() or self.tail_stop.is_set():
            return False
        elif self._cursor and self._cursor.alive:
            if self._cursor_addr and self._cursor.address and self._cursor.address != self._cursor_addr:
                self.backup_stop.set()
                raise OperationError("Cursor host changed from %s to %s!" % (self._cursor_addr, self._cursor.address))
            elif not self._cursor_addr:
                self._cursor_addr = self._cursor.address
            return True
        return False

    def status(self):
        if self.tail_stop.is_set():
            return
        now = time()
        if (now - self.status_last) >= self.status_secs:
            state = self.state.get()
            logging.info("Oplog getter %s status: %i oplog changes, ts: %s" % (self.uri, state['count'], state['last_ts']))
            self.status_last = now

    def connect(self):
        if not self.db:
            self.db = DB(self.uri, self.config, True, 'secondary', True)
        return self.db.connection()

    def run(self):
        try:
            logging.info("Getting oplog on %s for changes between %s and %s" % (self.uri, self.last_ts, self.tail_to_ts))
            self.timer.start(self.timer_name)

            self.state.set('running', True)
            self.connect()
            oplog = self.oplog()

            try:
                # check if the oplog has rolled over by checking against for the earliest timestamp it contains.
                # This is the same method mongodump does with --oplog
                if not self.db.is_ts_covered_by_oplog(self.last_ts):
                    logging.error("Oplog entry for %s has rolled over since %s. Unable to reach a consistent state." % (self.uri, self.last_ts))
                    raise OperationError("Oplog for %s has rolled over, stopping backup!" % self.uri)

                self._cursor = self.db.get_simple_oplog_cursor_from_to(self.__class__, self.last_ts, self.tail_to_ts)
                try:
                    first_iteration_ts = None
                    for doc in self._cursor:
                        if first_iteration_ts is None:
                            # even if we got here, until we actually started reading the cursor, there was still a race between us and
                            # the oplog being rolled over. So we need to check one last time; again: the same as mongodump --oplog
                            first_iteration_ts = doc['ts']
                            if not self.db.is_ts_covered_by_oplog(first_iteration_ts):
                                logging.error("Oplog entry for %s has rolled over since %s. Unable to reach a consistent state." %
                                              (self.uri, first_iteration_ts))
                                raise OperationError("Oplog for %s has rolled over, stopping backup!" % self.uri)

                        if self.last_ts and self.last_ts >= doc['ts']:
                            continue
                        oplog.add(doc)

                        # update states
                        self.count  += 1
                        self.last_ts = doc['ts']
                        if self.first_ts is None:
                            self.first_ts = self.last_ts

                        update = {
                            'count':    self.count,
                            'first_ts': self.first_ts,
                            'last_ts':  self.last_ts
                        }
                        self.state.set(None, update, True)

                        # print status report every N seconds
                        self.status()
                except NotMasterError:
                    # pymongo.errors.NotMasterError means a RECOVERING-state when connected to secondary (which should be true)
                    self.backup_stop.set()
                    logging.error("Node %s is in RECOVERING state! Stopping oplog getter thread" % self.uri)
                    raise OperationError("Node %s is in RECOVERING state! Stopping oplog getterthread" % self.uri)
                except CursorNotFound:
                    self.backup_stop.set()
                    logging.error("Cursor disappeared on server %s! Stopping oplog getter thread" % self.uri)
                    raise OperationError("Cursor disappeared on server %s! Stopping oplog getter thread" % self.uri)
                except (AutoReconnect, ConnectionFailure, ExceededMaxWaiters, ExecutionTimeout, NetworkTimeout), e:
                    logging.error("Oplog getter %s received %s exception: %s. Stoppin oplog getter thread." % (self.uri, type(e).__name__, e))
                    raise OperationError("Oplog getter %s received %s exception: %s. Stoppin oplog getter thread." % (self.uri, type(e).__name__, e))
            finally:
                if self._cursor:
                    logging.debug("Stopping oplog cursor on %s" % self.uri)
                    self._cursor.close()
        except OperationError, e:
            logging.error("Oplog getter %s encountered error: %s" % (self.uri, e))
            self.exit_code = 1
            self.backup_stop.set()
            raise OperationError(e)
        except ServerSelectionTimeoutError, e:
            logging.error("Oplog getter %s could not connect: %s" % (self.uri, e))
            self.exit_code = 1
            self.backup_stop.set()
            raise OperationError(e)
        except Exception, e:
            logging.error("Oplog getter %s encountered an unexpected error: %s" % (self.uri, e))
            self.exit_code = 1
            self.backup_stop.set()
            raise e
        finally:
            oplog.flush()
            oplog.close()
            self.stopped = True
            self.state.set('running', False)
            self.timer.stop(self.timer_name)

        if self.exit_code == 0:
            log_msg_extra = "%i oplog changes" % self.count
            if self.last_ts:
                log_msg_extra = "%s, end ts: %s" % (log_msg_extra, self.last_ts)
            logging.info("Done getting oplog on %s, %s" % (self.uri, log_msg_extra))
            self.state.set('completed', True)

        sys.exit(self.exit_code)
