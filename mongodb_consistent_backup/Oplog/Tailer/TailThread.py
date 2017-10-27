import logging
import sys

# noinspection PyPackageRequirements
from multiprocessing import Process
from pymongo.errors import AutoReconnect, ConnectionFailure, CursorNotFound, ExceededMaxWaiters
from pymongo.errors import ExecutionTimeout, NetworkTimeout, NotMasterError, ServerSelectionTimeoutError
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from time import sleep, time

from mongodb_consistent_backup.Common import DB
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Oplog import Oplog


class TailThread(Process):
    def __init__(self, backup_stop, tail_stop, uri, config, timer, oplog_file, state, dump_gzip=False):
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
        self.last_ts         = None
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
                raise OperationError("Tailer host changed from %s to %s!" % (self._cursor_addr, self._cursor.address))
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
            logging.info("Oplog tailer %s status: %i oplog changes, ts: %s" % (self.uri, state['count'], state['last_ts']))
            self.status_last = now

    def connect(self):
        if not self.db:
            self.db = DB(self.uri, self.config, True, 'secondary', True)
        return self.db.connection()

    def run(self):
        try:
            logging.info("Tailing oplog on %s for changes" % self.uri)
            self.timer.start(self.timer_name)

            self.state.set('running', True)
            self.connect()
            oplog = self.oplog()
            while not self.tail_stop.is_set() and not self.backup_stop.is_set():
                try:
                    self._cursor = self.db.get_oplog_cursor_since(self.__class__, self.last_ts)
                    while self.check_cursor():
                        try:
                            # get the next oplog doc and write it
                            doc = self._cursor.next()
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
                            logging.error("Node %s is in RECOVERING state! Stopping tailer thread" % self.uri)
                            raise OperationError("Node %s is in RECOVERING state! Stopping tailer thread" % self.uri)
                        except CursorNotFound:
                            self.backup_stop.set()
                            logging.error("Cursor disappeared on server %s! Stopping tailer thread" % self.uri)
                            raise OperationError("Cursor disappeared on server %s! Stopping tailer thread" % self.uri)
                        except (AutoReconnect, ConnectionFailure, ExceededMaxWaiters, ExecutionTimeout, NetworkTimeout), e:
                            logging.error("Tailer %s received %s exception: %s. Attempting retry" % (self.uri, type(e).__name__, e))
                            if self._tail_retry > self._tail_retry_max:
                                self.backup_stop.set()
                                logging.error("Reconnected to %s %i/%i times, stopping backup!" % (self.uri, self._tail_retry, self._tail_retry_max))
                                raise OperationError("Reconnected to %s %i/%i times, stopping backup!" % (self.uri, self._tail_retry, self._tail_retry_max))
                            self._tail_retry += 1
                        except StopIteration:
                            continue
                    sleep(1)
                finally:
                    if self._cursor:
                        logging.debug("Stopping oplog cursor on %s" % self.uri)
                        self._cursor.close()
        except OperationError, e:
            logging.error("Tailer %s encountered error: %s" % (self.uri, e))
            self.exit_code = 1
            self.backup_stop.set()
            raise OperationError(e)
        except ServerSelectionTimeoutError, e:
            logging.error("Tailer %s could not connect: %s" % (self.uri, e))
            self.exit_code = 1
            self.backup_stop.set()
            raise OperationError(e)
        except Exception, e:
            logging.error("Tailer %s encountered an unexpected error: %s" % (self.uri, e))
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
            logging.info("Done tailing oplog on %s, %s" % (self.uri, log_msg_extra))
            self.state.set('completed', True)

        sys.exit(self.exit_code)
