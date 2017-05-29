import logging
import sys

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson.codec_options import CodecOptions
from multiprocessing import Process
from pymongo import DESCENDING, CursorType
from pymongo.errors import AutoReconnect, CursorNotFound, ExceededMaxWaiters, ExecutionTimeout, NetworkTimeout, NotMasterError
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from time import sleep, time

from mongodb_consistent_backup.Common import DB
from mongodb_consistent_backup.Errors import Error, OperationError
from mongodb_consistent_backup.Oplog import Oplog


class TailThread(Process):
    def __init__(self, do_stop, uri, config, timer, oplog_file, state, dump_gzip=False):
        Process.__init__(self)
        self.do_stop     = do_stop
        self.uri         = uri
        self.config      = config
        self.timer       = timer
        self.oplog_file  = oplog_file
        self.state       = state
        self.dump_gzip   = dump_gzip
        self.status_secs = self.config.oplog.tailer.status_interval
        self.status_last = time()

        self.cursor_name  = "mongodb_consistent_backup.Oplog.Tailer.TailThread"
        self.timer_name   = "%s-%s" % (self.__class__.__name__, self.uri.replset)
        self.conn         = None
        self.count        = 0
        self.last_ts      = None
        self.stopped      = False
        self._oplog       = None
        self._cursor      = None
        self._cursor_addr = None
        self.exit_code    = 0

        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, self.close)

    def oplog(self):
        if not self._oplog:
            self._oplog = Oplog(self.config, self.oplog_file, self.dump_gzip, 'w+')
        return self._oplog

    def close(self, exit_code=None, frame=None):
        del exit_code
        del frame
        self.do_stop.set()
        if self.conn:
            self.conn.close()
        sys.exit(1)

    def check_cursor(self):
        if self._cursor and self._cursor.alive:
            if self._cursor_addr and self._cursor.address != self._cursor_addr:
                self.close()
                raise OperationError("Tailer host changed from %s to %s!" % (self._cursor_addr, self._cursor.address))
            elif not self._cursor_addr:
                self._cursor_addr = self._cursor.address
            return True
        return False

    def status(self):
        if self.do_stop.is_set():
            return
        now = time()
        if (now - self.status_last) >= self.status_secs:
            state = self.state.get()
            logging.info("Oplog tailer %s status: %i oplog changes, ts: %s" % (self.uri, state['count'], state['last_ts']))
            self.status_last = now

    def run(self):
        try:
            logging.info("Tailing oplog on %s for changes" % self.uri)
            self.timer.start(self.timer_name)
    
            self.conn = DB(self.uri, self.config, True, 'secondary').connection()
            db        = self.conn['local']
            oplog     = self.oplog()
            oplog_rs  = db.oplog.rs.with_options(codec_options=CodecOptions(unicode_decode_error_handler="ignore"))
    
            tail_start_doc = oplog_rs.find_one(sort=[('$natural', DESCENDING)])
            self.state.set('running', True)
            while not self.do_stop.is_set():
                try:
                    # http://api.mongodb.com/python/current/examples/tailable.html
                    query = {'ts':{'$gt':tail_start_doc['ts']}}
                    logging.debug("Querying oplog on %s with query: %s" % (self.uri, query))
                    self._cursor = oplog_rs.find(query, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True).comment(self.cursor_name)
                    while self.check_cursor():
                        try:
                            # get the next oplog doc and write it
                            doc = self._cursor.next()
                            oplog.add(doc)
    
                            # update states
                            self.count += 1
                            if self.count == 1:
                                self.state.set('first_ts', doc['ts'])
                            self.last_ts = doc['ts']
                            self.state.set('count', self.count)
                            self.state.set('last_ts', self.last_ts)
    
                            # print status report every N seconds
                            self.status()
                        except NotMasterError:
                            # pymongo.errors.NotMasterError means a RECOVERING-state when connected to secondary (which should be true)
                            self.do_stop.set()
                            logging.error("Node %s is in RECOVERING state! Stopping tailer thread" % self.uri)
                            raise OperationError("Node %s is in RECOVERING state! Stopping tailer thread" % self.uri)
                        except CursorNotFound:
                            self.do_stop.set()
                            logging.error("Cursor disappeared on server %s! Stopping tailer thread" % self.uri)
                            raise OperationError("Cursor disappeared on server %s! Stopping tailer thread" % self.uri)
                        except (AutoReconnect, ExceededMaxWaiters, ExecutionTimeout, NetworkTimeout), e:
                            logging.error("Tailer %s received pymongo.errors.AutoReconnect exception: %s" % (self.uri, e))
                            if self.do_stop.is_set():
                                break
                        except StopIteration:
                            if self.do_stop.is_set():
                                break
                    sleep(1)
                except Exception, e:
                    self.do_stop.set()
                    raise e
        except OperationError, e:
            logging.error("Tailer %s encountered error: %s" % (self.uri, e))
            self.exit_code = 1
        except Exception, e:
            logging.error("Tailer %s unexpected encountered error: %s" % (self.uri, e))
            self.exit_code = 1
            raise Error(e) 
        finally:
            if self._cursor:
                logging.debug("Stopping oplog cursor on %s" % self.uri)
                self._cursor.close()
            oplog.flush()
            oplog.close()
            self.stopped = True
            self.state.set('running', False)
            self.timer.stop(self.timer_name)

        if self.exit_code == 0:
            log_msg_extra = "%i oplog changes" % self.state.get('count')
            last_ts = self.state.get('last_ts')
            if last_ts:
                log_msg_extra = "%s, end ts: %s" % (log_msg_extra, last_ts)
            logging.info("Done tailing oplog on %s, %s" % (self.uri, log_msg_extra))
            self.state.set('completed', True)
 
        sys.exit(self.exit_code)
