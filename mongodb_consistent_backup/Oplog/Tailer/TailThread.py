import os
import logging

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from multiprocessing import Process, Event
from pymongo import CursorType
from pymongo.errors import AutoReconnect
from signal import signal, SIGINT, SIGTERM
from time import sleep, time

from mongodb_consistent_backup.Common import DB, LocalCommand
from mongodb_consistent_backup.Oplog import Oplog


class TailThread(Process):
    def __init__(self, state, backup_name, base_dir, host, port, dump_gzip=False, user=None,
                 password=None, authdb='admin', update_secs=10):
        Process.__init__(self)
        self.state          = state 
        self.backup_name    = backup_name
        self.base_dir       = base_dir
        self.host           = host
        self.port           = int(port)
        self.dump_gzip      = dump_gzip
        self.user           = user
        self.password       = password
        self.authdb         = authdb
        self.update_secs    = update_secs

        self.last_update     = time()
        self.last_update_str = ""
        self._connection     = None
        self._oplog          = None
        self._stop           = Event()
        self.stop_ts         = None

        self.out_dir    = "%s/%s" % (self.base_dir, self.backup_name)
        self.oplog_file = "%s/oplog-tailed.bson" % self.out_dir
        if not os.path.isdir(self.out_dir):
            try:
                LocalCommand("mkdir", ["-p", self.out_dir]).run()
            except Exception, e:
                logging.error("Cannot make directory %s! Error: %s" % (self.out_dir, e))
                raise e

        # init thread state
        self.state['host']     = self.host
        self.state['port']     = self.port
        self.state['file']     = self.oplog_file
        self.state['count']    = 0
        self.state['first_ts'] = None
        self.state['last_ts']  = None
        self.state['stop_ts']  = self.stop_ts

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

    # the DB connection has to be made outside of __init__ due to threading:
    def connection(self):
        try:
            self._connection = DB(self.host, self.port, self.user, self.password, self.authdb).connection()
        except Exception, e:
            logging.fatal("Cannot get connection - %s" % e)
            raise e
        return self._connection

    def oplog(self):
        if not self._oplog:
            try:
                self._oplog = Oplog(self.oplog_file, self.dump_gzip)
            except Exception, e:
                logging.fatal("Could not open oplog tailing file %s! Error: %s" % (self.oplog_file, e))
                raise e
        return self._oplog

    def stop(self, timestamp=None):
        if timestamp:
            try:
                  self.state['stop_ts'] = timestamp
                logging.info("Set oplog tail thread stop position to timestamp: %s" % timestamp)
            except Exception, e:
                logging.fatal("Cannot create bson.timestamp.Timestamp object! Error: %s" % e)
                raise e
        else:
            self._stop.set()

    def close(self, exit_code=None, frame=None):
        del exit_code
        del frame
        self.stop()
        if self._oplog:
            self._oplog.flush()
            self._oplog.close()

    def do_stop(self):
        if self.state['stop_ts'] and self.state['last_ts'] and self.state['last_ts'] >= self.state['stop_ts']:
            return True
        elif self._stop.is_set():
            logging.warn("Oplog tail thread killed at timestamp: %s" % self.state['last_ts'])
            return True
        return False

    def status(self):
        update_str = "Oplog tailing of %s:%i current position: %s" % (self.host, self.port, self.state['last_ts'])
        if update_str != self.last_update_str:
            logging.info(update_str)
            self.last_update     = time()
            self.last_update_str = update_str

    def do_status(self):
        return (time() - self.last_update) >= self.update_secs

    def run(self):
        conn  = self.connection()
        db    = conn['local']

        logging.info("Tailing oplog on %s:%i for changes (options: gzip=%s)" % (self.host, self.port, self.dump_gzip))

        oplog = self.oplog()
        tail_start_ts = db.oplog.rs.find().sort('$natural', -1)[0]['ts']
        while not self.do_stop():
            query  = {'ts': {'$gt': tail_start_ts}}
            oplog  = self.oplog()
            cursor = db.oplog.rs.find(query, cursor_type=CursorType.TAILABLE_AWAIT)
            try:
                while not self.do_stop():
                    try:
                        # get the next oplog doc and write it
                        doc = cursor.next()
                        if doc:
                            oplog.write(doc)
                            self.state['count']    = oplog.count()
                             self.state['first_ts'] = oplog.first_ts()
                               self.state['last_ts']  = oplog.last_ts()
                            self.state['stop_ts']  = self.stop_ts
                                if self.do_status():
                                self.status()
                    except (AutoReconnect, StopIteration):
                        if self.do_stop():
                            break
                        sleep(1)
            finally:
                logging.debug("Stopping oplog cursor on %s:%i" % (self.host, self.port))
                cursor.close()
                oplog.flush()
        oplog.close()

        logging.info("Done tailing oplog on %s:%i, %i changes captured to: %s" % (self.host, self.port, self.state['count'], self.state['last_ts']))
