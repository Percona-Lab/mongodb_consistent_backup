import os
import logging

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from multiprocessing import Process
from pymongo import CursorType
from pymongo.errors import AutoReconnect
from signal import signal, SIGINT, SIGTERM
from time import sleep, time

from mongodb_consistent_backup.Common import DB
from mongodb_consistent_backup.Oplog import Oplog


class TailThread(Process):
    def __init__(self, state, stop, backup_name, base_dir, host, port, dump_gzip=False, user=None,
                 password=None, authdb='admin', update_secs=10):
        Process.__init__(self)
        self.state       = state 
        self.stop        = stop
        self.backup_name = backup_name
        self.base_dir    = base_dir
        self.host        = host
        self.port        = int(port)
        self.dump_gzip   = dump_gzip
        self.user        = user
        self.password    = password
        self.authdb      = authdb
        self.update_secs = update_secs

        self.stopped     = False
        self._connection = None
        self._oplog      = None

        self.out_dir    = "%s/%s" % (self.base_dir, self.backup_name)
        self.oplog_file = "%s/oplog-tailed.bson" % self.out_dir
        if not os.path.isdir(self.out_dir):
            try:
                os.makedirs(self.out_dir)
            except Exception, e:
                logging.error("Cannot make directory %s! Error: %s" % (self.out_dir, e))
                raise e

        # init thread state
        self.state['host']     = self.host
        self.state['port']     = self.port
        self.state['file']     = self.oplog_file
        self.state['count']    = None
        self.state['first_ts'] = None
        self.state['last_ts']  = None

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

    def close(self, exit_code=None, frame=None):
        del exit_code
        del frame
        self.stop.set()
        if self._oplog:
            self._oplog.flush()
            self._oplog.close()

    def do_stop(self):
	if self.state('stop_ts') 
            logging.info("Oplog tail thread stopping at timestamp: %s" % self.state('last_ts'))
	    while self.statee('last_ts') < self.state('stop_ts'):
	        logging.info("Waiting to reach stop timestamp: %s" % self.state('stop_ts'))
		sleep(1)
            return True
        return False

    def state(self, key, value=None):
	if key and value:
	    self._state[key] = value
	if key in self._state:
	    return self._state[key]
        return None

    def run(self):
        conn  = self.connection()
        db    = conn['local']

        logging.info("Tailing oplog on %s:%i for changes (options: gzip=%s)" % (self.host, self.port, self.dump_gzip))

        oplog = self.oplog()
        tail_start_ts = db.oplog.rs.find().sort('$natural', -1)[0]['ts']
        while not self.do_stop():
            oplog  = self.oplog()
            query  = {'ts': {'$gt': tail_start_ts}}
            cursor = db.oplog.rs.find(query, cursor_type=CursorType.TAILABLE_AWAIT)
            try:
                while not self.do_stop():
                    try:
                        # get the next oplog doc and write it
                        doc = cursor.next()
                        oplog.write(doc)
			self.state('last_ts') = doc['ts']
			self.state('count')   = self.state('count') + 1
                    except (AutoReconnect, StopIteration):
                        if self.do_stop():
                            break
                        sleep(1)
            finally:
                logging.debug("Stopping oplog cursor on %s:%i" % (self.host, self.port))
                cursor.close()
                oplog.flush()
        oplog.close()
	self.stopped = True

        logging.info("Done tailing oplog on %s:%i, %i changes captured to: %s" % (self.host, self.port, self.state['count'], self.state['last_ts']))
