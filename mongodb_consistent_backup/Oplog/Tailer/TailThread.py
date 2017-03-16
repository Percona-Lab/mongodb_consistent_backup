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
    def __init__(self, do_stop, backup_name, oplog_file, state, host, port, dump_gzip=False, user=None,
                 password=None, authdb='admin', status_secs=15):
        Process.__init__(self)
        self.do_stop     = do_stop
        self.backup_name = backup_name
        self.oplog_file  = oplog_file
        self.state       = state
        self.host        = host
        self.port        = int(port)
        self.dump_gzip   = dump_gzip
        self.user        = user
        self.password    = password
        self.authdb      = authdb
        self.status_secs = status_secs
        self.status_last = time()

        self.count       = 0
        self.last_ts     = None
        self.stopped     = False
        self._connection = None
        self._oplog      = None

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
        self.do_stop.set()
        while not self.stopped:
            sleep(1)

    def status(self):
        if self.do_stop.is_set():
            return
        now = time()
        if (now - self.status_last) >= self.status_secs:
            state = self.state.get()
	    logging.info("Oplog tailer %s:%i status: %i changes captured to: %s" % (self.host, self.port, state['count'], state['last_ts']))
            self.status_last = now

    def run(self):
        conn = self.connection()
        db   = conn['local']

        logging.info("Tailing oplog on %s:%i for changes (options: gzip=%s, status_secs=%i)" % (self.host, self.port, self.dump_gzip, self.status_secs))

        self.state.set('running', True)
        oplog = self.oplog()
        tail_start_ts = db.oplog.rs.find().sort('$natural', -1)[0]['ts']
        while not self.do_stop.is_set():
            oplog  = self.oplog()
            query  = {'ts': {'$gt': tail_start_ts}}
            cursor = db.oplog.rs.find(query, cursor_type=CursorType.TAILABLE_AWAIT)
            try:
                while not self.do_stop.is_set():
                    try:
                        # get the next oplog doc and write it
                        doc = cursor.next()
                        oplog.write(doc)

			# update states
                        self.count += 1
                        self.last_ts = doc['ts']
                        self.state.set('count', self.count)
                        self.state.set('last_ts', self.last_ts)
                        if self.state.get('first_ts'):
                            self.state.set('first_ts', doc['ts'])

                        # print status report every N seconds
                        self.status()
                    except (AutoReconnect, StopIteration):
                        if self.do_stop.is_set():
                            break
                        sleep(1)
            finally:
                logging.debug("Stopping oplog cursor on %s:%i" % (self.host, self.port))
                cursor.close()
                oplog.flush()
        oplog.close()
        self.stopped = True

        logging.info("Done tailing oplog on %s:%i, %i changes captured to: %s" % (self.host, self.port, self.state.get('count'), self.state.get('last_ts')))
        self.state.set('running', False)
