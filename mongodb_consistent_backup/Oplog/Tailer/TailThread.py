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
    def __init__(self, do_stop, uri, oplog_file, state, dump_gzip=False, user=None, password=None,
                 authdb='admin', status_secs=15):
        Process.__init__(self)
        self.do_stop     = do_stop
        self.uri         = uri
        self.oplog_file  = oplog_file
        self.state       = state
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
        if not self._connection:
            self._connection = DB(self.uri.host, self.uri.port, self.user, self.password, self.authdb).connection()
        return self._connection

    def oplog(self):
        if not self._oplog:
            self._oplog = Oplog(self.oplog_file, self.dump_gzip, 'w+')
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
            logging.info("Oplog tailer %s status: %i changes captured to: %s" % (self.uri, state['count'], state['last_ts']))
            self.status_last = now

    def run(self):
        logging.info("Tailing oplog on %s for changes (options: gzip=%s, status_secs=%i)" % (self.uri, self.dump_gzip, self.status_secs))

        conn  = self.connection()
        db    = conn['local']
        oplog = self.oplog()
        tail_start_ts = db.oplog.rs.find().sort('$natural', -1)[0]['ts']
        self.state.set('running', True)
        while not self.do_stop.is_set():
            # http://api.mongodb.com/python/current/examples/tailable.html
            query  = {'ts': {'$gt': tail_start_ts}}
            cursor = db.oplog.rs.find(query, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True)
            try:
                while not self.do_stop.is_set():
                    try:
                        # get the next oplog doc and write it
                        doc = cursor.next()
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
                    except (AutoReconnect, StopIteration):
                        if self.do_stop.is_set():
                            break
                        sleep(1)
            finally:
                logging.debug("Stopping oplog cursor on %s" % self.uri)
                cursor.close()
                oplog.flush()
        oplog.close()
        self.stopped = True

        log_msg_extra = "%i changes captured" % self.state.get('count')
        last_ts = self.state.get('last_ts')
        if last_ts:
            log_msg_extra = "%s to: %s" % (log_msg_extra, last_ts)
        logging.info("Done tailing oplog on %s, %s" % (self.uri, log_msg_extra))
        self.state.set('running', False)
