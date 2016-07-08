import os
import logging

from bson import BSON
from gzip import GzipFile
from multiprocessing import Process, Event
from pymongo import CursorType
from pymongo.errors import AutoReconnect
from signal import signal, SIGINT, SIGTERM
from time import sleep

from MongoBackup.Common import DB, LocalCommand


class OplogTail(Process):
    def __init__(self, response_queue, backup_name, base_dir, host, port, dump_gzip=False, user=None, password=None,
                 authdb='admin'):
        Process.__init__(self)
        self.response_queue = response_queue
        self.backup_name    = backup_name
        self.base_dir       = base_dir
        self.host           = host
        self.port           = int(port)
        self.dump_gzip      = dump_gzip
        self.user           = user
        self.password       = password
        self.authdb         = authdb

        self.count       = 0
        self._connection = None
        self.first_ts    = None
        self.last_ts     = None
        self._stop       = Event()

        self.out_dir    = "%s/%s" % (self.base_dir, self.backup_name)
        self.oplog_file = "%s/oplog-tailed.bson" % self.out_dir
        if not os.path.isdir(self.out_dir):
            try:
                LocalCommand("mkdir", ["-p", self.out_dir]).run()
            except Exception, e:
                logging.error("Cannot make directory %s! Error: %s" % (self.out_dir, e))
                raise e

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

    def stop(self):
        self._stop.set()

    def close(self, code=None, frame=None):
        return self.stop()

    def stopped(self):
        return self._stop.is_set()

    def run(self):
        logging.info("Tailing oplog on %s:%i for changes" % (self.host, self.port))

        # open the oplog file for writing:
        try:
            if self.dump_gzip:
                oplog = GzipFile(self.oplog_file, 'w')
            else:
                oplog = open(self.oplog_file, 'w')
        except Exception, e:
            logging.fatal("Could not open oplog tailing file %s! Error: %s" % (self.oplog_file, e))
            raise e

        conn = self.connection()
        db = conn['local']
        last_ts = db.oplog.rs.find().sort('$natural', -1)[0]['ts']
        while not self.stopped():
            query = {'ts': {'$gt': last_ts}}
            cursor = db.oplog.rs.find(query, cursor_type=CursorType.TAILABLE_AWAIT)
            try:
                while not self.stopped() and cursor.alive:
                    try:
                        # get the next oplog doc and write it
                        doc = cursor.next()
                        oplog.write(BSON.encode(doc))

                        # increment count and set first/last timestamp
                        self.count += 1
                        if 'ts' in doc:
                            if self.first_ts is None:
                                self.first_ts = doc['ts']
                            self.last_ts = doc['ts']
                    except (AutoReconnect, StopIteration):
                        if self.stopped():
                            break
                        sleep(1)
            finally:
                logging.debug("Stopping oplog cursor on %s:%i" % (self.host, self.port))
                cursor.close()
                oplog.flush()

        # flush and close the oplog file:
        oplog.flush()
        oplog.close()

        self.response_queue.put({
            'host': self.host,
            'port': self.port,
            'file': self.oplog_file,
            'count': self.count,
            'last_ts': self.last_ts,
            'first_ts': self.first_ts
        })

        logging.info("Done tailing oplog on %s:%i, %i changes captured to: %s" % (self.host, self.port, self.count, str(self.last_ts)))
