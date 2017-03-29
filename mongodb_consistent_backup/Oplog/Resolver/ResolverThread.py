import logging
import os
import sys

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson import decode_file_iter

from mongodb_consistent_backup.Oplog import Oplog


class ResolverThread:
    def __init__(self, state, uri, tailed_oplog, mongodump_oplog, max_end_ts, dump_gzip=False):
        self.state           = state
        self.uri             = uri
        self.tailed_oplog    = tailed_oplog
        self.mongodump_oplog = mongodump_oplog
        self.max_end_ts      = max_end_ts
        self.dump_gzip       = dump_gzip

        self.changes = 0

    def run(self):
        self.mongodump_oplog_h = Oplog(self.mongodump_oplog['file'], self.dump_gzip, 'a+')
        self.tailed_oplog_fh   = Oplog(self.tailed_oplog['file'], self.dump_gzip)

        logging.info("Resolving oplog for %s to max ts: %s" % (self.uri, self.max_end_ts))
        try:
            self.state.set('running', True)
	    self.state.set('first_ts', self.mongodump_oplog['first_ts'])
            for change in decode_file_iter(self.tailed_oplog_fh):
                self.last_ts = change['ts']
                if not self.mongodump_oplog['last_ts'] or self.last_ts > self.mongodump_oplog['last_ts']:
                    if self.last_ts < self.max_end_ts:
                        self.mongodump_oplog_h.add(change)
                        self.changes += 1
                    elif self.last_ts > self.max_end_ts:
                        break
            self.tailed_oplog_fh.close()
            self.mongodump_oplog_h.flush()
            self.mongodump_oplog_h.close()

            # remove temporary tailed oplog
            os.remove(self.tailed_oplog['file'])

            self.state.set('count', self.mongodump_oplog['count'] + self.changes)
	    self.state.set('last_ts', self.last_ts)
            self.state.set('running', False)
        except Exception, e:
            logging.exception("Resolving of oplogs failed! Error: %s" % e)
            sys.exit(1)

        logging.info("Applied %i oplog changes to %s oplog, end ts: %s" % (self.changes, self.uri, self.mongodump_oplog_h.last_ts()))
