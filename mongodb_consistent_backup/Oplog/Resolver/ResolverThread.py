import logging
import os
import sys

# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson import decode_file_iter

from mongodb_consistent_backup.Oplog import Oplog


class ResolverThread:
    def __init__(self, uri, tailed_oplog, mongodump_oplog, max_end_ts, dump_gzip=False):
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
            for change in decode_file_iter(self.tailed_oplog_fh):
                ts = change['ts']
                if not self.mongodump_oplog['last_ts'] or ts > self.mongodump_oplog['last_ts']:
                    if ts < self.max_end_ts:
                        self.mongodump_oplog_h.add(change)
                        self.changes += 1
                    elif ts > self.max_end_ts:
                        break
            self.tailed_oplog_fh.close()
            self.mongodump_oplog_h.flush()
            self.mongodump_oplog_h.close()

            # remove temporary tailed oplog
            os.remove(self.tailed_oplog['file'])
        except Exception, e:
            logging.exception("Resolving of oplogs failed! Error: %s" % e)
            sys.exit(1)

        logging.info("Applied %i oplog changes to %s oplog, end ts: %s" % (self.changes, self.uri, self.mongodump_oplog_h.last_ts()))
