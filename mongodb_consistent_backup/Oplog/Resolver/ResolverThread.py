import logging
import os

from gzip import GzipFile
# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson import BSON, decode_file_iter


class ResolverThread:
    def __init__(self, uri, tailed_oplog, mongodump_oplog, max_end_ts, dump_gzip=False):
        self.uri             = uri
        self.tailed_oplog    = tailed_oplog
        self.mongodump_oplog = mongodump_oplog
        self.max_end_ts      = max_end_ts
        self.dump_gzip       = dump_gzip

        self.changes = 0
        self.last_ts = None

    #def append(self, change):

    def run(self):
        logging.info("Resolving oplog for %s to max timestamp: %s" % (self.uri, self.max_end_ts))
        try:
            if self.dump_gzip:
                tailed_oplog_fh = GzipFile(self.tailed_oplog['file'])
                mongodump_oplog_fh = GzipFile(self.mongodump_oplog['file'], 'a+')
            else:
                tailed_oplog_fh = open(self.tailed_oplog['file'])
                mongodump_oplog_fh = open(self.mongodump_oplog['file'], 'a+')
            for change in decode_file_iter(tailed_oplog_fh):
                ts = change['ts']
                if not self.mongodump_oplog['last_ts'] or ts > self.mongodump_oplog['last_ts']:
                    if ts < self.max_end_ts:
                        mongodump_oplog_fh.write(BSON.encode(change))
                        self.changes += 1
                        self.last_ts = ts
                    elif ts > self.max_end_ts:
                        break
            tailed_oplog_fh.close()
            mongodump_oplog_fh.flush()
            mongodump_oplog_fh.close()

            # remove temporary tailed oplog
            os.remove(self.tailed_oplog['file'])
        except Exception, e:
            logging.fatal("Resolving of oplogs failed! Error: %s" % e)
            raise e

        logging.info("Applied %i changes to %s oplog. New end timestamp: %s" % (self.changes, self.uri, self.last_ts))
