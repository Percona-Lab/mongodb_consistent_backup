import logging

from gzip import GzipFile
from bson import BSON, decode_file_iter


class ResolverThread:
    def __init__(self, host, port, tailed_oplog_file, mongodump_oplog_file, mongodump_oplog_last_ts, max_end_ts, dump_gzip=False):
        self.host                    = host
        self.port                    = port
        self.tailed_oplog_file       = tailed_oplog_file
        self.mongodump_oplog_file    = mongodump_oplog_file
        self.mongodump_oplog_last_ts = mongodump_oplog_last_ts
        self.max_end_ts              = max_end_ts
        self.dump_gzip               = dump_gzip

        self.changes = 0
        self.last_ts = None

    def run(self):
        logging.info("Resolving oplog for host %s:%s to max timestamp: %s" % (self.host, self.port, self.max_end_ts))

        try:
            if self.dump_gzip:
                tailed_oplog_fh = GzipFile(self.tailed_oplog_file)
                mongodump_oplog_fh = GzipFile(self.mongodump_oplog_file, 'a+')
            else:
                tailed_oplog_fh = open(self.tailed_oplog_file)
                mongodump_oplog_fh = open(self.mongodump_oplog_file, 'a+')

            for change in decode_file_iter(tailed_oplog_fh):
                if 'ts' in change:
                    ts = change['ts']
                    if ts > self.mongodump_oplog_last_ts or self.mongodump_oplog_last_ts is None:
                        if ts < self.max_end_ts:
                            mongodump_oplog_fh.write(BSON.encode(change))
                            self.changes += 1
                            self.last_ts = ts
                        elif ts > self.max_end_ts:
                            break
            tailed_oplog_fh.close()
            mongodump_oplog_fh.flush()
            mongodump_oplog_fh.close()
        except Exception, e:
            logging.fatal("Resolving of oplogs failed! Error: %s" % e)
            raise e

        logging.info("Applied %i changes to host %s:%s oplog. New end timestamp: %s" % (self.changes, self.host, self.port, self.last_ts))
