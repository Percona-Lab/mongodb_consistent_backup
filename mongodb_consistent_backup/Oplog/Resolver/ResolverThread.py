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

        self.oplogs  = {}
        self.changes = 0

    def cleanup(self):
        if 'tailed' in self.oplogs:
            self.oplogs['tailed'].close()
            del self.oplogs['tailed']
        if 'file' in self.tailed_opplog and os.path.isfile(self.tailed_oplog['file']):
            os.remove(self.tailed_oplog['file'])

    def run(self):
        self.oplogs['backup'] = Oplog(self.mongodump_oplog['file'], self.dump_gzip, 'a+')
        self.oplogs['tailed'] = Oplog(self.tailed_oplog['file'], self.dump_gzip)

        logging.info("Resolving oplog for %s to max ts: %s" % (self.uri, self.max_end_ts))
        try:
            self.state.set('running', True)
            self.state.set('first_ts', self.mongodump_oplog['first_ts'])
            if not self.state.get('first_ts'):
                self.state.set('first_ts', self.tailed_oplog['first_ts'])
            for change in decode_file_iter(self.oplogs['tailed']):
                self.last_ts = change['ts']
                if not self.mongodump_oplog['last_ts'] or self.last_ts > self.mongodump_oplog['last_ts']:
                    if self.last_ts < self.max_end_ts:
                        self.oplogs['backup'].add(change)
                        self.changes += 1
                    elif self.last_ts > self.max_end_ts:
                        break

            self.state.set('count', self.mongodump_oplog['count'] + self.changes)
            self.state.set('last_ts', self.last_ts)
            self.state.set('running', False)
            self.exit_code = 0
        except Exception, e:
            logging.exception("Resolving of oplogs failed! Error: %s" % e)
        finally:
            self.close()

        if self.exit_code == 0:
            logging.info("Applied %i oplog changes to %s oplog, end ts: %s" % (self.changes, self.uri, self.mongodump_oplog_h.last_ts()))
        sys.exit(self.exit_code)

    def close(self):
        self.cleanup()
        if len(self.oplogs) > 0:
            for oplog in self.oplogs:
                self.oplogs[oplog].flush()
                self.oplogs[oplog].close()
                del self.oplogs[oplog]
