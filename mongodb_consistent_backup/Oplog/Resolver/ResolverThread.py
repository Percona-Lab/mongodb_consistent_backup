import logging
import os
# Skip bson in requirements , pymongo provides
# noinspection PyPackageRequirements
from bson import decode_file_iter
from bson.codec_options import CodecOptions

from mongodb_consistent_backup.Errors import Error
from mongodb_consistent_backup.Oplog import Oplog
from mongodb_consistent_backup.Pipeline import PoolThread


class ResolverThread(PoolThread):
    def __init__(self, config, state, uri, tailed_oplog, mongodump_oplog, max_end_ts, compression='none'):
        super(ResolverThread, self).__init__(self.__class__.__name__, compression)
        self.config             = config
        self.state              = state
        self.uri                = uri
        self.tailed_oplog       = tailed_oplog
        self.mongodump_oplog    = mongodump_oplog
        self.max_end_ts         = max_end_ts
        self.compression_method = compression

        # Pool threads break self.config unless flattened to a normal dict:
        self.flush_docs = self.config['oplog']['flush']['max_docs']
        self.flush_secs = self.config['oplog']['flush']['max_secs']

        self.oplogs  = {}
        self.changes = 0
        self.stopped = False

    def run(self):
        try:
            self.oplogs['backup'] = Oplog(self.mongodump_oplog['file'], self.do_gzip(), 'a+', self.flush_docs, self.flush_secs)
            self.oplogs['tailed'] = Oplog(self.tailed_oplog['file'], self.do_gzip())
            logging.info("Resolving oplog for %s to max ts: %s" % (self.uri, self.max_end_ts))
            self.state.set('running', True)
            self.state.set('first_ts', self.mongodump_oplog['first_ts'])
            if not self.state.get('first_ts'):
                self.state.set('first_ts', self.tailed_oplog['first_ts'])
            for change in decode_file_iter(self.oplogs['tailed'], CodecOptions(unicode_decode_error_handler="ignore")):
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
            raise Error("Resolving of oplogs failed! Error: %s" % e)
        finally:
            self.close()

        if self.exit_code == 0:
            logging.info("Applied %i oplog changes to %s oplog, end ts: %s" % (self.changes, self.uri, self.last_ts))
            return self.uri.str()

    def close(self):
        if len(self.oplogs) > 0 and not self.stopped:
            logging.debug("Closing oplog file handles")
            for oplog in self.oplogs:
                self.oplogs[oplog].close()
            self.stopped = True
        if 'file' in self.tailed_oplog and os.path.isfile(self.tailed_oplog['file']):
            logging.debug("Removing temporary/tailed oplog file: %s" % self.tailed_oplog['file'])
            os.remove(self.tailed_oplog['file'])
