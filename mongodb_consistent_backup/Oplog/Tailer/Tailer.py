import os
import logging

from bson.timestamp import Timestamp
from multiprocessing import Event, Manager
from time import time, sleep

from TailThread import TailThread
from mongodb_consistent_backup.Common import parse_method
from mongodb_consistent_backup.Oplog import OplogState


class Tailer:
    def __init__(self, config, replsets, base_dir):
        self.config      = config
        self.replsets    = replsets
        self.base_dir    = base_dir
        self.backup_name = self.config.name
        self.user        = self.config.user
        self.password    = self.config.password
        self.authdb      = self.config.authdb

        self.manager  = Manager()
        self.shards   = {}
        self._summary = {}

    def compression(self, method=None):
        if method:
            self.config.oplog.compression = parse_method(method)
            logging.info("Setting oplog compression method to: %s" % self.config.oplog.compression)
        return parse_method(self.config.oplog.compression)

    def do_gzip(self):
        if self.compression() == 'gzip':
            return True
        return False

    def summary(self):
        return self._summary

    def prepare_tail_oplog(self, shard_name):
        oplog_dir = "%s/%s" % (self.base_dir, shard_name)
        if not os.path.isdir(oplog_dir):
            os.makedirs(oplog_dir)
        return "%s/oplog-tailed.bson" % oplog_dir

    def run(self):
        for shard in self.replsets:
            secondary   = self.replsets[shard].find_secondary()
            shard_name  = secondary['replSet']
            host, port  = secondary['host'].split(":")
            oplog_file  = self.prepare_tail_oplog(shard_name)
            oplog_state = OplogState(self.manager, host, port, oplog_file)
	    stop        = Event()
            thread = TailThread(
	        stop,
                shard_name,
                oplog_file,
                oplog_state,
                host,
                port,
                self.do_gzip(),
                self.user,
                self.password,
                self.authdb
            )
	    self.shards[shard] = {
	        'stop':   stop,
                'thread': thread,
		'state':  oplog_state,
	    }
            self.shards[shard]['thread'].start()

    def stop(self, timestamp=None):
        if not timestamp:
            timestamp = Timestamp(int(time()), 0)
        logging.info("Stopping oplog tailing threads at >= %s" % timestamp)
 	for shard in self.shards:
	    state  = self.shards[shard]['state']
	    stop   = self.shards[shard]['stop']
	    thread = self.shards[shard]['thread']
            while state.get('last_ts') <= timestamp or not state.get('last_ts'):
                print 'waiting for thread for host %s to reach %s, currrently: %s...' % (state.get('host'), timestamp, state.get('last_ts'))
                sleep(1)
	    print 'stopping thread for host %s' % state.get('host')
	    self.shards[shard]['stop'].set()
            while thread.is_alive():
                print 'waiting for thread for host %s to die...' % (state.get('host'))
                sleep(1)
        logging.info("Stopped all oplog threads")

        for shard in self.shards:
	    state = self.shards[shard]['state'].get().copy()
            host  = state['host']
            port  = state['port']
            if host not in self._summary:
                self._summary[host] = {}
            self._summary[host][port] = state
        return self._summary

    def close(self):
        self.stop()
