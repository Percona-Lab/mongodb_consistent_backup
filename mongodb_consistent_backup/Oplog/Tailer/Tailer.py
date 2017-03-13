import logging

from bson.timestamp import Timestamp
from multiprocessing import Event, Manager
from time import time, sleep

from TailThread import TailThread
from mongodb_consistent_backup.Common import parse_method


class Tailer:
    def __init__(self, config, replsets, base_dir):
        self.config      = config
        self.replsets    = replsets
        self.base_dir    = base_dir
        self.backup_name = self.config.name
        self.user        = self.config.user
        self.password    = self.config.password
        self.authdb      = self.config.authdb

        self.shards   = {}
        self._summary = {}

        self._manager      = Manager()
        self.thread_states = {}

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

    def thread_state(self, shard):
        if not shard in self.thread_states:
            self.thread_states[shard] = self._manager.dict()
        return self.thread_states[shard]

    def run(self):
        for shard in self.replsets:
            secondary    = self.replsets[shard].find_secondary()
            shard_name   = secondary['replSet']
            host, port   = secondary['host'].split(":")
	    thread_state = self.thread_state(shard)
	    thread_stop  = Event()
            thread = TailThread(
                thread_state,
		thread_stop,
                shard_name,
                self.base_dir,
                host,
                port,
                self.do_gzip(),
                self.user,
                self.password,
                self.authdb
            )
	    self.shards[shard] = {
	        'host':   host,
		'port':   port,
                'thread': thread,
		'state':  thread_state,
		'stop':   thread_stop
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
            if self.state('last_ts') >= timestamp:
                
	        #stop.set()
		#thread.stop()
            while thread.is_alive():
		print 'waiting...'
                sleep(1)
        logging.info("Stopped all oplog threads")

        for shard in self.shards:
	    state = self.shards[shard]['state']
            host  = state['host']
            port  = state['port']
            if host not in self._summary:
                self._summary[host] = {}
            self._summary[host][port] = state
        return self._summary

    def close(self):
        self.stop()
