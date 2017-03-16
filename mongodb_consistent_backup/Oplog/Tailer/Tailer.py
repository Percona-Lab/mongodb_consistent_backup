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

    def stop(self, kill=False, sleep_secs=2):
        for shard in self.shards:
            replset = self.replsets[shard]
            state   = self.shards[shard]['state']
            stop    = self.shards[shard]['stop']
            thread  = self.shards[shard]['thread']
            host    = state.get('host')
            port    = int(state.get('port'))

            if not kill:
                # get current optime of replset primary to use a stop position
                timestamp = replset.primary_optime()
                if not timestamp:
    		    logging.warning("Could not get current optime from PRIMARY! Using now as a stop time")
                    timestamp = Timestamp(int(time()), 0)
                logging.info("Stopping tailer %s:%i at >= %s" % (host, port, timestamp))
    
                # wait for replication to get in sync
                while state.get('last_ts') and state.get('last_ts') <= timestamp:
                    logging.info('Waiting for tailer %s:%i to reach position: %s, currrently: %s' % (host, port, timestamp, state.get('last_ts')))
                    sleep(sleep_secs)

            # set thread stop event
            self.shards[shard]['stop'].set()
            sleep(sleep_secs)

            # wait for thread to stop
            while thread.is_alive():
                logging.info('Waiting for tailer %s:%i to stop' % (host, port))
                sleep(sleeps_secs)
            logging.info("Stopped tailer thread %s:%i" % (host, port))

            # gather state info
            if host not in self._summary:
                self._summary[host] = {}
            self._summary[host][port] = state.get().copy()

        logging.info("Stopped all oplog threads")
        return self._summary

    def close(self):
        self.stop(True)
