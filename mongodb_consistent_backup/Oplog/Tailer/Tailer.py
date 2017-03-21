import bson
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

    def prepare_oplog_files(self, shard_name):
        oplog_dir = "%s/%s" % (self.base_dir, shard_name)
        if not os.path.isdir(oplog_dir):
            os.makedirs(oplog_dir)
        oplog_file = "%s/oplog-tailed.bson" % oplog_dir
        oplog_state_file = "%s/state.bson" % oplog_dir
        return oplog_file, oplog_state_file

    def write_state(self, state, state_file):
        f = open(state_file, "w")
        f.write(bson.BSON.encode(state))
        f.close()

    def run(self):
        for shard in self.replsets:
            stop        = Event()
            secondary   = self.replsets[shard].find_secondary()
            shard_name  = secondary['replSet']
            host, port  = secondary['host'].split(":")

            print self.replsets[shard].get_mongo_config()

            oplog_file, oplog_state_file = self.prepare_oplog_files(shard_name)
            oplog_state = OplogState(self.manager, host, port, oplog_file)
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
                'state_file': oplog_state_file
            }
            self.shards[shard]['thread'].start()

    def stop(self, kill=False, sleep_secs=2):
        for shard in self.shards:
            replset    = self.replsets[shard]
            state      = self.shards[shard]['state']
            state_file = self.shards[shard]['state_file']
            stop       = self.shards[shard]['stop']
            thread     = self.shards[shard]['thread']
            host       = state.get('host')
            port       = int(state.get('port'))

            if not kill:
                # get current optime of replset primary to use a stop position
                try:
                    timestamp = replset.primary_optime(True, True)
                except:
                    logging.warning("Could not get current optime from PRIMARY! Using now as a stop time")
                    timestamp = Timestamp(int(time()), 0)
                logging.info("Stopping tailer %s:%i at >= PRIMARY optime: %s" % (host, port, timestamp))
    
                # wait for replication to get in sync
                while state.get('last_ts') and state.get('last_ts') < timestamp:
                    logging.info('Waiting for tailer %s:%i to reach position: %s, currrently: %s' % (host, port, timestamp, state.get('last_ts')))
                    sleep(sleep_secs)

            # set thread stop event
            self.shards[shard]['stop'].set()
            sleep(sleep_secs)

            # wait for thread to stop
            while thread.is_alive():
                logging.info('Waiting for tailer %s:%i to stop' % (host, port))
                sleep(sleep_secs)
            logging.info("Stopped tailer thread %s:%i" % (host, port))

            # gather state info
            if host not in self._summary:
                self._summary[host] = {}
            state_data = state.get().copy()
            self._summary[host][port] = state_data

            # write state info
            self.write_state(state_data, state_file)
        logging.info("Stopped all oplog threads")
        return self._summary

    def close(self):
        self.stop(True)
