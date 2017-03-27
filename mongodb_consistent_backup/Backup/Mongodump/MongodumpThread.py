import os
import logging
import sys

from multiprocessing import Process
from select import select
from signal import signal, SIGINT, SIGTERM
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import Timer
from mongodb_consistent_backup.Errors import Error
from mongodb_consistent_backup.Oplog import Oplog


# noinspection PyStringFormat
class MongodumpThread(Process):
    def __init__(self, state, uri, user, password, authdb, base_dir, binary,
                 threads=0, dump_gzip=False, verbose=False):
        Process.__init__(self)
        self.state       = state
        self.uri         = uri
        self.user        = user
        self.password    = password
        self.authdb      = authdb
        self.base_dir    = base_dir
        self.binary      = binary
        self.threads     = threads
        self.dump_gzip   = dump_gzip
        self.verbose     = verbose

        self.exit_code  = 1
        self.timer      = Timer()
        self._command   = None
        self.backup_dir = "%s/%s" % (self.base_dir, self.uri.replset)
        self.dump_dir   = "%s/dump" % self.backup_dir
        self.oplog_file = "%s/oplog.bson" % self.dump_dir

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Stopping running subprocess/command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()
        sys.exit(self.exit_code)

    def parse_mongodump_line(self, line):
        try:
            line = line.rstrip()
	    if line == "":
	        return None
            if "\t" in line:
                (date, line) = line.split("\t")
	    return "%s:\t%s" % (self.uri, line) 
        except:
            return None

    def wait(self):
        try:
            while self._process.stderr:
                poll = select([self._process.stderr.fileno()], [], [])
                if len(poll) >= 1:
                    for fd in poll[0]:
                        read = self._process.stderr.readline()
                        line = self.parse_mongodump_line(read)
                        if line:
			    logging.info(line)
                if self._process.poll() != None:
                    break
        except Exception, e:
            raise Error(e)
        finally:
            self._process.communicate()

    def run(self):
        logging.info("Starting mongodump (with oplog) backup of %s" % self.uri)

        self.timer.start()
        self.state.set('running', True)
        self.state.set('file', self.oplog_file)

        mongodump_cmd   = [self.binary]
        mongodump_flags = ["--host", self.uri.host, "--port", str(self.uri.port), "--oplog", "--out", "%s/dump" % self.backup_dir]
        if self.threads > 0:
            mongodump_flags.extend(["--numParallelCollections="+str(self.threads)])
        if self.dump_gzip:
            mongodump_flags.extend(["--gzip"])
        if self.authdb and self.authdb != "admin":
            logging.debug("Using database %s for authentication" % self.authdb)
            mongodump_flags.extend(["--authenticationDatabase", self.authdb])
        if self.user and self.password:
            mongodump_flags.extend(["-u", self.user, "-p", self.password])
        mongodump_cmd.extend(mongodump_flags)

        try:
            if os.path.isdir(self.dump_dir):
                os.removedirs(self.dump_dir)
            os.makedirs(self.dump_dir)
            self._process = Popen(mongodump_cmd, stderr=PIPE)
            self.wait()
            self.exit_code = self._process.returncode
        except Exception, e:
            logging.error("Error performing mongodump: %s" % e)
            raise e

        oplog = Oplog(self.oplog_file, self.dump_gzip)
        oplog.load()

        self.state.set('running', False)
        self.state.set('count', oplog.count())
        self.state.set('first_ts', oplog.first_ts())
        self.state.set('last_ts', oplog.last_ts())
        self.timer.stop()

        log_msg_extra = "%i oplog changes" % oplog.count()
        if oplog.last_ts():
            log_msg_extra = "%s, end ts: %s" % (log_msg_extra, oplog.last_ts())
        logging.info("Backup %s completed in %.2f seconds, %s" % (self.uri, self.timer.duration(), log_msg_extra))

        sys.exit(self.exit_code)
