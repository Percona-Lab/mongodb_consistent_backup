import os
import logging
import sys

from multiprocessing import Process
from select import select
from shutil import rmtree
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import is_datetime
from mongodb_consistent_backup.Oplog import Oplog


# noinspection PyStringFormat
class MongodumpThread(Process):
    def __init__(self, state, uri, timer, user, password, authdb, base_dir, binary, version,
                 threads=0, dump_gzip=False, verbose=False):
        Process.__init__(self)
        self.state     = state
        self.uri       = uri
        self.timer     = timer
        self.user      = user
        self.password  = password
        self.authdb    = authdb
        self.base_dir  = base_dir
        self.binary    = binary
        self.version   = version
        self.threads   = threads
        self.dump_gzip = dump_gzip
        self.verbose   = verbose

        self.timer_name        = "%s-%s" % (self.__class__.__name__, self.uri.replset)
        self.exit_code         = 1
        self._command          = None
        self.do_stdin_passwd   = False
        self.stdin_passwd_sent = False

        self.backup_dir = os.path.join(self.base_dir, self.uri.replset)
        self.dump_dir   = os.path.join(self.backup_dir, "dump")
        self.oplog_file = os.path.join(self.dump_dir, "oplog.bson")

        signal(SIGINT, SIG_IGN)
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
            elif is_datetime(line):
                return None
            return "%s:\t%s" % (self.uri, line) 
        except:
            return None

    def is_password_prompt(self, line):
        if self.do_stdin_passwd and ("Enter Password:" in line or "reading password from standard input" in line):
            return True
        return False

    def handle_password_prompt(self):
        if self.do_stdin_passwd and not self.stdin_passwd_sent:
            logging.debug("Received password prompt from mongodump, writing password to stdin")
            self._process.stdin.write(self.password + "\n")
            self._process.stdin.flush()
            self.stdin_passwd_sent = True

    def wait(self):
        try:
            while self._process.stderr:
                poll = select([self._process.stderr.fileno()], [], [])
                if len(poll) >= 1:
                    for fd in poll[0]:
                        read = self._process.stderr.readline()
                        line = self.parse_mongodump_line(read)
                        if not line:
                            continue
                        elif self.is_password_prompt(read):
                            self.handle_password_prompt()
                        else:
                            logging.info(line)
                if self._process.poll() != None:
                    break
        except Exception, e:
            logging.exception("Error reading mongodump output: %s" % e)
        finally:
            self._process.communicate()

    def mongodump_cmd(self):
        mongodump_uri   = self.uri.get()
        mongodump_cmd   = [self.binary]
        mongodump_flags = ["--host", mongodump_uri.host, "--port", str(mongodump_uri.port), "--oplog", "--out", "%s/dump" % self.backup_dir]
        if self.threads > 0:
            mongodump_flags.extend(["--numParallelCollections="+str(self.threads)])
        if self.dump_gzip:
            mongodump_flags.extend(["--gzip"])
        if tuple("3.4.0".split(".")) <= tuple(self.version.split(".")):
            mongodump_flags.extend(["--readPreference=secondary"])
        if self.authdb and self.authdb != "admin":
            logging.debug("Using database %s for authentication" % self.authdb)
            mongodump_flags.extend(["--authenticationDatabase", self.authdb])
        if self.user and self.password:
            # >= 3.0.2 supports password input via stdin to mask from ps
            if tuple("3.0.2".split(".")) <= tuple(self.version.split(".")):
                mongodump_flags.extend(["-u", self.user, "-p", '""'])
                self.do_stdin_passwd = True
            else:
                logging.warning("Mongodump is too old to set password securely! Upgrade to mongodump >= 3.2.0 to resolve this") 
                mongodump_flags.extend(["-u", self.user, "-p", self.password])
        mongodump_cmd.extend(mongodump_flags)
        return mongodump_cmd

    def run(self):
        logging.info("Starting mongodump backup of %s" % self.uri)

        self.timer.start(self.timer_name)
        self.state.set('running', True)
        self.state.set('file', self.oplog_file)

        mongodump_cmd = self.mongodump_cmd()
        try:
            if os.path.isdir(self.dump_dir):
                rmtree(self.dump_dir)
            os.makedirs(self.dump_dir)
            logging.debug("Running mongodump cmd: %s" % mongodump_cmd)
            self._process = Popen(mongodump_cmd, stdin=PIPE, stderr=PIPE)
            self.wait()
            self.exit_code = self._process.returncode
            if self.exit_code > 0:
                sys.exit(self.exit_code)
        except Exception, e:
            logging.exception("Error performing mongodump: %s" % e)

        try:
            oplog = Oplog(self.oplog_file, self.dump_gzip)
            oplog.load()
        except Exception, e:
            logging.exception("Error loading oplog: %s" % e)

        self.state.set('running', False)
        self.state.set('completed', True)
        self.state.set('count', oplog.count())
        self.state.set('first_ts', oplog.first_ts())
        self.state.set('last_ts', oplog.last_ts())
        self.timer.stop(self.timer_name)

        log_msg_extra = "%i oplog changes" % oplog.count()
        if oplog.last_ts():
            log_msg_extra = "%s, end ts: %s" % (log_msg_extra, oplog.last_ts())
        logging.info("Backup %s completed in %.2f seconds, %s" % (self.uri, self.timer.duration(self.timer_name), log_msg_extra))
