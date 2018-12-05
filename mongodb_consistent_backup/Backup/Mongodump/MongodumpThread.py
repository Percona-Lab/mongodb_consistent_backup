import json
import os
import logging
import sys

from multiprocessing import Process
from select import select
from shutil import rmtree
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import is_datetime, parse_config_bool, parse_read_pref_tags
from mongodb_consistent_backup.Oplog import Oplog


# noinspection PyStringFormat
class MongodumpThread(Process):
    def __init__(self, state, uri, timer, config, base_dir, version, threads=0, dump_gzip=False):
        Process.__init__(self)
        self.state     = state
        self.uri       = uri
        self.timer     = timer
        self.config    = config
        self.base_dir  = base_dir
        self.version   = version
        self.threads   = threads
        self.dump_gzip = dump_gzip

        self.user                 = self.config.username
        self.password             = self.config.password
        self.authdb               = self.config.authdb
        self.ssl_ca_file          = self.config.ssl.ca_file
        self.ssl_crl_file         = self.config.ssl.crl_file
        self.ssl_client_cert_file = self.config.ssl.client_cert_file
        self.read_pref_tags       = self.config.replication.read_pref_tags
        self.binary               = self.config.backup.mongodump.binary

        self.timer_name        = "%s-%s" % (self.__class__.__name__, self.uri.replset)
        self.exit_code         = 1
        self.error_message     = None
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

    def do_ssl(self):
        return parse_config_bool(self.config.ssl.enabled)

    def do_ssl_insecure(self):
        return parse_config_bool(self.config.ssl.insecure)

    def is_version_gte(self, compare):
        if os.path.isfile(self.binary) and os.access(self.binary, os.X_OK):
            if tuple(compare.split(".")) <= tuple(self.version.split(".")):
                return True
        return False

    def parse_read_pref(self, mode="secondary"):
        rp = {"mode": mode}
        if self.read_pref_tags:
            rp["tags"] = parse_read_pref_tags(self.read_pref_tags)
        return json.dumps(rp, separators=(',', ':'))

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
        except Exception:
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

    def is_failed_line(self, line):
        if line and line.startswith("Failed: "):
            return True
        return False

    def handle_failure(self, line):
        self.error_message = line.replace("Failed: ", "").capitalize()
        logging.error("Mongodump error: %s" % self.error_message)
        self.exit_code = 1
        self.close()

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
                        elif self.is_failed_line(read):
                            self.handle_failure(read)
                            break
                        else:
                            logging.info(line)
                if self._process.poll() is not None:
                    break
        except Exception, e:
            logging.exception("Error reading mongodump output: %s" % e)
        finally:
            self._process.communicate()

    def mongodump_cmd(self):
        mongodump_uri   = self.uri.get()
        mongodump_cmd   = [self.binary]
        mongodump_flags = []

        # --host/--port (suport mongodb+srv:// too)
        if self.uri.srv:
            if not self.is_version_gte("3.6.0"):
                logging.fatal("Mongodump must be >= 3.6.0 to use mongodb+srv:// URIs")
                sys.exit(1)
            mongodump_flags.append("--host=%s" % self.uri.url)
        else:
            mongodump_flags.extend([
                "--host=%s" % mongodump_uri.host,
                "--port=%s" % str(mongodump_uri.port)
            ])

        mongodump_flags.extend([
            "--oplog",
            "--out=%s/dump" % self.backup_dir
        ])

        # --numParallelCollections
        if self.threads > 0:
            mongodump_flags.append("--numParallelCollections=%s" % str(self.threads))

        # --gzip
        if self.dump_gzip:
            mongodump_flags.append("--gzip")

        # --readPreference
        if self.is_version_gte("3.2.0"):
            read_pref = self.parse_read_pref()
            if read_pref:
                mongodump_flags.append("--readPreference=%s" % read_pref)
        elif self.read_pref_tags:
            logging.fatal("Mongodump must be >= 3.2.0 to set read preference!")
            sys.exit(1)

        # --username/--password/--authdb
        if self.authdb and self.authdb != "admin":
            logging.debug("Using database %s for authentication" % self.authdb)
            mongodump_flags.append("--authenticationDatabase=%s" % self.authdb)
        if self.user and self.password:
            # >= 3.0.2 supports password input via stdin to mask from ps
            if self.is_version_gte("3.0.2"):
                mongodump_flags.extend([
                    "--username=%s" % self.user,
                    "--password=\"\""
                ])
                self.do_stdin_passwd = True
            else:
                logging.warning("Mongodump is too old to set password securely! Upgrade to mongodump >= 3.0.2 to resolve this")
                mongodump_flags.extend([
                    "--username=%s" % self.user,
                    "--password=%s" % self.password
                ])

        # --ssl
        if self.do_ssl():
            if self.is_version_gte("2.6.0"):
                mongodump_flags.append("--ssl")
                if self.ssl_ca_file:
                    mongodump_flags.append("--sslCAFile=%s" % self.ssl_ca_file)
                if self.ssl_crl_file:
                    mongodump_flags.append("--sslCRLFile=%s" % self.ssl_crl_file)
                if self.ssl_client_cert_file:
                    mongodump_flags.append("--sslPEMKeyFile=%s" % self.ssl_client_cert_file)
                if self.do_ssl_insecure():
                    mongodump_flags.extend(["--sslAllowInvalidCertificates", "--sslAllowInvalidHostnames"])
            else:
                logging.fatal("Mongodump must be >= 2.6.0 to enable SSL encryption!")
                sys.exit(1)

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
            logging.debug("Running mongodump cmd: %s" % " ".join(mongodump_cmd))
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
