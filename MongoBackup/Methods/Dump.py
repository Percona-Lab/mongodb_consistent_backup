import os
import logging

from multiprocessing import Process
from signal import signal, SIGINT, SIGTERM
from time import time

from MongoBackup.Common import LocalCommand
from MongoBackup.Oplog import OplogInfo


# noinspection PyStringFormat
class Dump(Process):
    def __init__(self, response_queue, backup_name, host_port, user, password, authdb, base_dir, binary,
                 dump_gzip=False, verbose=False):
        Process.__init__(self)
        self.host, port     = host_port.split(":")
        self.host_port      = host_port
        self.port           = int(port)
        self.response_queue = response_queue
        self.backup_name    = backup_name
        self.user           = user
        self.password       = password
        self.authdb         = authdb
        self.base_dir       = base_dir
        self.binary         = binary
        self.dump_gzip      = dump_gzip
        self.verbose        = verbose

        self._command   = None
        self.completed  = False 
        self.backup_dir = "%s/%s" % (self.base_dir, self.backup_name)
        self.dump_dir   = "%s/dump" % self.backup_dir
        self.oplog_file = "%s/oplog.bson" % self.dump_dir
        self.start_time = time()

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Killing running subprocess/command: %s" % self._command.command)
            self._command.close()

    def run(self):
        logging.info("Starting mongodump (with oplog) backup of %s/%s:%i" % (
            self.backup_name,
            self.host,
            self.port
        ))

        mongodump_flags = ["-h", self.host_port, "--oplog", "-o", "%s/dump" % self.backup_dir]
        if self.dump_gzip:
            mongodump_flags.extend(["--gzip"])
        if self.authdb and self.authdb != "admin":
            logging.debug("Using database %s for authentication" % self.authdb)
            mongodump_flags.extend(["--authenticationDatabase", self.authdb])
        if self.user and self.password:
            mongodump_flags.extend(["-u", self.user, "-p", self.password])

        try:
            commands = []
            if os.path.isdir(self.dump_dir):
                commands.append(["rm", ["-rf", self.dump_dir]])
            commands.append(["mkdir", ["-p", self.dump_dir]])
            commands.append([self.binary, mongodump_flags])

            for (command, command_flags) in commands:
                self._command = LocalCommand(command, command_flags, self.verbose)
                self._command.run()
        except Exception, e:
            logging.error("Error performing mongodump: %s" % e)
            return None

        oplog = OplogInfo(self.oplog_file, self.dump_gzip)
        self.completed = True
        self.response_queue.put({
            'host': self.host,
            'port': self.port,
            'file': self.oplog_file,
            'count': oplog.count(),
            'last_ts': oplog.last_ts(),
            'first_ts': oplog.first_ts(),
            'completed': self.completed
        })

        time_diff = time() - self.start_time
        logging.info("Backup for %s/%s:%s completed in %s sec with %i oplog changes captured to: %s" % (
            self.backup_name, self.host, self.port, time_diff, oplog.count(), str(oplog.last_ts())
        ))
