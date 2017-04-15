import os
import logging

from multiprocessing import cpu_count
from signal import signal, SIGINT, SIGTERM
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import LocalCommand
from mongodb_consistent_backup.Errors import OperationError


# TODO: move to mongodb_consistent_backup.Pipeline.Task
class Zbackup:
    def __init__(self, config, source_dir):
        self.config      = config
        self.source_dir  = source_dir
        self.backup_name = self.config.backup.name

        self.zbackup_binary      = config.archive.zbackup.binary
        self.zbackup_passwd_file = config.archive.zbackup.password_file
        self.zbackup_threads     = config.archive.zbackup.threads

        self.zbackup_dir         = os.path.join(self.config.backup.location, self.backup_name, "zbackup")
        self.zbackup_backup_path = os.path.join(self.zbackup_dir, "backups", "%s.tar" % self.backup_name)
        self.zbackup_bundles     = os.path.join(self.zbackup_dir, "bundles")
        self.zbackup_info        = os.path.join(self.zbackup_dir, "info")

        self.encrypted          = False
        self.compression_method = None
        self._command           = None
        self._version           = None

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

        self.init()

    def is_zbackup_dir(self):
        if os.path.isfile(self.zbackup_info) and os.path.isdir(self.zbackup_bundles):
            return True
        return False

    def compression(self, method=None):
        # only lzma supported
        return 'lzma'

    def threads(self, count=None):
        if count:
            self.config.archive.zbackup.threads = int(count)
        if self.config.archive.zbackup.threads < 1:
            self.config.archive.zbackup.threads = cpu_count()
        return int(self.config.archive.zbackup.threads)

    def init(self):
        if os.path.isdir(self.zbackup_dir):
            if self.is_zbackup_dir():
                logging.info("Found existing ZBackup storage dir at: %s (encrypted: %s)" % (self.zbackup_dir, self.encrypted))
            else:
                raise OperationError("ZBackup dir: %s is not a zbackup storage directory!" % self.zbackup_dir)
        else:
            try:
                cmd_line = [self.zbackup_binary]
                if self.zbackup_passwd_file:
                    self.encrypted = True
                    cmd_line.extend(["--password-file", self.zbackup_passwd_file, "init", self.zbackup_dir])
                    logging.info("Using ZBackup AES encryption with password file: %s" % self.zbackup_passwd_file)
                else:
                    cmd_line.extend(["--non-encrypted", "init", self.zbackup_dir])
                logging.warning("Initializing new ZBackup storage directory at: %s (encrypted: %s)" % (self.zbackup_dir, self.encrypted))
                logging.debug("Using ZBackup command: '%s'" % cmd_line)
                cmd = Popen(cmd_line, stdout=PIPE)
                stdout, stderr = cmd.communicate()
                if cmd.returncode == 0:
                    logging.info("Initialization complete, stdout:\n%s" % stdout)
            except Exception, e:
                raise OperationError("Error creating ZBackup storage directory! Error: %s" % e)

    def version(self):
        if self._version:
            return self._version
        else:
            try:
                cmd = Popen([self.zbackup_binary, "--help"], stderr=PIPE)
                stdout, stderr = cmd.communicate()
                if stderr:
                    line = stderr.split("\n")[0]
                    if line.startswith("ZBackup") and "version " in line:
                        fields  = line.split(" ")
                        version = fields[len(fields) - 1]
                        if len(version.split(".")) == 3:
                            self._version = version
                            return self._version
                return None
            except OSError, e:
                return None
            except Exception, e:
                raise OperationError("Could not gather ZBackup version: %s" % e)
    
    def has_zbackup(self):
        if self.version():
           return True
        return False

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Stopping running Zbackup command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()

    def run(self):
        if self.has_zbackup():
            tar_cmd_line = ["tar", "-c", self.source_dir]
            zbackup_cmd_line = [self.zbackup_binary]
            if self.encrypted:
                zbackup_cmd_line.extend(["--password-file", self.zbackup_passwd_file, "backup", self.zbackup_backup_path])
            else:
                zbackup_cmd_line.extend(["--non-encrypted", "backup", self.zbackup_backup_path])
            logging.info("Starting ZBackup version: %s" % self.version())
            z_cmd = Popen(zbackup_cmd_line, stdin=PIPE, stdout=PIPE)
            t_cmd = Popen(tar_cmd_line, stdout=z_cmd.stdin, stderr=PIPE)
            t_stdout, t_stderr = t_cmd.communicate()
        else:
            raise OperationError("Cannot find Zbackup at %s!" % self.zbackup_binary)
