import os
import logging

from multiprocessing import cpu_count
from signal import signal, SIGINT, SIGTERM
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import LocalCommand


class Zbackup:
    def __init__(self, config, source_dir):
        self.config     = config
        self.source_dir = source_dir

        self.zbackup_binary      = config.archive.zbackup.binary
        self.zbackup_backup_dir  = config.archive.zbackup.backup_dir
        self.zbackup_passwd_file = config.archive.zbackup.password_file
        self.zbackup_threads     = config.archive.zbackup.threads

        self.encrypted          = False
        self.compression_method = None
        self.compression_supported = ['lzma']

        self._command  = None
        self._version  = None

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

        self.init()

    def compression(self, method=None):
        # only lzma supported
        return 'lzma'

    def threads(self, count=None):
        if count:
            self.config.archive.zbackup.threads = int(count)
	if self.config.archive.zbackup.threads < 1:
	    self.config.archive.zbackup.threads = cpu_count()
        return int(self.config.archive.zbackup.threads)

    def backup_dir(self, backup_dir=None):
        if backup_dir:
	    self.config.archive.zbackup.backup_dir = backup_dir
	return self.config.archive.zbackup.backup_dir

    def init_storage_dir(self):
        if os.path.isdir(self.backup_dir()):
            if os.path.isfile("%s/info" % self.backup_dir()) and os.path.isdir("%s/bundles" % self.backup_dir()):
		logging.info("Found existing ZBackup storage dir at: %s (encrypted: %s)" % (self.backup_dir(), self.encrypted))
            else:
	        raise Exception, "ZBackup dir: %s is not a zbackup storage directory!" % self.backup_dir(), None
        else:
            try:
                cmd_line = [self.zbackup_binary]
                if self.zbackup_passwd_file:
                    self.encrypted = True
                    cmd_line.extend(["--password-file", self.zbackup_passwd_file, "init", self.backup_dir()])
		    logging.info("Using ZBackup encryption")
                else:
                    cmd_line.extend(["--non-encrypted", "init", self.backup_dir()])
		logging.warning("Initializing new ZBackup storage directory at: %s (encrypted: %s)" % (self.backup_dir(), self.encrypted))
                logging.debug("Using ZBackup command: '%s'" % cmd_line)
                cmd = Popen(cmd_line, stdout=PIPE)
                stdout, stderr = cmd.communicate()
                if cmd.returncode == 0:
                    logging.info("Initialization complete, stdout:\n%s" % stdout)
            except Exception, e:
		logging.error("Error creating ZBackup storage directory! Error: %s" % e)
		raise e

    def init(self):
        if not self.backup_dir() and self.config.backup.location:
            if not os.path.isdir(self.config.backup.location):
                try:
                    os.mkdir(self.config.backup.location)
                except Exception, e:
                    raise Exception, "Error making backup base dir: %s" % e, None
            self.backup_dir(os.path.join(self.config.backup.location, 'zbackup'))
	self.init_storage_dir()

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
                raise e
    
    def has_zbackup(self):
        if self.version():
           return True
        return False

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Killing running Zbackup command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()

    def run(self):
        if self.has_zbackup():
	    backup_name = os.path.basename(self.source_dir)
            tar_cmd_line = ["tar", "c", self.source_dir]
	    zbackup_cmd_line = [self.zbackup_binary]
	    if self.encrypted:
                zbackup_cmd_line.extend(["--password-file", self.zbackup_passwd_file, "backup", "%s/backups/%s.tar" % (self.backup_dir(), backup_name)])
	    else:
		zbackup_cmd_line.extend(["--non-encrypted", "backup", "%s/backups/%s.tar" % (self.backup_dir(), backup_name)])
            logging.info("Starting ZBackup version: %s" % self.version())
            z_cmd = Popen(zbackup_cmd_line, stdin=PIPE, stdout=PIPE)
	    t_cmd = Popen(tar_cmd_line, stdout=z_cmd.stdin, stderr=PIPE)
	    t_stdout, t_stderr = t_cmd.communicate()
        else:
            logging.error("Cannot find Zbackup at %s!" % self.zbackup_binary)
            raise Exception, "Cannot find Zbackup at %s!" % self.zbackup_binary, None
