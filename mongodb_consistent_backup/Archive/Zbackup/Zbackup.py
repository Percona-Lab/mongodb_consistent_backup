import os
import logging

from select import select
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import LocalCommand
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task


class Zbackup(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(Zbackup, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.backup_name         = self.config.backup.name
        self.zbackup_binary      = self.config.archive.zbackup.binary
        self.zbackup_passwd_file = self.config.archive.zbackup.password_file
        self.zbackup_threads     = self.config.archive.zbackup.threads
        self.zbackup_dir         = os.path.join(self.config.backup.location, self.backup_name, "zbackup")
        self.zbackup_backups     = os.path.join(self.zbackup_dir, "backups")
        self.zbackup_backup_path = os.path.join(self.zbackup_backups, "%s.tar" % self.backup_name)
        self.zbackup_bundles     = os.path.join(self.zbackup_dir, "bundles")
        self.zbackup_info        = os.path.join(self.zbackup_dir, "info")

        self.compression_method = 'lzma'
        self.encrypted          = False
        self._zbackup           = None
        self._tar               = None
        self._version           = None

        self.init()

    def is_zbackup_dir(self):
        if os.path.isfile(self.zbackup_info) and os.path.isdir(self.zbackup_backups) and os.path.isdir(self.zbackup_bundles):
            return True
        return False

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
        del exit_code
        del frame
        if not self.stopped:
            if self._zbackup and not self._zbackup.poll():
                logging.debug("Stopping running Zbackup command")
                self._zbackup.terminate()
            if self._tar and not self._tar.poll():
                logging.debug("Stopping running Zbackup tar command")
                self._tar.terminate()
            self.stopped = True

    def wait(self):
        try:
            while self._zbackup.stderr:
                poll = select([self._zbackup.stderr.fileno()], [], [], 1)
                if len(poll) >= 1:
                    for fd in poll[0]:
                        line = self._zbackup.stderr.readline()
                        if line:
                            logging.info(line.rstrip())
		if self._zbackup.poll() != None and self._tar.poll() != None:
                    break
        except Exception, e:
            logging.exception("Error reading ZBackup output: %s" % e)
        finally:
            self._zbackup.communicate()

    def run(self):
        if self.has_zbackup():
            zbackup_cmd_line = [self.zbackup_binary]
            if self.encrypted:
                zbackup_cmd_line.extend(["--password-file", self.zbackup_passwd_file, "backup", self.zbackup_backup_path])
            else:
                zbackup_cmd_line.extend(["--non-encrypted", "backup", self.zbackup_backup_path])
            logging.info("Starting ZBackup version: %s" % self.version())
            self._zbackup = Popen(zbackup_cmd_line, stdin=PIPE, stderr=PIPE)
            self._tar     = Popen(["tar", "-c", self.backup_dir], stdout=self._zbackup.stdin, stderr=PIPE)
            self._tar.communicate()
            self.wait()
            self.exit_code = self._zbackup.returncode
            if self.exit_code == 0:
                self.completed = True
        else:
            raise OperationError("Cannot find Zbackup at %s!" % self.zbackup_binary)
