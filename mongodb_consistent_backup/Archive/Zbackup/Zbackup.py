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
        self.backup_time         = os.path.basename(self.backup_dir)
        self.zbackup_binary      = self.config.archive.zbackup.binary
        self.zbackup_cache_mb    = self.config.archive.zbackup.cache_mb
        self.zbackup_passwd_file = self.config.archive.zbackup.password_file

        if self.config.archive.zbackup.threads and self.config.archive.zbackup.threads > 0:
            self.threads(self.config.archive.zbackup.threads)

        self.zbackup_dir         = os.path.join(self.config.backup.location, self.backup_name, "zbackup")
        self.zbackup_backups     = os.path.join(self.zbackup_dir, "backups")
        self.zbackup_backup_path = os.path.join(self.zbackup_backups, "%s.tar" % self.backup_time)
        self.zbackup_bundles     = os.path.join(self.zbackup_dir, "bundles")
        self.zbackup_info        = os.path.join(self.zbackup_dir, "info")

        self.encrypted          = False
        self._zbackup           = None
        self._tar               = None
        self._version           = None

        self.init()

    def compression(self, method=None):
        return 'lzma'

    def is_zbackup_init(self):
        if os.path.isfile(self.zbackup_info) and os.path.isdir(self.zbackup_backups) and os.path.isdir(self.zbackup_bundles):
            return True
        return False

    def init(self):
        if os.path.isdir(self.zbackup_dir):
            if self.is_zbackup_init():
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
                if cmd.returncode != 0:
                    raise OperationError("ZBackup initialization failed! Error: %s" % stdout)
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
            if self._zbackup and self._zbackup.poll() == None:
                logging.debug("Stopping running ZBackup command")
                self._zbackup.terminate()
            if self._tar and self._tar.poll() == None:
                logging.debug("Stopping running ZBackup tar command")
                self._tar.terminate()
            self.stopped = True

    def wait(self):
        try:
            tar_done = False
            while self._zbackup.stderr and self._tar.stderr:
                try:
                    poll = select([self._zbackup.stderr.fileno()], [], [], 1)
                except ValueError:
                    break
                if len(poll) >= 1:
                    for fd in poll[0]:
                        line = self._zbackup.stderr.readline()
                        if line:
                            logging.info(line.rstrip())
                if tar_done:
                    self._zbackup.communicate()
                    if self._zbackup.poll() != None:
                        logging.info("ZBackup completed successfully with exit code: %i" % self._zbackup.returncode)
                        self.running   = False
                        self.stopped   = True
                        self.exit_code = self._zbackup.returncode
                        if self.exit_code == 0:
                            self.completed = True
                        break
                elif self._tar.poll() != None:
                    if self._tar.returncode == 0:
                        logging.debug("ZBackup tar command completed successfully with exit code: %i" % self._tar.returncode)
                        tar_done = True
                    else:
                        raise OperationError("ZBackup archiving failed on tar command with exit code: %i" % self._tar.returncode)
        except Exception, e:
            raise OperationError("Error reading ZBackup output: %s" % e)

    def run(self):
        if self.has_zbackup():
            try:
                tar_cmd_line     = ["tar", "--exclude", "mongodb-consistent-backup_META", "-C", self.backup_dir, "-c", "."]
                zbackup_cache_mb = str(self.zbackup_cache_mb) + "mb"
                zbackup_cmd_line = [self.zbackup_binary, "--cache-size", zbackup_cache_mb, "--compression", self.compression()]
                if self.encrypted:
                    zbackup_cmd_line.extend(["--password-file", self.zbackup_passwd_file, "backup", self.zbackup_backup_path])
                else:
                    zbackup_cmd_line.extend(["--non-encrypted", "backup", self.zbackup_backup_path])
                logging.info("Starting ZBackup version: %s (options: compression=%s, encryption=%s, threads=%i, cache_mb=%i)" %
                    (self.version(), self.compression(), self.encrypted, self.threads(), self.zbackup_cache_mb)
                )
                logging.debug("Running ZBackup tar command: %s" % tar_cmd_line)
                logging.debug("Running ZBackup command: %s" % zbackup_cmd_line)
                self._zbackup = Popen(zbackup_cmd_line, stdin=PIPE, stderr=PIPE)
                self._tar     = Popen(tar_cmd_line, stdout=self._zbackup.stdin, stderr=PIPE)
                self.running  = True
                self.wait()
            except Exception, e:
                raise e #OperationError("Could not execute ZBackup: %s" % e)
        else:
            raise OperationError("Cannot find ZBackup at %s!" % self.zbackup_binary)
