import os
import logging

from select import select
from subprocess import Popen, PIPE, call

from mongodb_consistent_backup.Common import Lock
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

        self.threads(self.config.archive.zbackup.threads)

        # only lzma compression supported (for now)
        self.compression_method    = 'lzma'
        self.compression_supported = ['lzma']

        self.base_dir            = os.path.join(self.config.backup.location, self.backup_name)
        self.zbackup_dir         = os.path.join(self.base_dir, "mongodb-consistent-backup_zbackup")
        self.zbackup_lock        = os.path.join(self.base_dir, "mongodb-consistent-backup_zbackup.lock")
        self.zbackup_backups     = os.path.join(self.zbackup_dir, "backups")
        self.zbackup_backup_path = os.path.join(self.zbackup_backups, "%s.tar" % self.backup_time)
        self.zbackup_bundles     = os.path.join(self.zbackup_dir, "bundles")
        self.zbackup_info        = os.path.join(self.zbackup_dir, "info")
        self.backup_meta_dir     = "mongodb-consistent-backup_META"

        self.encrypted = False
        self._zbackup  = None
        self._tar      = None
        self._version  = None

        self.init()

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
            if not os.path.isdir(self.base_dir):
                os.makedirs(self.base_dir)
            lock = Lock(self.zbackup_lock)
            lock.acquire()
            try:
                cmd_line = [self.zbackup_binary]
                if self.zbackup_passwd_file:
                    cmd_line.extend(["--password-file", self.zbackup_passwd_file, "init", self.zbackup_dir])
                    logging.info("Using ZBackup AES encryption with password file: %s" % self.zbackup_passwd_file)
                    self.encrypted = True
                else:
                    cmd_line.extend(["--non-encrypted", "init", self.zbackup_dir])
                logging.warning("Initializing new ZBackup storage directory at: %s (encrypted: %s)" % (self.zbackup_dir, self.encrypted))
                logging.debug("Using ZBackup command: '%s'" % cmd_line)
                exit_code = call(cmd_line)
                if exit_code != 0:
                    raise OperationError("ZBackup initialization failed! Exit code: %i" % exit_code)
            except Exception, e:
                raise OperationError("Error creating ZBackup storage directory! Error: %s" % e)
            finally:
                lock.release()

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
            if self._zbackup and self._zbackup.poll() is None:
                logging.debug("Stopping running ZBackup command")
                self._zbackup.terminate()
            if self._tar and self._tar.poll() is None:
                logging.debug("Stopping running ZBackup tar command")
                self._tar.terminate()
            self.stopped = True

    def poll(self, timeout=1):
        try:
            poll = select([self._zbackup.stderr.fileno()], [], [], timeout)
        except ValueError:
            return
        if len(poll) >= 1:
            for fd in poll[0]:
                line = self._zbackup.stderr.readline()
                if line:
                    logging.info(line.rstrip())

    def wait(self):
        try:
            tar_done = False
            while self._zbackup.stderr and self._tar.stderr:
                self.poll()
                if tar_done:
                    self._zbackup.communicate()
                    if self._zbackup.poll() is not None:
                        logging.info("ZBackup completed successfully with exit code: %i" % self._zbackup.returncode)
                        if self._zbackup.returncode != 0:
                            raise OperationError("ZBackup exited with code: %i!" % self._zbackup.returncode)
                        break
                elif self._tar.poll() is not None:
                    if self._tar.returncode == 0:
                        logging.debug("ZBackup tar command completed successfully with exit code: %i" % self._tar.returncode)
                        tar_done = True
                    else:
                        raise OperationError("ZBackup archiving failed on tar command with exit code: %i" % self._tar.returncode)
        except Exception, e:
            raise OperationError("Error reading ZBackup output: %s" % e)

    def get_commands(self, base_dir, sub_dir):
        tar          = ["tar", "--remove-files", "-C", base_dir, "-c", sub_dir]
        zbackup      = [self.zbackup_binary, "--cache-size", "%imb" % self.zbackup_cache_mb, "--compression", self.compression()]
        zbackup_path = os.path.join(self.zbackup_backups, "%s.%s.tar" % (self.backup_time, sub_dir))
        if self.encrypted:
            zbackup.extend(["--password-file", self.zbackup_passwd_file, "backup", zbackup_path])
        else:
            zbackup.extend(["--non-encrypted", "backup", zbackup_path])
        return tar, zbackup

    def run(self):
        if self.has_zbackup():
            lock = Lock(self.zbackup_lock)
            lock.acquire()
            try:
                logging.info("Starting ZBackup version: %s (options: compression=%s, encryption=%s, threads=%i, cache_mb=%i)" % (
                    self.version(), self.compression(), self.encrypted, self.threads(), self.zbackup_cache_mb
                ))
                self.running = True
                try:
                    for sub_dir in os.listdir(self.backup_dir):
                        if sub_dir == self.backup_meta_dir:
                            continue
                        logging.info("Running ZBackup for path: %s" % os.path.join(self.backup_dir, sub_dir))
                        tar_cmd, zbkp_cmd = self.get_commands(self.backup_dir, sub_dir)
                        logging.debug("Running ZBackup tar command: %s" % tar_cmd)
                        logging.debug("Running ZBackup command: %s" % zbkp_cmd)
                        self._zbackup = Popen(zbkp_cmd, stdin=PIPE, stderr=PIPE)
                        self._tar     = Popen(tar_cmd, stdout=self._zbackup.stdin, stderr=PIPE)
                        self.wait()
                except Exception, e:
                    raise OperationError("Could not execute ZBackup: %s" % e)
                logging.info("Completed running all ZBackups")
                self.completed = True
            finally:
                self.running = False
                self.stopped = True
                lock.release()
        else:
            raise OperationError("Cannot find ZBackup at %s!" % self.zbackup_binary)
