import logging
import os

from shutil import rmtree
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import wait_popen


class RsyncUploadThread:
    def __init__(self, src_path, base_path, rsync_flags, rsync_path, rsync_user, rsync_host,
                 rsync_port=22, rsync_ssh_key=None, remove_uploaded=False, retries=5,
                 rsync_binary="rsync"):
        self.src_path        = src_path
        self.base_path       = base_path
        self.rsync_flags     = rsync_flags
        self.rsync_path      = rsync_path
        self.rsync_user      = rsync_user
        self.rsync_host      = rsync_host
        self.rsync_port      = rsync_port
        self.rsync_ssh_key   = rsync_ssh_key
        self.remove_uploaded = remove_uploaded
        self.retries         = retries
        self.rsync_binary    = rsync_binary

        self.completed = False
        self.rsync_url = None
        self.rsync_cmd = None
        self.meta_dir  = "mongodb-consistent-backup_META"

    def init(self):
        self.rsync_url = "%s@%s:%s" % (self.rsync_user, self.rsync_host, self.get_dest_path())
        self.rsync_cmd = [self.rsync_binary]
        self.rsync_cmd.extend(self.rsync_flags)
        self.rsync_cmd.extend([self.src_path, self.rsync_url])

    def get_dest_path(self):
        return os.path.join(self.rsync_path, self.base_path)

    def handle_success(self):
        if self.remove_uploaded:
            if self.meta_dir in self.src_path:
                logging.info("Skipping removal of metadata path: %s" % self.src_path)
            else:
                logging.info("Removing uploaded path: %s" % self.src_path)
                rmtree(self.src_path)

    def stderr(self, data):
        if data:
            logging.error(data)

    def stdout(self, data):
        if data:
            logging.info(data)

    def do_rsync(self):
        # do the rsync
        self._command = Popen(self.rsync_cmd, stderr=PIPE, stdout=PIPE)
        wait_popen(self._command, self.stderr, self.stdout)

    def run(self):
        self.init()
        try:
            logging.info("Uploading to %s" % (self.rsync_url))
            logging.debug("Rsync cmd: %s" % self.rsync_cmd)
            self._command  = Popen(self.rsync_cmd, stderr=PIPE, stdout=PIPE)
            self.completed = wait_popen(self._command, self.stderr, self.stdout)

            if self.completed:
                self.handle_success()
        finally:
            self.close()
            return self.completed, self.src_path

    def close(self, code=None, frame=None):
        logging.info("Stopping upload to %s@%s:%s" % (
            self.rsync_user,
            self.rsync_host,
            self.dest_path
        ))
        if not self.completed and self._command:
            self._command.terminate()
