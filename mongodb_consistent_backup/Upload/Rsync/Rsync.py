import os
import logging
import re

from copy_reg import pickle
from multiprocessing import Pool
from subprocess import check_output
from types import MethodType

from RsyncUploadThread import RsyncUploadThread

from mongodb_consistent_backup.Common import config_to_string
from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


pickle(MethodType, _reduce_method)


class Rsync(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(Rsync, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.backup_location = self.config.backup.location
        self.backup_name     = self.config.backup.name
        self.remove_uploaded = self.config.upload.remove_uploaded
        self.retries         = self.config.upload.retries
        self.rsync_path      = self.config.upload.rsync.path
        self.rsync_user      = self.config.upload.rsync.user
        self.rsync_host      = self.config.upload.rsync.host
        self.rsync_port      = self.config.upload.rsync.port
        self.rsync_ssh_key   = self.config.upload.rsync.ssh_key
        self.rsync_binary    = "rsync"

        self.rsync_flags   = ["--archive", "--compress"]
        self.rsync_version = None
        self._rsync_info   = None

        self.threads(self.config.upload.threads)
        self._pool = Pool(processes=self.threads())

    def init(self):
        if not self.host_has_rsync():
            raise OperationError("Cannot find rsync binary on this host!")
        if not os.path.isdir(self.backup_dir):
            logging.error("The source directory: %s does not exist or is not a directory! Skipping Rsync upload!" % self.backup_dir)
            raise OperationError("The source directory: %s does not exist or is not a directory! Skipping Rsync upload!" % self.backup_dir)

    def rsync_info(self):
        if not self._rsync_info:
            output = check_output([self.rsync_binary, "--version"])
            search = re.search(r"^rsync\s+version\s([0-9.-]+)\s+protocol\sversion\s(\d+)", output)
            self.rsync_version = search.group(1)
            self._rsync_info   = {"version": self.rsync_version, "protocol_version": int(search.group(2))}
        return self._rsync_info

    def host_has_rsync(self):
        if self.rsync_info():
            return True
        return False

    def get_dest_path(self):
        return os.path.join(self.rsync_path, self.base_dir)

    def prepare_dest_dir(self):
        # mkdir -p the rsync dest path via ssh
        ssh_mkdir_cmd = ["ssh"]
        if self.rsync_ssh_key:
            ssh_mkdir_cmd.extend(["-i", self.rsync_ssh_key])
        ssh_mkdir_cmd.extend([
            "%s@%s" % (self.rsync_user, self.rsync_host),
            "mkdir", "-p", self.get_dest_path()
        ])

        # run the mkdir via ssh
        try:
            check_output(ssh_mkdir_cmd)
        except Exception, e:
            logging.error("Creating rsync dest path with ssh failed for %s: %s" % (
                self.rsync_host,
                e
            ))
            raise e

        return True

    def done(self, data):
        logging.info(data)

    def run(self):
        try:
            self.init()
            self.timer.start(self.timer_name)

            logging.info("Preparing destination path on %s" % self.rsync_host)
            self.prepare_dest_dir()

            rsync_config = {
                "dest": "%s@%s:%s" % (self.rsync_user, self.rsync_host, self.get_dest_path()),
                "threads": self.threads(),
                "retries": self.retries
            }
            rsync_config.update(self.rsync_info())
            logging.info("Starting upload using rsync version %s (%s)" % (
                self.rsync_info()['version'],
                config_to_string(rsync_config)
            ))
            for child in os.listdir(self.backup_dir):
                self._pool.apply_async(RsyncUploadThread(
                    os.path.join(self.backup_dir, child),
                    self.base_dir,
                    self.rsync_flags,
                    self.rsync_path,
                    self.rsync_user,
                    self.rsync_host,
                    self.rsync_port,
                    self.rsync_ssh_key,
                    self.remove_uploaded,
                    self.retries
                ).run, callback=self.done)
            self.wait()
        except Exception, e:
            logging.error("Rsync upload failed! Error: %s" % e)
            raise OperationError(e)
        finally:
            self.timer.stop(self.timer_name)
        self.completed = True

    def wait(self):
        if self._pool:
            logging.info("Waiting for Rsync upload threads to stop")
            self._pool.close()
            self._pool.join()

    def close(self):
        if self._pool:
            logging.error("Stopping Rsync upload threads")
            self._pool.terminate()
            self._pool.join()
