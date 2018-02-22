import logging
import os

from copy_reg import pickle
from multiprocessing import Pool
from types import MethodType

from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task
from mongodb_consistent_backup.Upload.Util import get_upload_files

from GsUploadThread import GsUploadThread


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


pickle(MethodType, _reduce_method)


class Gs(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(Gs, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.backup_location = self.config.backup.location
        self.remove_uploaded = self.config.upload.remove_uploaded
        self.retries         = self.config.upload.retries
        self.project_id      = self.config.upload.gs.project_id
        self.access_key      = self.config.upload.gs.access_key
        self.secret_key      = self.config.upload.gs.secret_key
        self.bucket          = self.config.upload.gs.bucket

        self.threads(self.config.upload.threads)
        self._pool = Pool(processes=self.threads())

    def close(self):
        if self.running and not self.stopped:
            self._pool.terminate()
            self.stopped = True

    def run(self):
        if not os.path.isdir(self.backup_dir):
            logging.error("The source directory: %s does not exist or is not a directory! Skipping Google Cloud Storage upload!" % self.backup_dir)
            return
        try:
            self.running = True
            self.timer.start(self.timer_name)
            logging.info("Uploading %s to Google Cloud Storage (bucket=%s, threads=%i)" % (self.base_dir, self.bucket, self.threads()))
            for file_path in get_upload_files():
                gs_path = os.path.relpath(file_path, self.backup_location)
                self._pool.apply_async(GsUploadThread(
                    self.backup_dir,
                    file_path,
                    gs_path,
                    self.bucket,
                    self.project_id,
                    self.access_key,
                    self.secret_key,
                    self.remove_uploaded,
                    self.retries
                ).run)
            self._pool.close()
            self._pool.join()
            self.exit_code = 0
            self.completed = True
        except Exception, e:
            logging.error("Uploading to Google Cloud Storage failed! Error: %s" % e)
            raise OperationError(e)
        finally:
            self.timer.stop(self.timer_name)
            self.stopped = True
