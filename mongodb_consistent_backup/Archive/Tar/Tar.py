import os
import logging

from copy_reg import pickle
from multiprocessing import Pool
from time import sleep
from types import MethodType

from TarThread import TarThread
from mongodb_consistent_backup.Errors import Error
from mongodb_consistent_backup.Pipeline import Task


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


pickle(MethodType, _reduce_method)


class Tar(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(Tar, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.compression_method = self.config.archive.tar.compression
        self.binary             = self.config.archive.tar.binary

        self._pool   = None
        self._pooled = []

        self.threads(self.config.archive.tar.threads)
        self._all_threads_successful = True

    def done(self, result):
        success   = result["success"]
        message   = result["message"]
        error     = result["error"]
        directory = result["directory"]
        exit_code = result["exit_code"]

        if success:
            if directory in self._pooled:
                logging.debug("Archiving completed for: %s" % directory)
            else:
                logging.warning("Tar thread claimed success, but delivered unexpected response %s for directory %s. "
                                "Assuming failure anyway." % (message, directory))
                self._all_threads_successful = False
        else:
            self._all_threads_successful = False
            logging.error("Tar thread failed for directory %s: %s; Exit code %s; Error %s)" %
                          (directory, message, exit_code, error))
        self._pooled.remove(directory)

    def wait(self):
        if len(self._pooled) > 0:
            self._pool.close()
            while len(self._pooled):
                logging.debug("Waiting for %i tar thread(s) to stop" % len(self._pooled))
                sleep(2)
            self._pool.terminate()
            logging.debug("Stopped all tar threads")
            self.stopped = True
            self.running = False

    def run(self):
        try:
            self._pool   = Pool(processes=self.threads())
            logging.info("Archiving backup directories with pool of %i thread(s)" % self.threads())
        except Exception, e:
            logging.fatal("Could not start pool! Error: %s" % e)
            raise Error(e)

        if os.path.isdir(self.backup_dir):
            try:
                self.running = True
                for backup_dir in os.listdir(self.backup_dir):
                    subdir_name = os.path.join(self.backup_dir, backup_dir)
                    if not os.path.isdir(os.path.join(subdir_name, "dump")):
                        continue
                    output_file = "%s.tar" % subdir_name
                    if self.do_gzip():
                        output_file = "%s.tgz" % subdir_name
                    self._pool.apply_async(
                        TarThread(subdir_name, output_file, self.compression(), self.verbose, self.binary).run,
                        callback=self.done)
                    self._pooled.append(subdir_name)
            except Exception, e:
                self._pool.terminate()
                logging.fatal("Could not create tar archiving thread! Error: %s" % e)
                raise Error(e)
            finally:
                self.wait()
                self.completed = self._all_threads_successful

    def close(self, code=None, frame=None):
        if not self.stopped and self._pool is not None:
            logging.debug("Stopping tar archiving threads")
            self._pool.terminate()
            logging.info("Stopped all tar archiving threads")
            self.stopped = True
