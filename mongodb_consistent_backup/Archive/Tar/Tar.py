import os
import logging

from copy_reg import pickle
from multiprocessing import Pool
from time import sleep
from types import MethodType

from TarThread import TarThread
from mongodb_consistent_backup.Common import parse_method
from mongodb_consistent_backup.Errors import Error, OperationError
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
        self.binary             = "tar"

        self._pool   = None
        self._pooled = []

    def done(self, done_dir):
        if done_dir in self._pooled:
            logging.debug("Archiving completed for: %s" % done_dir)
            self._pooled.remove(done_dir)
        else:
            raise OperationError("Unexpected response from tar thread: %s" % done_dir)

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
            thread_count = self.threads()
            self._pool   = Pool(processes=thread_count)
            logging.info("Archiving backup directories with pool of %i thread(s)" % thread_count)
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
                        output_file  = "%s.tgz" % subdir_name
                    self._pool.apply_async(TarThread(subdir_name, output_file, self.compression(), self.verbose, self.binary).run, callback=self.done)
                    self._pooled.append(subdir_name)
            except Exception, e:
                self._pool.terminate()
                logging.fatal("Could not create tar archiving thread! Error: %s" % e)
                raise Error(e)
            finally:
                self.wait()
                self.completed = True

    def close(self, code=None, frame=None):
        if not self.stopped and self._pool is not None:
            logging.debug("Stopping tar archiving threads")
            self._pool.terminate()
            logging.info("Stopped all tar archiving threads")
            self.stopped = True
