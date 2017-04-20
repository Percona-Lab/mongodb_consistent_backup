import os
import logging

from copy_reg import pickle
from multiprocessing import Pool
from types import MethodType

from TarThread import TarThread
from mongodb_consistent_backup.Common import parse_method
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
        self.binary             = "tar"

        self._pool   = None
        self._pooled = []

    def wait(self):
        if len(self._pooled) > 0:
            self._pool.close()
            logging.debug("Waiting for tar threads to stop")
            while len(self._pooled) > 0:
                try:
                    item = self._pooled[0]
                    path, result = item
                    result.get(1)
		    logging.debug("Archiving completed for directory: %s" % path)
                    self._pooled.remove(item)
                except TimeoutError:
                    continue
            self.stopped = True

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
                    result = self._pool.apply_async(TarThread(subdir_name, output_file, self.do_gzip(), self.verbose, self.binary).run)
                    self._pooled.append((subdir_name, result))
            except Exception, e:
                self._pool.terminate()
                logging.fatal("Could not create tar archiving thread! Error: %s" % e)
                raise Error(e)
            finally:
                self.wait()
            self.completed = True

    def close(self, code=None, frame=None):
        logging.debug("Stopping tar archiving threads")
        if not self.stopped and self._pool is not None:
            self._pool.terminate()
            self._pool.join()
            logging.info("Stopped all tar archiving threads")
            self.stopped = True
