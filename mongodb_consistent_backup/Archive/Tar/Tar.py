import os
import logging

from copy_reg import pickle
from multiprocessing import Pool, cpu_count
from types import MethodType

from TarThread import TarThread


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

pickle(MethodType, _reduce_method)


class Tar:
    def __init__(self, config, backup_base_dir):
        self.config          = config
        self.backup_base_dir = backup_base_dir
        self.compression     = self.config.archive.compression
        self.thread_count    = self.config.archive.threads
        self.verbose         = self.config.verbose
        self.binary          = "tar"

        if self.thread_count is None or self.thread_count < 1:
            self.thread_count = cpu_count()

        try:
            self._pool = Pool(processes=self.thread_count)
        except Exception, e:
            logging.fatal("Could not start pool! Error: %s" % e)
            raise e

    def run(self):
        logging.info("Archiving backup directories with pool of %i thread(s)" % self.thread_count)
        if os.path.isdir(self.backup_base_dir):
            try:
                for backup_dir in os.listdir(self.backup_base_dir):
                    subdir_name = "%s/%s" % (self.backup_base_dir, backup_dir)
                    output_file = "%s.tar" % subdir_name

                    do_gzip = False
                    if self.compression == "gzip":
                        output_file = "%s.tgz" % subdir_name
                        do_gzip = True

                    self._pool.apply_async(TarThread(subdir_name, output_file, do_gzip, self.verbose, self.binary).run)
            except Exception, e:
                self._pool.terminate()
                logging.fatal("Could not create archiving thread! Error: %s" % e)
                raise e
            self._pool.close()
            self._pool.join()
        logging.info("Archiver threads completed")

    def close(self):
        logging.info("Killing all Archiver threads...")
        if self._pool is not None:
            self._pool.terminate()
            self._pool.join()
        logging.info("Killed all Archiver threads")
