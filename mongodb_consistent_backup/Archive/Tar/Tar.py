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
        self.verbose         = self.config.verbose
        self.binary          = "tar"
        self.do_gzip         = False
	self._pool           = None

    def compression(self, method=None):
	if method:
	    self.config.archive.tar.compression = method.lower()
        return self.config.archive.tar.compression

    def threads(self, thread_count=None):
	if thread_count:
	    self.config.archive.tar.threads = int(thread_count)
	    logging.info("Setting tar thread count to: %i" % self.config.archive.tar.threads)
        if self.config.archive.tar.threads is None or self.config.archive.tar.threads < 1:
            self.config.archive.tar.threads = cpu_count()
	return int(self.config.archive.tar.threads)

    def run(self):
        try:
            thread_count = self.threads()
            self._pool   = Pool(processes=thread_count)
            logging.info("Archiving backup directories with pool of %i thread(s)" % thread_count)
        except Exception, e:
            logging.fatal("Could not start pool! Error: %s" % e)
            raise e

        if os.path.isdir(self.backup_base_dir):
            try:
                for backup_dir in os.listdir(self.backup_base_dir):
                    subdir_name = "%s/%s" % (self.backup_base_dir, backup_dir)
                    output_file = "%s.tar" % subdir_name

                    if self.compression() == 'gzip':
                        output_file  = "%s.tgz" % subdir_name
                        self.do_gzip = True

                    self._pool.apply_async(TarThread(subdir_name, output_file, self.do_gzip, self.verbose, self.binary).run)
            except Exception, e:
                self._pool.terminate()
                logging.fatal("Could not create archiving thread! Error: %s" % e)
                raise e
            self._pool.close()
            self._pool.join()
        logging.info("Archiver threads completed")

    def close(self):
	logging.debug("Stopping Archiver threads")
        if self._pool is not None:
            self._pool.terminate()
            self._pool.join()
        logging.info("Stopped all Archiver threads")
