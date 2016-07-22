import os
import logging

from copy_reg import pickle
from multiprocessing import Pool, cpu_count
from signal import signal, SIGINT, SIGTERM
from types import MethodType

from Common import LocalCommand


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)

pickle(MethodType, _reduce_method)


class Archive:
    def __init__(self, backup_dir, output_file, no_gzip=False, verbose=False, binary="tar"):
        self.backup_dir  = backup_dir
        self.output_file = output_file
        self.no_gzip     = no_gzip
        self.verbose     = verbose
        self.binary      = binary

        self._command  = None

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Killing running subprocess/command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()

    def run(self):
        if os.path.isdir(self.backup_dir):
            if not os.path.isfile(self.output_file):
                try:
                    backup_base_dir  = os.path.dirname(self.backup_dir)
                    backup_base_name = os.path.basename(self.backup_dir)

                    log_msg   = "Archiving and compressing directory: %s" % self.backup_dir
                    cmd_flags = ["-C", backup_base_dir, "-czf", self.output_file, "--remove-files", backup_base_name]

                    if self.no_gzip:
                        log_msg   = "Archiving directory: %s" % self.backup_dir
                        cmd_flags = ["-C", backup_base_dir, "-cf", self.output_file, "--remove-files", backup_base_name]

                    try:
                        logging.info(log_msg)
                        self._command = LocalCommand(self.binary, cmd_flags, self.verbose)
                        self._command.run()
                    except Exception, e:
                        raise e
                except Exception, e:
                    logging.fatal("Failed archiving file: %s! Error: %s" % (self.output_file, e))
                    raise e
            elif os.path.isfile(self.output_file):
                logging.fatal("Output file: %s already exists!" % self.output_file)
                raise Exception, "Output file %s already exists!" % self.output_file, None


class Archiver:
    def __init__(self, backup_base_dir, no_gzip=False, thread_count=None, verbose=False):
        self.backup_base_dir = backup_base_dir
        self.no_gzip         = no_gzip
        self.thread_count    = thread_count
        self.verbose         = verbose
        self.binary          = "tar"

        if self.thread_count is None:
            self.thread_count = cpu_count()

        try:
            self._pool = Pool(processes=self.thread_count)
        except Exception, e:
            logging.fatal("Could not start pool! Error: %s" % e)
            raise e

    def run(self):
        logging.info("Archiving backup directories with %i threads max" % self.thread_count)
        if os.path.isdir(self.backup_base_dir):
            try:
                for backup_dir in os.listdir(self.backup_base_dir):
                    subdir_name = "%s/%s" % (self.backup_base_dir, backup_dir)
                    output_file = "%s.tgz" % subdir_name
                    if self.no_gzip:
                        output_file = "%s.tar" % subdir_name

                    self._pool.apply_async(Archive(subdir_name, output_file, self.no_gzip, self.verbose, self.binary).run)
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
