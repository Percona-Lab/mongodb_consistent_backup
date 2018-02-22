import logging
import os
import sys

from gzip import GzipFile


class StdoutLogFilter(object):
    def __init__(self, max_level):
        self.max_level = max_level

    def filter(self, entry):
        return entry.levelno < self.max_level


class Logger:
    def __init__(self, config, backup_time):
        self.config      = config
        self.backup_name = self.config.backup.name
        self.backup_time = backup_time

        self.stderr_level = logging.ERROR
        self.stdout_level = logging.INFO
        if self.config.verbose:
            self.stdout_level = logging.DEBUG

        self.do_file_log = False
        if self.config.log_dir is not '':
            self.do_file_log = True
            if not os.path.isdir(self.config.log_dir):
                print "WARNING: Creating logging directory: %s" % self.config.log_dir
                os.mkdir(self.config.log_dir)

        self.log_format = '[%(asctime)s] [%(levelname)s] [%(processName)s] [%(module)s:%(funcName)s:%(lineno)d] %(message)s'
        self.file_log   = None
        self.last_log   = None

    def start(self):
        try:
            logging.getLogger('').setLevel(self.stdout_level)

            # stdout logging: DEBUG/INFO -> WARNING
            stdout = logging.StreamHandler(sys.stdout)
            stdout.setLevel(self.stdout_level)
            stdout.setFormatter(logging.Formatter(self.log_format))
            stdout.addFilter(StdoutLogFilter(self.stderr_level))
            logging.getLogger('').addHandler(stdout)

            # sterrr logging: ERROR -> FATAL
            stderr = logging.StreamHandler(sys.stderr)
            stderr.setLevel(self.stderr_level)
            stderr.setFormatter(logging.Formatter(self.log_format))
            logging.getLogger('').addHandler(stderr)
        except Exception, e:
            print("Could not start logger: %s" % e)
            raise e

    def start_file_logger(self):
        if self.do_file_log:
            try:
                self.current_log_file = os.path.join(self.config.log_dir, "backup.%s.log" % self.backup_name)
                self.backup_log_file  = os.path.join(self.config.log_dir, "backup.%s.%s.log" % (self.backup_name, self.backup_time))
                self.file_log = logging.FileHandler(self.backup_log_file)
                self.file_log.setLevel(self.stdout_level)
                self.file_log.setFormatter(logging.Formatter(self.log_format))
                logging.getLogger('').addHandler(self.file_log)
            except OSError:
                logging.warning("Could not start file log handler, writing to stdout only")
                pass

    def close(self):
        if self.file_log:
            self.file_log.close()

    def compress(self, current=False):
        gz_log = None
        try:
            compress_file = self.backup_log_file
            if not current:
                compress_file = self.last_log
                if not os.path.isfile(self.last_log) or self.last_log == self.backup_log_file:
                    return
            logging.info("Compressing log file: %s" % compress_file)
            gz_file = "%s.gz" % compress_file
            gz_log  = GzipFile(gz_file, "w+")
            with open(compress_file) as f:
                for line in f:
                    gz_log.write(line)
            os.remove(compress_file)
        finally:
            if gz_log:
                gz_log.close()

    def update_symlink(self):
        if not self.do_file_log:
            return
        if os.path.islink(self.current_log_file):
            self.last_log = os.readlink(self.current_log_file)
            os.remove(self.current_log_file)
        os.symlink(self.backup_log_file, self.current_log_file)

    def rotate(self):
        if self.do_file_log and self.last_log:
            logging.info("Running rotation of log files")
            self.compress()
