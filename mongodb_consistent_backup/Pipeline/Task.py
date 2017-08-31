import logging

from multiprocessing import cpu_count
from signal import signal, SIGINT, SIGTERM, SIG_IGN

from mongodb_consistent_backup.Common import parse_method
from mongodb_consistent_backup.Errors import Error


class Task(object):
    def __init__(self, task_name, manager, config, timer, base_dir, backup_dir, **kwargs):
        self.task_name  = task_name
        self.manager    = manager
        self.config     = config
        self.timer      = timer
        self.base_dir   = base_dir
        self.backup_dir = backup_dir
        self.args       = kwargs
        self.verbose    = self.config.verbose

        self.runnning  = False
        self.stopped   = False
        self.completed = False
        self.exit_code = 255

        self.thread_count          = None
        self.cpu_count             = cpu_count()
        self.compression_method    = 'none'
        self.compression_supported = ['none']
        self.timer_name            = self.__class__.__name__

        signal(SIGINT, SIG_IGN)
        signal(SIGTERM, self.close)

    def compression(self, method=None):
        if method and method in self.compression_supported:
            self.compression_method = parse_method(method)
            logging.info("Setting %s compression method: %s" % (self.task_name, self.compression_method))
        return parse_method(self.compression_method)

    def is_compressed(self):
        if self.compression() == 'auto' and hasattr(self, "can_compress"):
            return self.can_compress()
        elif self.compression() != 'none':
            return True
        return False

    def do_gzip(self):
        if self.compression() == 'gzip':
            return True
        return False

    def threads(self, thread_count=None, default_cpu_multiply=1):
        if thread_count:
            self.thread_count = int(thread_count)
            logging.info("Setting %s thread count to: %i" % (self.task_name, self.thread_count))
        if self.thread_count is None or self.thread_count < 1:
            self.thread_count = self.cpu_count * default_cpu_multiply
        return int(self.thread_count)

    def run(self):
        raise Error("Must define a .run() method when using %s class!" % self.__class__.__name__)

    def close(self, code=None, frame=None):
        raise Error("Must define a .close() method when using %s class!" % self.__class__.__name__)
