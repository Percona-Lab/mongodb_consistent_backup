from mongodb_consistent_backup.Errors import Error


class PoolThread(object):
    def __init__(self, thread_name, config, compression_method='none'):
        self.thread_name        = thread_name
        self.config             = config
        self.compression_method = compression_method

        self.timer_name = self.__class__.__name__
        self.stopped    = False
        self.running    = False
        self.completed  = False
        self.exit_code  = 255

    def compression(self, method=None):
        if method:
            self.compression_method = method
        return self.compression_method

    def do_gzip(self):
        if self.compression() == 'gzip':
            return True
        return False

    def run(self):
        raise Error("Must define a .run() method when using %s class!" % self.__class__.__name__)

    def close(self, code=None, frame=None):
        raise Error("Must define a .close() method when using %s class!" % self.__class__.__name__)
