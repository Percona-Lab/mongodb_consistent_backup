from Dump import Dump
from Dumper import Dumper


class Backup:
    def __init__(self, config):
        self.config = config
        self._method = None

    def do_gzip(self):
        pass

    def backup(self):
        pass
