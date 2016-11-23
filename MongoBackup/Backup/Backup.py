import logging

from Dump import Dump
from Dumper import Dumper


class Backup:
    def __init__(self, config, backup_root_dir, secondaries, config_server=None):
        self.config          = config
        self.backup_root_dir = backup_root_dir
        self.secondaries     = secondaries
        self.config_server   = config_server

        self.init()

    def init(self):
        if self.config.backup.method == "mongodump":
            logging.info("Using backup method: mongodump")
            try:
                self._method = Dumper(
                    self.config,
                    self.backup_root_dir,
                    self.secondaries,
                    self.config_server
                )
            except Exception, e:
                raise Exception, "Problem performing mongodump! Error: %s" % e, None

    def is_gzip(self):
        return self._method.is_gzip()

    def backup(self):
        return self._method.run()

    def close(self):
        return self._method.close()
