import logging

from Mongodump import Mongodump


class Backup:
    def __init__(self, config, backup_dir, secondaries, config_server=None):
        self.config        = config
        self.backup_dir    = backup_dir
        self.secondaries   = secondaries
        self.config_server = config_server

        self._method = None
        self.init()

    def init(self):
        if self.config.backup.method == "mongodump":
            logging.info("Using backup method: mongodump")
            try:
                self._method = Mongodump(
                    self.config,
                    self.backup_dir,
                    self.secondaries,
                    self.config_server
                )
            except Exception, e:
                raise Exception, "Problem performing mongodump! Error: %s" % e, None
        else:
            raise Exception, 'Must specify a backup method!', None

    def is_gzip(self):
        if self._method:
            return self._method.is_gzip()

    def backup(self):
        if self._method:
            return self._method.run()

    def close(self):
        if self._method:
            return self._method.close()
