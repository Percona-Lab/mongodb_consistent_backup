import logging

from Mongodump import Mongodump


class Backup:
    def __init__(self, config, backup_dir, secondaries, config_server=None):
        self.config        = config
        self.backup_dir    = backup_dir
        self.secondaries   = secondaries
        self.config_server = config_server

        self.method  = None
        self._method = None
        self.init()

    def init(self):
        backup_method = self.config.backup.method
        if not backup_method or backup_method.lower() == "none":
            raise Exception, 'Must specify a backup method!', None
        self.method = backup_method.lower()
        logging.info("Using backup method: %s" % self.method)
        try:
            self._method = globals()[self.method.capitalize()](
                self.config,
                self.backup_dir,
                self.secondaries,
                self.config_server
            )
        except Exception, e:
            raise Exception, "Problem performing %s! Error: %s" % (self.method, e), None

    def is_compressed(self):
        if self._method:
            return self._method.is_compressed()

    def backup(self):
        if self._method:
            return self._method.run()

    def close(self):
        if self._method:
            return self._method.close()
