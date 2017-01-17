import logging


class Backup:
    def __init__(self, config, backup_dir, secondaries, config_server=None):
        self.config        = config
        self.backup_dir    = backup_dir
        self.secondaries   = secondaries
        self.config_server = config_server

        self._method = None
        self.init()

    def init(self):
        backup_method = self.config.backup.method
        if backup_method is None:
            raise Exception, 'Must specify a backup method!', None
        logging.info("Using backup method: %s" % backup_method)
        try:
            self._method = globals()[backup_method](
                self.config,
                self.backup_dir,
                self.secondaries,
                self.config_server
            )
        except Exception, e:
            raise Exception, "Problem performing %s! Error: %s" % (backup_method, e), None

    def is_compressed(self):
        if self._method:
            return self._method.is_compressed()

    def backup(self):
        if self._method:
            return self._method.run()

    def close(self):
        if self._method:
            return self._method.close()
