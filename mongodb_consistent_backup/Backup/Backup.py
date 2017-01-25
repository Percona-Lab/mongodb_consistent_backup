import logging

from Mongodump import Mongodump
from mongodb_consistent_backup.Common import config_to_string, parse_submodule


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
        if not backup_method or parse_submodule(backup_method) == "none":
            raise Exception, 'Must specify a backup method!', None
        self.method   = parse_submodule(backup_method)
        config_string = config_to_string(self.config.backup[self.method])
        logging.info("Using backup method: %s (options: %s)" % (self.method, config_string))
        try:
            self._method = globals()[self.method.capitalize()](
                self.config,
                self.backup_dir,
                self.secondaries,
                self.config_server
            )
        except LookupError, e:
            raise Exception, 'No backup method: %s' % self.method, None
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
