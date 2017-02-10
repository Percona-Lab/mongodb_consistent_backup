import logging

from Mongodump import Mongodump
from mongodb_consistent_backup.Common import Timer, config_to_string, parse_method


class Backup:
    def __init__(self, config, backup_dir, replsets, config_server=None):
        self.config        = config
        self.backup_dir    = backup_dir
        self.replsets      = replsets
        self.config_server = config_server

        self.method  = None
        self._method = None
        self.timer   = Timer()
        self.init()

    def init(self):
        backup_method = self.config.backup.method
        if not backup_method or parse_method(backup_method) == "none":
            raise Exception, 'Must specify a backup method!', None
        self.method = parse_method(backup_method)
        try:
            self._method = globals()[self.method.capitalize()](
                self.config,
                self.backup_dir,
                self.replsets,
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
            config_string = config_to_string(self.config.backup[self.method])
            logging.info("Using backup method: %s (options: %s)" % (self.method, config_string))
            self.timer.start()

            info = self._method.run()

            self.timer.stop()
            logging.info("Backup completed in %s seconds" % self.timer.duration())

            return info

    def close(self):
        if self._method:
            return self._method.close()
