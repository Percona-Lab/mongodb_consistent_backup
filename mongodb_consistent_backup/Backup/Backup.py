import logging

from mongodb_consistent_backup.Backup.Mongodump import Mongodump
from mongodb_consistent_backup.Common import config_to_string, parse_method
from mongodb_consistent_backup.Errors import Error, OperationError


class Backup:
    def __init__(self, manager, config, timer, backup_dir, replsets, sharding=None):
        self.manager    = manager
        self.config     = config
        self.timer      = timer
        self.backup_dir = backup_dir
        self.replsets   = replsets
        self.sharding   = sharding

        self.timer_name = self.__class__.__name__
        self.method     = None
        self._method    = None

        self.init()

    def init(self):
        backup_method = self.config.backup.method
        if not backup_method or parse_method(backup_method) == "none":
            raise OperationError('Must specify a backup method!')
        self.method = parse_method(backup_method)
        try:
            self._method = globals()[self.method.capitalize()](
                self.manager,
                self.config,
                self.timer,
                self.backup_dir,
                self.replsets,
                self.sharding
            )
        except LookupError, e:
            raise OperationError('No backup method: %s' % self.method)
        except Exception, e:
            raise Error("Problem performing %s! Error: %s" % (self.method, e))

    def is_compressed(self):
        if self._method:
            return self._method.is_compressed()

    def backup(self):
        if self._method:
            config_string = config_to_string(self.config.backup[self.method])
            logging.info("Using backup method: %s (options: %s)" % (self.method, config_string))
            self.timer.start(self.timer_name)
            info = self._method.run()
            self.timer.stop(self.timer_name)
            logging.info("Backup completed in %.2f seconds" % self.timer.duration(self.timer_name))
            return info

    def close(self):
        if self._method:
            return self._method.close()
