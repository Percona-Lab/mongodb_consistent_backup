import logging

from mongodb_consistent_backup.Archive.Tar import Tar
from mongodb_consistent_backup.Common import config_to_string, parse_method
from mongodb_consistent_backup.Errors import Error, OperationError


class Archive:
    def __init__(self, config, timer, backup_dir):
        self.config     = config
        self.timer      = timer 
        self.backup_dir = backup_dir

        self.timer_name = self.__class__.__name__
        self.method     = None
        self._archiver  = None
        self.init()

    def init(self):
        archive_method = self.config.archive.method
        if not archive_method or parse_method(archive_method) == "none":
            logging.info("Archiving disabled, skipping")
        else:
            self.method = parse_method(archive_method)
            logging.info("Using archiving method: %s" % self.method)
            try:
                self._archiver = globals()[self.method.capitalize()](
                    self.config,
                    self.backup_dir
                )
            except LookupError, e:
                raise OperationError('No archiving method: %s' % self.method)
            except Exception, e:
                raise Error("Problem performing %s! Error: %s" % (self.method, e))

    def compression(self, method=None):
        if self._archiver:
            return self._archiver.compression(method)

    def threads(self, threads=None):
        if self._archiver:
            return self._archiver.threads(threads)

    def archive(self):
        if self._archiver:
            config_string = config_to_string(self.config.archive[self.method])
            logging.info("Archiving with method: %s (options: %s)" % (self.method, config_string))
            self.timer.start(self.timer_name)

            self._archiver.run()

            self.timer.stop(self.timer_name)
            logging.info("Archiving completed in %.2f seconds" % self.timer.duration(self.timer_name))

    def close(self):
        if self._archiver:
            return self._archiver.close()
