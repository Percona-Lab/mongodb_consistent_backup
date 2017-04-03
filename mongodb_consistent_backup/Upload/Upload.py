import logging

from mongodb_consistent_backup.Upload.S3 import S3
from mongodb_consistent_backup.Common import config_to_string, parse_method
from mongodb_consistent_backup.Errors import Error, OperationError


class Upload:
    def __init__(self, config, timer, base_dir, backup_dir):
        self.config     = config
        self.timer      = timer
        self.base_dir   = base_dir
        self.backup_dir = backup_dir

        self.timer_name = self.__class__.__name__
        self.method     = None
        self._uploader  = None
        self.init()

    def init(self):
        upload_method = self.config.upload.method
        if not upload_method or parse_method(upload_method) == "none":
            logging.info("Uploading disabled, skipping")
        else:
            self.method = parse_method(upload_method)
            try:
                self._uploader = globals()[self.method.capitalize()](
                    self.config,
                    self.timer,
                    self.base_dir,
                    self.backup_dir
                )
            except LookupError, e:
                raise OperationError('No upload method: %s' % self.method)
            except Exception, e:
                raise Error("Problem settings up %s Uploader Error: %s" % (self.method, e))

    def upload(self):
        if self._uploader:
            config_string = config_to_string(self.config.upload[self.method])
            logging.info("Using upload method: %s (options: %s)" % (self.method, config_string))
            self.timer.start(self.timer_name)
            self._uploader.run()
            self.timer.stop(self.timer_name)
            logging.info("Uploading completed in %s seconds" % self.timer.duration(self.timer_name))

    def close(self):
        if self._uploader:
            self._uploader.close()
