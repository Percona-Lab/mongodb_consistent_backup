import logging

from S3 import S3
from mongodb_consistent_backup.Common import config_to_string, parse_submodule


class Upload:
    def __init__(self, config, base_dir, backup_dir):
        self.config     = config
        self.base_dir   = base_dir
        self.backup_dir = backup_dir

        self.method    = None
        self._uploader = None
        self.init()

    def init(self):
        upload_method = self.config.upload.method
        if not upload_method or parse_submodule(upload_method) == "none":
            logging.info("Uploading disabled, skipping")
        else:
            self.method   = parse_submodule(upload_method)
            config_string = config_to_string(self.config.upload[self.method])
            logging.info("Using upload method: %s (options: %s)" % (self.method, config_string))
            try:
                self._uploader = globals()[self.method.capitalize()](
                    self.config,
                    self.base_dir,
                    self.backup_dir
                )
            except LookupError, e:
                raise Exception, 'No upload method: %s' % self.method, None
            except Exception, e:
                raise Exception, "Problem settings up %s Uploader Error: %s" % (self.method, e), None

    def upload(self):
        if self._uploader:
            self._uploader.run()

    def close(self):
        if self._uploader:
            self._uploader.close()
