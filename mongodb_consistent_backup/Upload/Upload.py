import logging

from S3 import S3


class Upload:
    def __init__(self, config, base_dir, backup_dir):
        self.config     = config
        self.base_dir   = base_dir
        self.backup_dir = backup_dir

        self._uploader = None
        self.init()

    def init(self):
        upload_method = self.config.upload.method
        if not upload_method or upload_method is "none":
            logging.info("Uploading disabled, skipping")
        else:
            #TODO Remove this line and move to  S3 Lib for checking
            # if self.config.upload.method == "s3" and self.config.upload.s3.bucket_name and self.config.upload.s3.bucket_prefix and self.config.upload.s3.access_key and self.config.upload.s3.secret_key:

            logging.info("Using upload method: %s" % upload_method)
            try:
                self._uploader = globals()[upload_method.capitalize()](
                    self.config,
                    self.base_dir,
                    self.backup_dir
                )
            except Exception, e:
                raise Exception, "Problem settings up %s Uploader Error: %s" % (self.config.upload.method, e), None

    def upload(self):
        if self._uploader:
            self._uploader.run()

    def close(self):
        if self._uploader:
            self._uploader.close()
