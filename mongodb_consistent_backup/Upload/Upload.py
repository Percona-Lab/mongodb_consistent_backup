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
        if self.config.upload.method == "s3" and self.config.upload.s3.bucket_name and self.config.upload.s3.bucket_prefix and self.config.upload.s3.access_key and self.config.upload.s3.secret_key:
            # AWS S3 secure multipart uploader
            logging.info("Using upload method: S3")
            try:
                self._uploader = S3(
                    self.config,
                    self.base_dir,
                    self.backup_dir
                )
            except Exception, e:
                raise Exception, "Problem performing AWS S3 multipart upload! Error: %s" % e, None
        else:
            logging.info("Uploading disabled, skipping")

    def upload(self):
        if self._uploader:
            self._uploader.run()

    def close(self):
        if self._uploader:
            self._uploader.close()
