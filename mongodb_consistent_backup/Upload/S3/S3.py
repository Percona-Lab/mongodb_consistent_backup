import os
import logging

from S3UploadPool import S3UploadPool

from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task
from mongodb_consistent_backup.Upload.Util import get_upload_files


class S3(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(S3, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.remove_uploaded     = self.config.upload.remove_uploaded
        self.retries             = self.config.upload.retries
        self.region              = self.config.upload.s3.region
        self.bucket_name         = getattr(self.config.upload.s3, 'bucket_name', None)
        self.bucket_prefix       = getattr(self.config.upload.s3, 'bucket_prefix', None)
        self.bucket_explicit_key = getattr(self.config.upload.s3, 'bucket_explicit_key', None)
        self.access_key          = getattr(self.config.upload.s3, 'access_key', None)
        self.secret_key          = getattr(self.config.upload.s3, 'secret_key', None)
        self.chunk_size_mb       = self.config.upload.s3.chunk_size_mb
        self.chunk_size          = self.chunk_size_mb * 1024 * 1024
        self.target_bandwidth    = None
        if self.config.upload.s3.target_mb_per_second is not None:
            self.target_bandwidth    = self.config.upload.s3.target_mb_per_second * 1024 * 1024
        self.s3_acl              = self.config.upload.s3.acl
        self.key_prefix          = base_dir
        self.validate_bucket     = not self.config.upload.s3.skip_bucket_validation
        if self.config.upload.file_regex == "none":
            self.upload_file_regex = None
        else:
            self.upload_file_regex = self.config.upload.file_regex

        self.threads(self.config.upload.threads)
        self._pool = None

        if self.region is None:
            raise OperationError("Invalid or missing AWS S3 region detected!")

        self._pool = S3UploadPool(
            self.bucket_name,
            self.region,
            self.access_key,
            self.secret_key,
            self.threads(),
            self.remove_uploaded,
            self.chunk_size,
            self.s3_acl,
            validate_bucket=self.validate_bucket,
            target_bandwidth=self.target_bandwidth
        )

    def get_key_name(self, file_path):
        rel_path = os.path.relpath(file_path, self.backup_dir)
        if self.bucket_explicit_key:
            key_name = self.bucket_explicit_key
        elif self.bucket_prefix == "/":
            key_name = "/%s/%s" % (self.key_prefix, rel_path)
        else:
            key_name = "%s/%s/%s" % (self.bucket_prefix, self.key_prefix, rel_path)
        return key_name

    def run(self):
        if not os.path.isdir(self.backup_dir):
            logging.error("The source directory: %s does not exist or is not a directory! Skipping AWS S3 Upload!" % self.backup_dir)
            return
        try:
            self.timer.start(self.timer_name)
            logging.info("Starting AWS S3 upload to %s (%i threads, %imb multipart chunks, %i retries)" % (
                self.bucket_name,
                self.threads(),
                self.chunk_size_mb,
                self.retries
            ))
            for file_path in get_upload_files(self.backup_dir, self.upload_file_regex):
                key_name = self.get_key_name(file_path)
                self._pool.upload(file_path, key_name)
            self._pool.wait()
        except Exception, e:
            logging.error("Uploading to AWS S3 failed! Error: %s (error type: %s)" % (e, type(e)))
            raise OperationError(e)
        finally:
            self.timer.stop(self.timer_name)
            self._pool.close()

        self.completed = True

    def close(self, code=None, frame=None):
        if self._pool:
            self._pool.close()
