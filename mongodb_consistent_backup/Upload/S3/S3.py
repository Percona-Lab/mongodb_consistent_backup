import os
import logging

import boto.s3.multipart
from copy_reg import pickle
from math import ceil
from multiprocessing import Pool
from types import MethodType

from S3Session import S3Session
from S3UploadThread import S3UploadThread

from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)
pickle(MethodType, _reduce_method)


class S3(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(S3, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.remove_uploaded = self.config.upload.remove_uploaded
        self.region          = self.config.upload.s3.region
        self.bucket_name     = self.config.upload.s3.bucket_name
        self.bucket_prefix   = self.config.upload.s3.bucket_prefix
        self.access_key      = self.config.upload.s3.access_key
        self.secret_key      = self.config.upload.s3.secret_key
        self.thread_count    = self.config.upload.s3.threads
        self.chunk_size_mb   = self.config.upload.s3.chunk_size_mb
        self.chunk_size      = self.chunk_size_mb * 1024 * 1024
        self.secure          = self.config.upload.s3.secure
        self.retries         = self.config.upload.s3.retries
        self.s3_acl          = self.config.upload.s3.acl

        self.key_prefix = base_dir
        if 'key_prefix' in self.args:
            self.key_prefix = key_prefix

        self._pool        = None
        self._multipart   = None
        self._upload_done = False
        if None in (self.access_key, self.secret_key, self.region):
            raise "Invalid S3 security key or region detected!"
        try:
            self.s3_conn = S3Session(self.region, self.access_key, self.secret_key, self.bucket_name)
            self.bucket  = self.s3_conn.get_bucket(self.bucket_name)
        except Exception, e:
            raise OperationError(e)

    def run(self):
        if not os.path.isdir(self.backup_dir):
            logging.error("The source directory: %s does not exist or is not a directory! Skipping AWS S3 Upload!" % self.backup_dir)
            return
        try:
            self.timer.start(self.timer_name)
            for file_name in os.listdir(self.backup_dir):
                file_path = os.path.join(self.backup_dir, file_name)
                # skip mongodb-consistent-backup_META dir
                if os.path.isdir(file_path):
                    continue
                file_size = os.stat(file_path).st_size
                chunk_count = int(ceil(file_size / float(self.chunk_size)))

                if self.bucket_prefix == "/":
                    key_name = "/%s/%s" % (self.key_prefix, file_name)
                else:
                    key_name = "%s/%s/%s" % (self.bucket_prefix, self.key_prefix, file_name)

                logging.info("Starting multipart AWS S3 upload to key: %s%s using %i threads, %imb chunks, %i retries" % (
                    self.bucket_name,
                    key_name,
                    self.thread_count,
                    self.chunk_size_mb,
                    self.retries
                ))
                self._multipart = self.bucket.initiate_multipart_upload(key_name)
                self._pool      = Pool(processes=self.thread_count)

                for i in range(chunk_count):
                    offset = self.chunk_size * i
                    byte_count = min(self.chunk_size, file_size - offset)
                    part_num = i + 1
                    self._pool.apply_async(S3UploadThread(
                        self.bucket_name,
                        self.region,
                        self.access_key,
                        self.secret_key,
                        self._multipart.id,
                        part_num,
                        file_path,
                        offset,
                        byte_count,
                        self.retries,
                        self.secure
                    ).run)
                self._pool.close()
                self._pool.join()

                part_count = 0
                for part in boto.s3.multipart.part_lister(self._multipart):
                  part_count += 1
                if part_count == chunk_count:
                    self._multipart.complete_upload()
                    key = self.bucket.get_key(key_name)
                    if self.s3_acl:
                        key.set_acl(self.s3_acl)
                    self._upload_done = True

                    if self.remove_uploaded:
                        logging.info("Uploaded AWS S3 key: %s%s successfully. Removing local file" % (self.bucket_name, key_name))
                        os.remove(os.path.join(self.backup_dir, file_name))
                    else:
                        logging.info("Uploaded AWS S3 key: %s%s successfully" % (self.bucket_name, key_name))
                else:
                    self._multipart.cancel_upload()
                    logging.error("Failed to upload all multiparts for key: %s%s! Upload cancelled" % (self.bucket_name, key_name))
                    raise OperationError("Failed to upload all multiparts for key: %s%s! Upload cancelled" % (self.bucket_name, key_name))

            if self.remove_uploaded:
                logging.info("Removing backup source dir after successful AWS S3 upload of all backups")
                os.rmdir(self.backup_dir)
            self.timer.stop(self.timer_name)
        except Exception, e:
            logging.error("Uploading to AWS S3 failed! Error: %s" % e)
            if self._multipart:
                self._multipart.cancel_upload()
            raise OperationError(e)
        self.completed = True

    def close(self):
        if self._pool:
            logging.error("Terminating multipart AWS S3 upload threads")
            self._pool.terminate()
            self._pool.join()

        if self._multipart and not self._upload_done:
            logging.error("Cancelling incomplete multipart AWS S3 upload")
            self._multipart.cancel_upload()

        if self.s3_conn:
            self.s3_conn.close()
