import boto.s3.multipart
import logging
import os

from boto.s3.key import Key
from copy_reg import pickle
from math import ceil
from multiprocessing import Pool, TimeoutError
from time import sleep
from types import MethodType

from S3Session import S3Session
from S3UploadThread import S3UploadThread

from mongodb_consistent_backup.Common.Util import file_md5hash
from mongodb_consistent_backup.Errors import OperationError


# Allows pooled .apply_async()s to work on Class-methods:
def _reduce_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


pickle(MethodType, _reduce_method)


class S3UploadPool():
    def __init__(self, bucket_name, region, access_key, secret_key, threads=4, remove_uploaded=False, chunk_bytes=50 * 1024 * 1024, key_acl=None, **kwargs):
        self.bucket_name     = bucket_name
        self.region          = region
        self.access_key      = access_key
        self.secret_key      = secret_key
        self.threads         = threads
        self.remove_uploaded = remove_uploaded
        self.chunk_bytes     = chunk_bytes
        self.key_acl         = key_acl
        self.validate_bucket = kwargs.get("validate_bucket")
        self.target_bandwidth = kwargs.get("target_bandwidth")
        self.upload_file_regex = kwargs.get("upload_file_regex")

        self.multipart_min_bytes = 5242880

        self._closed     = False
        self._uploads    = {}
        self._mp_uploads = {}
        self._pool       = Pool(processes=self.threads)

        try:
            self.s3_conn = S3Session(self.region, self.access_key, self.secret_key, self.bucket_name,
                                     validate_bucket=self.validate_bucket)
            self.bucket  = self.s3_conn.get_bucket(self.bucket_name)
        except Exception, e:
            raise OperationError(e)

    def close(self, code=None, frame=None):
        if self._closed:
            return
        if self._pool:
            logging.info("Stopping AWS S3 upload pool")
            self._pool.terminate()
            self._pool.join()
        self.cancel_multipart_uploads()
        self._closed = True

    def s3_exists(self, key_name):
        key = None
        try:
            logging.debug("Checking if key exists s3://%s%s" % (self.bucket_name, key_name))
            key = Key(bucket=self.bucket, name=key_name)
            return key.exists()
        finally:
            if key:
                key.close()

    def s3_md5hex(self, key_name):
        key = None
        try:
            logging.debug("Gathering checksum for s3://%s%s" % (self.bucket_name, key_name))
            key = self.bucket.get_key(key_name)
            if hasattr(key, 'etag'):
                return key.etag[1:-1]
        finally:
            if key:
                key.close()

    def cancel_multipart_uploads(self):
        if len(self._mp_uploads):
            for file_name in self._mp_uploads:
                mp_upload = self._mp_uploads[file_name]
                if not mp_upload["complete"]:
                    logging.info("Cancelling multipart upload: %s" % file_name)
                    mp_upload["upload"].cancel_upload()
                    mp_upload["complete"] = True

    def is_dir_empty(self, dir_name):
        if os.path.isdir(dir_name):
            if not len(os.listdir(dir_name)):
                return True
            return False

    def remove_file(self, file_name):
        if os.path.isfile(file_name):
            dir_name = os.path.dirname(file_name)
            logging.debug("Removing uploaded file: %s" % file_name)
            os.remove(file_name)
            if self.is_dir_empty(dir_name):
                logging.debug("Removing empty directory: %s" % dir_name)
                os.rmdir(dir_name)

    def set_key_acl(self, key_name):
        if self.key_acl:
            try:
                self.bucket.set_acl(self.key_acl, key_name)
            except Exception:
                logging.exception("Unable to set ACLs on uploaded key: {}.".format(key_name))

    def get_uploaded_multiparts(self, file_name, key_name):
        logging.debug("Getting completed upload parts for s3://%s%s" % (self.bucket_name, key_name))
        mp    = self.get_multipart_upload(file_name, key_name)
        parts = []
        for part in boto.s3.multipart.part_lister(mp):
            parts.append(part)
        return parts

    def is_multiparts_uploaded(self, file_name):
        if file_name in self._uploads:
            upload = self._uploads[file_name]
            if 'multipart' in upload and upload['multipart']:
                for part in upload['parts']:
                    if not upload['parts'][part]['complete']:
                        return False
                return True

    def complete_multipart(self, file_name, key_name):
        if self.is_multiparts_uploaded(file_name):
            upload         = self._uploads[file_name]
            uploaded_parts = len(self.get_uploaded_multiparts(file_name, key_name))
            total_parts    = len(upload['parts'])

            if uploaded_parts == total_parts and not upload['complete']:
                logging.debug("Completing multipart upload for key: s3://%s%s" % (self.bucket_name, key_name))
                multipart = self.get_multipart_upload(file_name, key_name)
                if multipart:
                    multipart.complete_upload()
                    self._mp_uploads[file_name]["complete"] = True
                    upload['complete'] = True
                    self.set_key_acl(key_name)
                    if self.remove_uploaded:
                        self.remove_file(file_name)
                    logging.info("Uploaded AWS S3 key successfully: s3://%s%s" % (self.bucket_name, key_name))

    def complete(self, output_tuple):
        file_name, key_name, mp_num = output_tuple
        if file_name:
            upload = self._uploads[file_name]
            logging.debug("Got success callback for upload: s3://%s%s (multipart: %s)" % (self.bucket_name, key_name, mp_num))
            if mp_num and "parts" in upload and mp_num in upload["parts"]:
                upload["parts"][mp_num]["complete"] = True
                self.complete_multipart(file_name, key_name)
            else:
                upload["complete"] = True
                self.set_key_acl(key_name)
                if self.remove_uploaded:
                    self.remove_file(file_name)
                logging.info("Uploaded AWS S3 key successfully: s3://%s%s" % (self.bucket_name, key_name))

    def get_multipart_upload(self, file_name, key_name):
        if file_name not in self._mp_uploads:
            self._mp_uploads[file_name] = {
                "upload": self.bucket.initiate_multipart_upload(key_name),
                "complete": False
            }
        return self._mp_uploads[file_name]["upload"]

    def get_file_size(self, file_name):
        if os.path.isfile(file_name):
            return os.stat(file_name).st_size
        else:
            logging.error("Upload file does not exist (or is not a file): %s" % file_name)
            raise OperationError("Upload file does not exist (or is not a file)!")

    def start(self, file_name, key_name, byte_count, mp_id=None, mp_num=None, mp_parts=None, mp_offset=None):
        logging.debug("Adding to pool: s3://%s%s (multipart: %s)" % (self.bucket_name, key_name, mp_num))
        return self._pool.apply_async(
            S3UploadThread(
                self.bucket_name,
                self.region,
                self.access_key,
                self.secret_key,
                file_name,
                key_name,
                byte_count,
                self.target_bandwidth,
                mp_id,
                mp_num,
                mp_parts,
                mp_offset
            ).run,
            callback=self.complete
        )

    def upload_multipart(self, file_name, key_name, file_size):
        if file_name not in self._uploads:
            self._uploads[file_name] = {
                "complete":  False,
                "multipart": True,
                "parts":     {},
            }
        part_num    = 0
        chunk_count = int(ceil(file_size / float(self.chunk_bytes)))
        mp_upload   = self.get_multipart_upload(file_name, key_name)
        for i in range(chunk_count):
            self.check_uploads()
            offset     = self.chunk_bytes * i
            part_num  += 1
            byte_count = min(self.chunk_bytes, file_size - offset)
            result     = self.start(file_name, key_name, byte_count, mp_upload.id, part_num, chunk_count, offset)
            self._uploads[file_name]['parts'][part_num] = {
                "complete": False,
                "result":   result
            }

    def upload(self, file_name, key_name):
        if self.s3_exists(key_name):
            s3_md5hex   = self.s3_md5hex(key_name)
            file_md5hex = file_md5hash(file_name)
            if s3_md5hex and file_md5hex == s3_md5hex:
                logging.warning("Key %s already exists with same checksum (%s), skipping" % (key_name, s3_md5hex))
                return
            else:
                logging.debug("Key %s already exists but the local file checksum differs (local:%s, s3:%s). Re-uploading" % (
                    key_name,
                    file_md5hex,
                    s3_md5hex
                ))
        file_size = self.get_file_size(file_name)
        if file_size >= self.multipart_min_bytes and file_size >= self.chunk_bytes:
            self.upload_multipart(file_name, key_name, file_size)
        else:
            result = self.start(file_name, key_name, file_size)
            self._uploads[file_name] = {
                "complete":  False,
                "multipart": False,
                "result":    result
            }

    def incomplete(self):
        incomplete = {}
        for file_name in self._uploads:
            upload = self._uploads[file_name]
            if upload['complete']:
                continue
            incomplete[file_name] = upload
        return incomplete

    def check_upload(self, upload, poll_secs=0.5):
        if 'result' in upload:
            try:
                output = upload['result'].get(poll_secs)
                if output:
                    return output
            except TimeoutError:
                pass
            except Exception as e:
                logging.error("Got error from upload pool thread: %s" % e)
                self.cancel_multipart_uploads()
                raise e

    def check_uploads(self, poll_secs=0.5):
        for file_name in self._uploads:
            upload = self._uploads[file_name]
            if 'multipart' in upload and upload['multipart']:
                for part_num in upload['parts']:
                    part = upload['parts'][part_num]
                    self.check_upload(part, poll_secs)
            else:
                self.check_upload(upload, poll_secs)

    def wait(self, poll_secs=0.5):
        incomplete = self.incomplete()
        logging.debug("Waiting for upload pool to complete %d uploads" % len(incomplete))
        while len(incomplete):
            self.check_uploads()
            incomplete = self.incomplete()
            sleep(poll_secs)
