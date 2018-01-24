import exceptions
import httplib
import logging
import socket

from boto.s3.key import Key
from filechunkio import FileChunkIO
from progress.bar import Bar
from time import sleep

from S3Session import S3Session

from mongodb_consistent_backup.Errors import OperationError


class S3ProgressBar(Bar):
    width  = 24
    suffix = "%(index).1f/%(max).1fmb"
    bar_prefix = ' ['
    bar_suffix = '] '

    def writeln(self, line):
        if self.index > 0:
            self.status(line)

    def status(self, line):
        logging.info(line)


class S3UploadThread:
    def __init__(self, bucket_name, region, access_key, secret_key, file_name, key_name, byte_count, multipart_id=None,
                 multipart_num=None, multipart_parts=None, multipart_offset=None, retries=5, secure=True, retry_sleep_secs=1):
        self.bucket_name      = bucket_name
        self.region           = region
        self.access_key       = access_key
        self.secret_key       = secret_key
        self.file_name        = file_name
        self.key_name         = key_name
        self.byte_count       = byte_count
        self.multipart_id     = multipart_id
        self.multipart_num    = multipart_num
        self.multipart_parts  = multipart_parts
        self.multipart_offset = multipart_offset
        self.retries          = retries
        self.secure           = secure
        self.retry_sleep_secs = retry_sleep_secs
        self.do_stop          = False

        progress_key_name = self.short_key_name(self.key_name)
        if self.multipart_num and self.multipart_parts:
            progress_key_name = "%s %d/%d" % (self.short_key_name(self.key_name), self.multipart_num, self.multipart_parts)
        self._progress    = S3ProgressBar(progress_key_name, max=float(self.byte_count / 1024.00 / 1024.00))
        self._last_bytes  = None

        try:
            self.s3_conn = S3Session(self.region, self.access_key, self.secret_key, self.bucket_name, self.secure, self.retries)
            self.bucket  = self.s3_conn.get_bucket(self.bucket_name)
        except Exception, e:
            logging.fatal("Could not get AWS S3 connection to bucket %s! Error: %s" % (self.bucket_name, e))
            raise OperationError("Could not get AWS S3 connection to bucket")

    def close(self, code=None, frame=None):
        self.do_stop = True

    def short_key_name(self, key_name):
        if "/" not in key_name:
            return None
        path   = key_name.split('/')
        fields = len(path)
        if fields > 4:
            return "/" + "/".join([path[1], "...", path[fields - 2], path[fields - 1]])
        else:
            return key_name

    def status(self, bytes_uploaded, bytes_total):
        self._progress.max = float(bytes_total / 1024.00 / 1024.00)
        update_bytes = bytes_uploaded
        if self._last_bytes:
            update_bytes = bytes_uploaded - self._last_bytes
        if update_bytes > 0:
            self._progress.next(float(update_bytes / 1024.00 / 1024.00))
        self._last_bytes = bytes_uploaded

    def run(self):
        try:
            tries     = 0
            exception = None
            while tries < self.retries:
                if self.do_stop:
                    break
                try:
                    if self.multipart_id and self.multipart_num and self.multipart_parts:
                        for mp in self.bucket.get_all_multipart_uploads():
                            if mp.id == self.multipart_id:
                                logging.info("Uploading AWS S3 key: s3://%s%s (multipart: %d/%d, size: %.2fmb)" % (
                                    self.bucket_name,
                                    self.short_key_name(self.key_name),
                                    self.multipart_num,
                                    self.multipart_parts,
                                    float(self.byte_count / 1024.00 / 1024.00)
                                ))
                                with FileChunkIO(self.file_name, 'r', offset=self.multipart_offset, bytes=self.byte_count) as fp:
                                    mp.upload_part_from_file(fp=fp, cb=self.status, num_cb=10, part_num=self.multipart_num)
                                break
                    else:
                        key = None
                        try:
                            logging.info("Uploading AWS S3 key: %s (multipart: None, size: %.2fmb)" % (
                                self.short_key_name(self.key_name),
                                float(self.byte_count / 1024.00 / 1024.00)
                            ))
                            key = Key(bucket=self.bucket, name=self.key_name)
                            key.set_contents_from_filename(self.file_name, cb=self.status, num_cb=10)
                        finally:
                            if key:
                                key.close()
                    break
                except (httplib.HTTPException, exceptions.IOError, socket.error, socket.gaierror) as e:
                    logging.error("Got exception during upload: '%s', retrying upload" % e)
                    exception = e
                finally:
                    sleep(self.retry_sleep_secs)
                    tries += 1
            if tries >= self.retries and exception:
                raise exception
        except Exception as e:
            logging.fatal("AWS S3 upload failed after %i retries! Error: %s" % (self.retries, e))
            raise e

        return self.file_name, self.key_name, self.multipart_num
