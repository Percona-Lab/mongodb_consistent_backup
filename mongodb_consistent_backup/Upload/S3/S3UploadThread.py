import logging

from filechunkio import FileChunkIO
# TODO-timv Why do we have this if its not used?
from S3Session import S3Session


class S3UploadThread:
    def __init__(self, bucket_name, access_key, secret_key, s3_host, multipart_id, part_num, file_name, offset,
                 byte_count, retries=5, secure=True):
        self.bucket_name  = bucket_name
        self.access_key   = access_key
        self.secret_key   = secret_key
        self.s3_host      = s3_host
        self.multipart_id = multipart_id
        self.part_num     = part_num
        self.file_name    = file_name
        self.offset       = offset
        self.byte_count   = byte_count
        self.retries      = retries
        self.secure       = secure

        try:
            # TODO-timv S3 looks to be missing did you forget to import it maybe?
            self.s3_conn = S3(self.access_key, self.secret_key, self.s3_host, self.secure, self.retries)
            self.bucket  = self.s3_conn.get_bucket(self.bucket_name)
        except Exception, e:
            logging.error("Could not get AWS S3 connection to bucket %s! Error: %s" % (self.bucket_name, e))
            raise e

    def run(self):
        try:
            for mp in self.bucket.get_all_multipart_uploads():
                if mp.id == self.multipart_id:
                    logging.info("Uploading file: %s (part num: %s)" % (self.file_name, self.part_num))
                    with FileChunkIO(self.file_name, 'r', offset=self.offset, bytes=self.byte_count) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=self.part_num)
                    logging.debug("Uploaded file: %s (part num: %s)" % (self.file_name, self.part_num))
                    break
        except Exception, e:
            logging.error("AWS S3 multipart upload failed after %i retries! Error: %s" % (self.retries, e))
            raise e
