import logging

import boto
import boto.s3

from mongodb_consistent_backup.Errors import OperationError

class S3Session:
    def __init__(self, access_key, secret_key, s3_host='s3.amazonaws.com', secure=True, num_retries=5, socket_timeout=15):
        self.access_key     = access_key
        self.secret_key     = secret_key
        self.s3_host        = s3_host
        self.secure         = secure
        self.num_retries    = num_retries
        self.socket_timeout = socket_timeout

        for section in boto.config.sections():
            boto.config.remove_section(section)
        boto.config.add_section('Boto')
        boto.config.setbool('Boto', 'is_secure', self.secure)
        boto.config.set('Boto', 'http_socket_timeout', str(self.socket_timeout))
        boto.config.set('Boto', 'num_retries', str(self.num_retries))

        self._conn = None
        self.connect()

    def close(self):
        if not self._conn:
            self._conn.close()
        pass

    def connect(self):
        if not self._conn:
            try:
                logging.debug("Connecting to AWS S3 with Access Key: %s" % self.access_key)
                self._conn = boto.s3.S3Connection(
                    self.access_key,
                    self.secret_key,
                    host=self.s3_host,
                    is_secure=self.secure
                )
                logging.debug("Successfully connected to AWS S3 with Access Key: %s" % self.access_key)
            except Exception, e:
                logging.error("Cannot connect to AWS S3 with Access Key: %s!" % self.access_key)
                raise OperationError(e)
        return self._conn

    def get_bucket(self, bucket_name):
        try:
            logging.debug("Connecting to AWS S3 Bucket: %s" % bucket_name)
            return self._conn.get_bucket(bucket_name)
        except Exception, e:
            logging.error("Cannot connect to AWS S3 Bucket: %s!" % bucket_name)
            raise OperationError(e)
