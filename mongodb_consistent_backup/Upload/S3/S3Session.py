import logging

import boto
import boto.s3
from boto.s3.connection import OrdinaryCallingFormat, SubdomainCallingFormat

from mongodb_consistent_backup.Errors import OperationError


class S3Session:
    def __init__(self, region, access_key, secret_key, bucket_name, secure=True, num_retries=5, socket_timeout=15,
                 **kwargs):
        self.region          = region
        self.access_key      = access_key
        self.secret_key      = secret_key
        self.secure          = secure
        self.num_retries     = num_retries
        self.socket_timeout  = socket_timeout
        self.validate_bucket = kwargs.get("validate_bucket")
        # monkey patch for bucket_name with dots
        # https://github.com/boto/boto/issues/2836
        if self.secure and '.' in bucket_name:
            self.calling_format = OrdinaryCallingFormat()
        else:
            self.calling_format = SubdomainCallingFormat()

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

    def is_forbidden_error(self, e):
        if 'S3ResponseError: 403 Forbidden' in str(e):
            return True
        return False

    def connect(self):
        if not self._conn:
            try:
                if (self.access_key is not None and self.secret_key is not None):
                    logging.debug("Connecting to AWS S3 with Access Key: %s" % self.access_key)
                    self._conn = boto.s3.connect_to_region(
                        self.region,
                        aws_access_key_id=self.access_key,
                        aws_secret_access_key=self.secret_key,
                        is_secure=self.secure,
                        calling_format=self.calling_format
                    )
                    logging.debug("Successfully connected to AWS S3 with Access Key: %s" % self.access_key)
                else:
                    logging.debug("Connecting to AWS S3 with IAM Role")
                    self._conn = boto.s3.connect_to_region(
                        self.region,
                        is_secure=self.secure,
                        calling_format=self.calling_format
                    )
                    logging.debug("Successfully connected to AWS S3 with IAM Role")
            except boto.exception.S3ResponseError, e:
                if self.is_forbidden_error(e):
                    logging.error("Not authorized to connect to AWS S3 with Access Key: %s!" % self.access_key)
                else:
                    logging.error("Cannot connect to AWS S3 with Access Key: %s!" % self.access_key)
                return OperationError(e)
            except Exception, e:
                logging.error("Cannot connect to AWS S3 with Access Key: %s!" % self.access_key)
                raise OperationError(e)
        return self._conn

    def get_bucket(self, bucket_name):
        try:
            logging.debug("Connecting to AWS S3 Bucket: %s (%s validation)" % (bucket_name,
                                                                               "with" if self.validate_bucket
                                                                               else "without"))
            return self._conn.get_bucket(bucket_name, validate=self.validate_bucket)
        except boto.exception.S3ResponseError, e:
            if self.is_forbidden_error(e):
                logging.error("Got forbidden error from AWS S3 for bucket %s! Please check your access/secret key" % bucket_name)
            else:
                logging.error("Error with operation on AWS S3 Bucket %s: %s!" % (bucket_name, str(e).rstrip()))
            raise OperationError(str(e).rstrip())
        except Exception, e:
            logging.error("Cannot connect to AWS S3 Bucket: %s!" % bucket_name)
            raise OperationError(e)
