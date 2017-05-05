import boto
import logging
import os
import time

from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task


class GS(Task):
    def __init__(self):
        self.remove_uploaded = self.config.upload.remove_uploaded
        self.project_id      = self.config.upload.gs.project_id
        self.access_key      = self.config.upload.gs.access_key
        self.secret_key      = self.config.upload.gs.secret_key
        self.bucket_name     = self.config.upload.gs.bucket_name
        self.bucket_prefix   = self.config.upload.gs.bucket_prefix
        self.thread_count    = self.config.upload.gs.threads

        self.boto_scheme   = 'gs'
        self.header_values = {"x-goog-project-id": self.project_id}

    def init(self):
        try:
            if not boto.config.has_section("Credentials"):
                boto.config.add_section("Credentials")
            boto.config.set("Credentials", "gs_access_key_id", self.access_key)
            boto.config.set("Credentials", "gs_secret_access_key", self.secret_key)
            if not boto.config.has_section("Boto"):
                boto.config.add_section("Boto")
            boto.config.setbool('Boto', 'https_validate_certificates', True)
        except Exception, e:
            return OperationError("Error setting up boto for Google Cloud Storage: '%s'!" % e)
    
    def upload_file(self, filename):
        f = None
        if os.path.exists(filename):
            logging.info("Uploading file to GS: %s" % filename)
            try:
                path = os.path.join(self.bucket_name, self.bucket_prefix, os.path.basename(filename))
                f    = open(filename, 'r')
                uri  = boto.storage_uri(path, self.boto_scheme)
                uri.new_key().set_contents_from_file(f)
                return path
            except Exception, e:
                return OperationError("Failed to upload file to GS: %s" % filename)
            finally:
                if f:
                    f.close()
