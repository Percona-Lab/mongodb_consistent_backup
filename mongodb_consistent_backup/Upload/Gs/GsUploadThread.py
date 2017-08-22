import boto
import hashlib
import logging
import os

from mongodb_consistent_backup.Errors import OperationError


class GsUploadThread:
    def __init__(self, backup_dir, file_path, gs_path, bucket, project_id, access_key, secret_key, remove_uploaded=False):
        self.backup_dir      = backup_dir
        self.file_path       = file_path
        self.gs_path         = gs_path
        self.bucket          = bucket
        self.project_id      = project_id
        self.access_key      = access_key
        self.secret_key      = secret_key
        self.remove_uploaded = remove_uploaded

        self.path          = "%s/%s" % (self.bucket, self.gs_path)
        self.meta_data_dir = "mongodb_consistent_backup-META"
        self._metadata     = None

    def configure(self):
        if not boto.config.has_section("Credentials"):
            boto.config.add_section("Credentials")
        boto.config.set("Credentials", "gs_access_key_id", self.access_key)
        boto.config.set("Credentials", "gs_secret_access_key", self.secret_key)
        if not boto.config.has_section("Boto"):
            boto.config.add_section("Boto")
        boto.config.setbool('Boto', 'https_validate_certificates', True)

    def get_uri(self):
        return boto.storage_uri(self.path, 'gs')

    def exists(self):
        try:
            self.metadata()
            return True
        except boto.exception.InvalidUriError:
            return False

    def metadata(self):
        logging.debug("Getting metadata for path: %s" % self.path)
        if not self._metadata:
            self._metadata = self.get_uri().get_key()
        return self._metadata

    def gs_md5hash(self):
        key = self.metadata()
        if hasattr(key, 'etag'):
            return key.etag.strip('"\'')

    def file_md5hash(self, blocksize=65536):
        md5 = hashlib.md5()
        with open(self.file_path, "rb") as f:
            for block in iter(lambda: f.read(blocksize), b""):
                md5.update(block)
        return md5.hexdigest()

    def success(self):
        if self.remove_uploaded and not self.file_path.startswith(os.path.join(self.backup_dir, self.meta_data_dir)):
            logging.debug("Removing successfully uploaded file: %s" % self.file_path)
            os.remove(self.file_path)

    def run(self):
        f = None
        try:
            self.configure()
            if self.exists():
                if self.gs_md5hash() and self.file_md5hash() == self.gs_md5hash():
                    logging.debug("Path %s already exists with the same checksum (%s), skipping" % (self.path, self.gs_md5hash()))
                    return
                logging.debug("Path %s checksum and local checksum differ, re-uploading" % self.path)
            else:
                logging.debug("Path %s does not exist, uploading" % self.path)

            try:
                f   = open(self.file_path, 'r')
                uri = self.get_uri()
                logging.info("Uploading %s to Google Cloud Storage" % self.path)
                uri.new_key().set_contents_from_file(f)
            finally:
                if f:
                    f.close()
            self.success()
        except Exception, e:
            logging.error("Uploading to Google Cloud Storage failed! Error: %s" % e)
            raise OperationError(e)
