import boto
import logging
import os
import time

from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task


class Gs(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(Gs, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.backup_location = self.config.backup.location
        self.remove_uploaded = self.config.upload.remove_uploaded
        self.project_id      = self.config.upload.gs.project_id
        self.access_key      = self.config.upload.gs.access_key
        self.secret_key      = self.config.upload.gs.secret_key
        self.bucket          = self.config.upload.gs.bucket

        self.meta_data_dir   = "mongodb-consistent-backup_META"
        self._header_values  = {"x-goog-project-id": self.project_id}
        self._key_meta_cache = {}

        self.init()

    def init(self):
        if not boto.config.has_section("Credentials"):
            boto.config.add_section("Credentials")
        boto.config.set("Credentials", "gs_access_key_id", self.access_key)
        boto.config.set("Credentials", "gs_secret_access_key", self.secret_key)
        if not boto.config.has_section("Boto"):
            boto.config.add_section("Boto")
        boto.config.setbool('Boto', 'https_validate_certificates', True)

    def close(self):
        pass

    def get_backup_files(self, base_dir=None, files=[]):
        if not base_dir:
            base_dir = self.backup_dir
        for child in os.listdir(base_dir):
            path = os.path.join(base_dir, child)
            if os.path.isfile(path):
                files.append(path)
            elif os.path.isdir(path):
                self.get_backup_files(path, files)
        return files

    def get_uri(self, path):
        return boto.storage_uri(path, 'gs')
    
    def object_exists(self, path):
        try:
            self.get_object_metadata(path)
            return True
        except boto.exception.InvalidUriError:
            pass
        return False
    
    def get_object_metadata(self, path, force=False):
        if force or not path in self._key_meta_cache:
            logging.debug("Getting metadata for path: %s" % path)
            uri = self.get_uri(path)
            self._key_meta_cache[path] = uri.get_key()
        if path in self._key_meta_cache:
            return self._key_meta_cache[path]
    
    def get_object_md5hash(self, path):
        key = self.get_object_metadata(path)
        if hasattr(key, 'etag'):
            return key.etag.strip('"\'')
    
    def get_file_md5hash(self, filename, blocksize=65536):
        md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for block in iter(lambda: f.read(blocksize), b""):
                md5.update(block)
        return md5.hexdigest()
    
    def upload(self, filename, path=None):
        if not path:
            path = filename
        path = "%s/%s" % (self.bucket, path)
        if self.object_exists(path):
            object_md5hash = self.get_object_md5hash(path)
            if object_md5hash and self.get_file_md5hash(filename) == object_md5hash:
                logging.debug("Path %s already exists with the same checksum (%s), skipping" % (path, object_md5hash))
                return
            logging.debug("Path %s checksum and local checksum differ, re-uploading" % path)
            return self.upload_object(path)
        logging.debug("Path %s does not exist, uploading" % path)
        return self.upload_object(filename, path)
    
    def upload_object(self, filename, path):
        f = None
        try:
            f   = open(filename, 'r')
            uri = self.get_uri(path)
            logging.info("Uploading object to Google Cloud Storage: %s" % path)
            uri.new_key().set_contents_from_file(f)
            self.handle_uploaded(filename)
        finally:
            if f:
                f.close()

    def handle_uploaded(self, local_path):
        if self.remove_uploaded and not local_path.startswith(os.path.join(self.backup_dir, self.meta_data_dir)):
            logging.debug("Removing successfully uploaded file: %s" % local_path)
            os.remove(local_path)

    def run(self):
        if not os.path.isdir(self.backup_dir):
            logging.error("The source directory: %s does not exist or is not a directory! Skipping Google Cloud Storage upload!" % self.backup_dir)
            return
        try:
            self.running = True
            self.timer.start(self.timer_name)
            logging.info("Uploading backup dir %s to Google Cloud Storage bucket: %s" % (self.backup_dir, self.bucket))
            for file_path in self.get_backup_files():
                gs_path = os.path.relpath(file_path, self.backup_location)
                self.upload(file_path, gs_path)
            self.exit_code = 0
            self.completed = True
        except Exception, e:
            logging.error("Uploading to Google Cloud Storage failed! Error: %s" % e)
            raise OperationError(e)
        finally:
            self.timer.stop(self.timer_name)
            self.stopped = True
