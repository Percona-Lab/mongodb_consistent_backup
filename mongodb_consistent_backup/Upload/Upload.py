from mongodb_consistent_backup.Upload.GS import GS
from mongodb_consistent_backup.Upload.S3 import S3
from mongodb_consistent_backup.Pipeline import Stage


class Upload(Stage):
    def __init__(self, manager, config, timer, base_dir, backup_dir):
        super(Upload, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
        self.task = self.config.upload.method
        self.init()
