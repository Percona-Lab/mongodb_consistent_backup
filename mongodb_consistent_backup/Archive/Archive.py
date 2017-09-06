from mongodb_consistent_backup.Archive.Tar import Tar  # NOQA
from mongodb_consistent_backup.Archive.Zbackup import Zbackup  # NOQA
from mongodb_consistent_backup.Pipeline import Stage


class Archive(Stage):
    def __init__(self, manager, config, timer, base_dir, backup_dir):
        super(Archive, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
        self.task = self.config.archive.method
        self.init()
