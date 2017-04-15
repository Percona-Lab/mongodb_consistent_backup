from mongodb_consistent_backup.Archive.Tar import Tar
from mongodb_consistent_backup.Pipeline import Stage

from Tar import Tar
from Zbackup import Zbackup
from mongodb_consistent_backup.Common import Timer, config_to_string, parse_method


class Archive(Stage):
    def __init__(self, manager, config, timer, base_dir, backup_dir):
        super(Archive, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
        self.task = self.config.archive.method
        self.init()
