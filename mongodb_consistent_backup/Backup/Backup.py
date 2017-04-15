from mongodb_consistent_backup.Backup.Mongodump import Mongodump
from mongodb_consistent_backup.Pipeline import Stage


class Backup(Stage):
    def __init__(self, manager, config, timer, base_dir, backup_dir, replsets, sharding=None):
        super(Backup, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, replsets=replsets, sharding=sharding)
        self.task = self.config.backup.method
        self.init()
