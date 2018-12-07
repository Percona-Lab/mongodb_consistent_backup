import logging
import os

from math import ceil
from shutil import rmtree
from time import time

from mongodb_consistent_backup.Errors import OperationError


class Rotate(object):
    def __init__(self, config, state_root, state_bkp):
        self.config      = config
        self.state_root  = state_root
        self.state_bkp   = state_bkp
        self.backup_name = self.config.backup.name
        self.max_backups = self.config.rotate.max_backups
        self.max_days    = self.config.rotate.max_days

        self.previous = None
        self.backups  = self.backups_by_unixts()
        self.latest   = state_bkp.get()

        self.base_dir         = os.path.join(self.config.backup.location, self.config.backup.name)
        self.latest_symlink   = os.path.join(self.base_dir, "latest")
        self.previous_symlink = os.path.join(self.base_dir, "previous")

        self.max_secs = 0
        if self.max_days > 0:
            seconds       = float(self.max_days) * 86400.00
            self.max_secs = int(ceil(seconds))

    def backups_by_unixts(self):
        backups = {}
        for name in self.state_root.backups:
            backup      = self.state_root.backups[name]
            backup_time = backup["updated_at"]
            backups[backup_time] = backup
            if not self.previous or backup_time > self.previous["updated_at"]:
                self.previous = backup
        return backups

    def remove(self, ts):
        if ts in self.backups:
            backup = self.backups[ts]
            try:
                logging.debug("Removing backup path: %s" % backup["path"])
                rmtree(backup["path"])
            except Exception, e:
                raise OperationError("Unable to remove backup path %s. %s" % (backup["path"], e))
            if self.previous == backup:
                self.previous = None
            del self.backups[ts]

    def rotate(self):
        if self.max_days == 0 and self.max_backups == 0:
            logging.info("Backup rotation is disabled, skipping")
            return
        logging.info("Rotating backups (max_backups=%i, max_days=%.2f)" % (self.max_backups, self.max_days))
        kept_backups = 1
        now = int(time())
        remove_backups = {}
        for ts in sorted(self.backups.iterkeys(), reverse=True):
            backup = self.backups[ts]
            name   = backup["name"].encode("ascii", "ignore")
            if self.max_backups == 0 or kept_backups < self.max_backups:
                if self.max_secs > 0 and (now - ts) > self.max_secs:
                    remove_backups[name] = ts
                    continue
                logging.debug("Keeping previous backup %s" % name)
                kept_backups += 1
            else:
                remove_backups[name] = ts
        if len(remove_backups) > 0:
            logging.info("Backup(s) exceeds max backup count or age, removing: %s" % sorted(remove_backups.keys()))
            for name in remove_backups:
                self.remove(remove_backups[name])

    def symlink(self):
        try:
            if os.path.islink(self.latest_symlink):
                os.remove(self.latest_symlink)
            logging.info("Updating %s latest symlink to current backup path: %s" % (self.backup_name, self.latest["path"]))
            os.symlink(self.latest["path"], self.latest_symlink)

            if os.path.islink(self.previous_symlink):
                os.remove(self.previous_symlink)
            if self.previous:
                logging.info("Updating %s previous symlink to: %s" % (self.backup_name, self.previous["path"]))
                os.symlink(self.previous["path"], self.previous_symlink)
        except Exception, e:
            logging.error("Error creating backup symlinks: %s" % e)
            raise OperationError(e)

    def run(self):
        self.rotate()
        self.symlink()
