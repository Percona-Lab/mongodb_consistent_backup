import logging

from mongodb_consistent_backup.Notify.Nsca import Nsca
from mongodb_consistent_backup.Pipeline import Stage


class Notify(Stage):
    def __init__(self, manager, config, timer, base_dir, backup_dir):
        super(Notify, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir)
        self.task = self.config.notify.method

        self.completed = False
        self.notifications = []
        self.init()

    def notify(self, message, success=False):
        notification = (success, message)
        self.notifications.append(notification)

    def run(self, *args):
        if self._task and len(self.notifications) > 0:
            logging.info("Sending %i notification(s) to: %s" % (len(self.notifications), self._task.server))
            self.timers.start(self.stage)
            while len(self.notifications) > 0:
                try:
                    (success, message) = self.notifications.pop()
                    state = self._task.failed
                    if success:
                        state = self._task.success
                    self._task.run(success, message)
                except:
                    continue
            self.timers.stop(self.stage)
        self.completed = True

    def close(self):
        if self._task:
            return self._task.close()
