import logging

from NSCA import NotifyNSCA


class Notify:
    def __init__(self, config):
        self.config = config
	self._notifier = None

        if self.config.notify.method == "none":
            logger.info("Notifying disabled! Skipping.")
        elif self.config.notify.method == "nsca" and self.config.notify.nsca:
            if self.config.notify.nsca.server and self.config.notify.nsca.check_name:
                try:
                    self._notifier = NotifyNSCA(self.config)
                except Exception, e:
                    raise e

    def notify(self, message, success=False):
	if self._notifier:
            state = self._notifier.failed
            if success:
                state = self._notifier.success
            return self._notifier.notify(state, message)
