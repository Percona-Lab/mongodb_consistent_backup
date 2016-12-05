import logging

from NSCA import NSCA


class Notify:
    def __init__(self, config):
        self.config = config

        self._notifier = None
        self.init()

    def init(self):
        if self.config.notify.method == "nsca":
            logging.info("Using notify method: nsca")
            try:
                self._notifier = NSCA(self.config)
            except Exception, e:
                raise e
        else:
            logging.info("Notifying disabled, skipping")

    def notify(self, message, success=False):
        if self._notifier:
            state = self._notifier.failed
            if success:
                state = self._notifier.success
            return self._notifier.notify(state, message)

    def close(self):
        if self._notifier:
            return self._notifier.close()
