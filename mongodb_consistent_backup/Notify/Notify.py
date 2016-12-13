import logging

class Notify:
    def __init__(self, config):
        self.config = config
        self._notifier = None
        self.init()

    def init(self):
        notify_method = self.config.notify.method
        if notify_method is None:
            logging.info("Notifying disabled, skipping")

        logging.info("Using notify method: %s" % notify_method)
        try:
            self._notifier = globals()[notify_method](self.config)
        except Exception, e:
            raise e


    def notify(self, message, success=False):
        if self._notifier:
            state = self._notifier.failed
            if success:
                state = self._notifier.success
            return self._notifier.notify(state, message)

    def close(self):
        if self._notifier:
            return self._notifier.close()
