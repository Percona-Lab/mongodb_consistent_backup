import logging

from mongodb_consistent_backup.Common import config_to_string, parse_submodule


class Notify:
    def __init__(self, config):
        self.config    = config

        self.method    = None
        self._notifier = None
        self.init()

    def init(self):
        notify_method = self.config.notify.method
        if not notify_method or parse_submodule(notify_method) == "none":
            logging.info("Notifying disabled, skipping")
        else:
            self.method   = parse_submodule(notify_method)
            config_string = config_to_string(self.config.notify[self.method])
            logging.info("Using notify method: %s (options: %s)" % (self.method, config_string))
            try:
                self._notifier = globals()[self.method.capitalize()](self.config)
            except LookupError, e:
                raise Exception, 'No notify method: %s' % self.method, None
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
