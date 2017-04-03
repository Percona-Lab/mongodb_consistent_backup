import logging

from mongodb_consistent_backup.Common import config_to_string, parse_method
from mongodb_consistent_backup.Errors import Error, OperationError
from mongodb_consistent_backup.Notify.Nsca import Nsca


class Notify:
    def __init__(self, config, timer):
        self.config = config
        self.timer  = timer

        self.timer_name = self.__class__.__name__
        self.method     = None
        self._notifier  = None
        self.init()

    def init(self):
        notify_method = self.config.notify.method
        if not notify_method or parse_method(notify_method) == "none":
            logging.info("Notifying disabled, skipping")
        else:
            self.method   = parse_method(notify_method)
            config_string = config_to_string(self.config.notify[self.method])
            logging.info("Using notify method: %s (options: %s)" % (self.method, config_string))
            try:
                self._notifier = globals()[self.method.capitalize()](
                    self.config,
                    self.timer
                )
            except LookupError, e:
                raise OperationError('No notify method: %s' % self.method)

    def notify(self, message, success=False):
        if self._notifier:
            self.timer.start(self.timer_name)
            state = self._notifier.failed
            if success:
                state = self._notifier.success
            result = self._notifier.notify(state, message)
            self.timer.stop(self.timer_name)
            return result

    def close(self):
        if self._notifier:
            return self._notifier.close()
