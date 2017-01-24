import os
import logging

from signal import signal, SIGINT, SIGTERM

from mongodb_consistent_backup.Common import LocalCommand


class Zbackup:
    def __init__(self, config, backup_dir):
        self.config     = config
        self.backup_dir = backup_dir

        self._command  = None

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Killing running subprocess/command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()

    def run(self):
	logging.info("This class: %s is a dummy!" % self.__class__)
        pass
