import os
import logging

from signal import signal, SIGINT, SIGTERM

from mongodb_consistent_backup.Common import LocalCommand


class Zbackup:
    def __init__(self, config, backup_dir):
        self.config     = config
        self.backup_dir = backup_dir

        self._command  = None
	self.compression_method = None
	self.compression_supported = ['lzma']

	self.zbackup_binary      = config.archive.zbackup.binary
	self.zbackup_dir         = config.archive.zbackup.dir
	self.zbackup_passwd_file = config.archive.zbackup.password_file
	self.zbackup_threads     = config.archive.zbackup.threads

        signal(SIGINT, self.close)
        signal(SIGTERM, self.close)

    def compression(self, method=None):
	# only lzma supported
        return 'lzma'

    def threads(self, count=None):
        if count:
	    config.archive.zbackup.threads = int(count)
	return config.archive.zbackup.threads

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Killing running subprocess/command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()

    def run(self):
	logging.info("This class: %s is a dummy!" % self.__class__)
        pass
