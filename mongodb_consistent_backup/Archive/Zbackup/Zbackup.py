import os
import logging

from signal import signal, SIGINT, SIGTERM
from subprocess import Popen, PIPE

from mongodb_consistent_backup.Common import LocalCommand


class Zbackup:
    def __init__(self, config, backup_dir):
        self.config     = config
        self.backup_dir = backup_dir

        self.compression_method = None
        self.compression_supported = ['lzma']
        self._command = None
        self._version = None

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

    def version(self):
        if self._version:
            return self._version
        else:
            try:
                cmd = Popen([self.zbackup_binary, "--help"], stderr=PIPE)
                stdout, stderr = cmd.communicate()
                if stderr:
                    line = stderr.split("\n")[0]
                    if " " in line:
                        fields  = line.split(" ")
                        version = fields[len(fields) - 1]
                        if len(version.split(".")) == 3:
                            self._version = version
                            return self._version
                return None
            except OSError, e:
                return None
            except Exception, e:
                raise e
    
    def has_zbackup(self):
        if self.version():
           return True
        return False

    def close(self, exit_code=None, frame=None):
        if self._command:
            logging.debug("Killing running Zbackup command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()

    def run(self):
        logging.info("This class: %s is a dummy!" % self.__class__)
        pass
