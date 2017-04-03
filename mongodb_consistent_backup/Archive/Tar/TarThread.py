import os
import logging
import sys

from signal import signal, SIGINT, SIGTERM

from mongodb_consistent_backup.Common import LocalCommand


class TarThread:
    def __init__(self, backup_dir, output_file, do_gzip=False, verbose=False, binary="tar"):
        self.backup_dir  = backup_dir
        self.output_file = output_file
        self.do_gzip     = do_gzip
        self.verbose     = verbose
        self.binary      = binary

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
        if os.path.isdir(self.backup_dir):
            if not os.path.isfile(self.output_file):
                try:
                     backup_base_dir  = os.path.dirname(self.backup_dir)
                     backup_base_name = os.path.basename(self.backup_dir)
        
                     log_msg   = "Archiving and compressing directory: %s" % self.backup_dir
                     cmd_flags = ["-C", backup_base_dir, "-cf", self.output_file, "--remove-files", backup_base_name]
        
                     if self.do_gzip:
                         log_msg   = "Archiving directory: %s" % self.backup_dir
                         cmd_flags = ["-C", backup_base_dir, "-czf", self.output_file, "--remove-files", backup_base_name]
        
                     logging.info(log_msg)
                     self._command = LocalCommand(self.binary, cmd_flags, self.verbose)
                     self._command.run()
                except Exception, e:
                    logging.fatal("Failed archiving file: %s! Error: %s" % (self.output_file, e))
                    sys.exit(1)
            else:
                logging.fatal("Output file: %s already exists!" % self.output_file)
                sys.exit(1)
