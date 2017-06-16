import os
import logging

from mongodb_consistent_backup.Common import LocalCommand
from mongodb_consistent_backup.Pipeline import PoolThread


class TarThread(PoolThread):
    def __init__(self, backup_dir, output_file, compression='none', verbose=False, binary="tar"):
        super(TarThread, self).__init__(self.__class__.__name__, compression)
        self.compression_method = compression
        self.backup_dir         = backup_dir
        self.output_file        = output_file
        self.verbose            = verbose
        self.binary             = binary

        self._command = None

    def close(self, exit_code=None, frame=None):
        if self._command and not self.stopped:
            logging.debug("Stopping running tar command: %s" % self._command.command)
            del exit_code
            del frame
            self._command.close()
            self.stopped = True

    def run(self):
        if os.path.isdir(self.backup_dir):
            if not os.path.isfile(self.output_file):
                try:
                     backup_base_dir  = os.path.dirname(self.backup_dir)
                     backup_base_name = os.path.basename(self.backup_dir)
        
                     log_msg   = "Archiving directory: %s" % self.backup_dir
                     cmd_flags = ["-C", backup_base_dir, "-cf", self.output_file, "--remove-files", backup_base_name]
        
                     if self.do_gzip():
                         log_msg   = "Archiving and compressing directory: %s" % self.backup_dir
                         cmd_flags = ["-C", backup_base_dir, "-czf", self.output_file, "--remove-files", backup_base_name]
        
                     logging.info(log_msg)
                     self.running  = True
                     self._command = LocalCommand(self.binary, cmd_flags, self.verbose)
                     self.exit_code = self._command.run()
                except Exception, e:
                    logging.fatal("Failed archiving file: %s! Error: %s" % (self.output_file, e))
                finally:
                    self.running   = False
                    self.stopped   = True
                    self.completed = True
            else:
                logging.fatal("Output file: %s already exists!" % self.output_file)
            return self.backup_dir
