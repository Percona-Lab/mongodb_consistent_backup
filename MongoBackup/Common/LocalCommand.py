import logging

from subprocess import Popen, PIPE
from time import sleep


class LocalCommand:
    def __init__(self, command, command_flags=[], verbose=False):
        self.command       = command
        self.command_flags = command_flags
        self.verbose       = verbose

        self.output   = []
        self._process = None

        self.command_line = [self.command]
        if len(self.command_flags):
            self.command_line.extend(self.command_flags)

    def parse_output(self):
        if self._process:
            try:
                stdout, stderr = self._process.communicate()
                output = stdout.strip()
                if output == "" and stderr.strip() != "":
                    output = stderr.strip()
                if not output == "":
                    self.output.append("\n\t".join(output.split("\n")))
            except Exception, e:
                raise Exception, "Error parsing output: %s" % e, None
        return self.output

    def run(self):
        try:
            self._process = Popen(self.command_line, stdout=PIPE, stderr=PIPE)
            while self._process.poll() is None:
                self.parse_output()
                sleep(0.1)
        except Exception, e:
            raise e
    
        if self._process.returncode != 0:
            raise Exception, "%s command failed with exit code %i! Stderr output:\n%s" % (
                self.command,
                self._process.returncode,
                "\n".join(self.output)
            ), None
        elif self.verbose:
            if len(self.output) > 0:
                logging.debug("%s command completed with output:\n\t%s" % (self.command, "\n".join(self.output)))
            else:
                logging.debug("%s command completed" % (self.command))

    def close(self):
        if self._process:
            self._process.terminate()
