import logging

from subprocess import Popen, PIPE
from time import sleep


class LocalCommand:
    def __init__(self, command, command_flags=[], verbose=False):
        self.command       = command
        self.command_flags = command_flags
        self.verbose       = verbose

        self.output   = ""
        self.stdout   = None
        self.stderr   = None
        self._process = None

        self.command_line = [self.command]
        if len(self.command_flags):
            self.command_line.extend(self.command_flags)

    def parse_output(self):
        if self._process:
            try:
                self.stdout, self.stderr = self._process.communicate()
                output = self.stdout.strip()
                if output == "" and self.stderr.strip() != "":
                    output = self.stderr.strip()
                self.output = "\n\t".join(output.split("\n"))
            except Exception, e:
                raise Exception, "Error parsing output: %s" % e, None
        return self.output

    def run(self):
        try:
            self._process = Popen(self.command_line, stdout=PIPE, stderr=PIPE)
            while self._process.poll() is None:
                sleep(0.5)
        except Exception, e:
            raise e
    
        self.parse_output()
        if self._process.returncode != 0:
            raise Exception, "%s command failed with exit code %i! Stderr output:\n%s" % (
                self.command,
                self._process.returncode,
                self.stderr.strip()
            ), None
        elif self.verbose:
            if self.output == "":
                logging.debug("%s command completed" % (self.command))
            else:
                logging.debug("%s command completed with output:\n\t%s" % (self.command, self.output))

    def close(self):
        if self._process:
            self._process.terminate()
