import os
import logging

from fcntl import flock, LOCK_EX, LOCK_NB
from mongodb_consistent_backup.Errors import OperationError


class Lock:
    def __init__(self, lock_file, acquire=True):
        self.lock_file = lock_file

        self._lock = None
        if acquire:
            self.acquire()

    def acquire(self):
        try:
            self._lock = open(self.lock_file, "w")
            flock(self._lock, LOCK_EX | LOCK_NB)
            logging.debug("Acquired exclusive lock on file: %s" % self.lock_file)
            return self._lock
        except Exception:
            logging.debug("Error acquiring lock on file: %s" % self.lock_file)
            if self._lock:
                self._lock.close()
            raise OperationError("Could not acquire lock on file: %s!" % self.lock_file)

    def release(self):
        if self._lock:
            logging.debug("Releasing exclusive lock on file: %s" % self.lock_file)
            self._lock.close()
            self._lock = None
            return os.remove(self.lock_file)
