import os
import logging

from fcntl import flock, LOCK_EX, LOCK_NB

class Lock:
    def __init__(self, lock_file):
        self.lock_file = lock_file
    
        self._lock = None
        self.acquire()
    
    def acquire(self):
        try:
            self._lock = open(self.lock_file, "w")
            flock(self._lock, LOCK_EX | LOCK_NB)
            logging.debug("Acquired exclusive lock on file: %s" % self.lock_file)
            return self._lock
        except Exception, e:
            logging.debug("Error acquiring lock on file: %s" % self.lock_file)
            if self._lock:
                self._lock.close()
            raise Exception, "Could not acquire lock!", None
    
    def release(self):
        logging.debug("Releasing exclusive lock on file: %s" % self.lock_file)
        self._lock.close()
        return os.remove(self.lock_file)
