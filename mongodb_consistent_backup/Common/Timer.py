from time import time

from mongodb_consistent_backup.Errors import OperationError


class Timer:
    def __init__(self, manager):
        self.timers = manager.dict()

    def start(self, timer_name):
        self.timers[timer_name] = {'start': time(), 'started': True}

    def stop(self, timer_name):
        try:
            if timer_name in self.timers and 'started' in self.timers[timer_name]:
                timer = self.timers.copy()[timer_name]
                del timer['started']
                timer['end'] = time()
                timer['stopped'] = True
                timer['duration'] = timer['end'] - timer['start']
                self.timers[timer_name] = timer
            else:
                raise OperationError("No started timer named %s to stop!" % timer_name)
        except IOError:
            pass

    def duration(self, timer_name):
        try:
            if timer_name in self.timers and 'duration' in self.timers[timer_name]:
                return self.timers[timer_name]['duration']
            return 0
        except IOError:
            return 0

    def dump(self, timer_name=None):
        if timer_name and timer_name in self.timers:
            return self.timers.copy()[timer_name]
        return self.timers.copy()
