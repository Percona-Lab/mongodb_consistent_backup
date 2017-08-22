import json
import logging

from mongodb_consistent_backup.Errors import OperationError


class OplogState:
    def __init__(self, manager, uri, oplog_file=None):
        self.uri = uri
        self.oplog_file = oplog_file

        try:
            self._state = manager.dict()
            if uri:
                self._state['uri'] = self.uri.str()
            self._state['file'] = self.oplog_file
            self._state['count'] = 0
            self._state['first_ts'] = None
            self._state['last_ts'] = None
            self._state['running'] = False
            self._state['completed'] = False
        except Exception, e:
            raise OperationError(e)

    def state(self):
        return self._state

    def get(self, key=None):
        try:
            state = self._state.copy()
            if key:
                if key in state:
                    return state[key]
                else:
                    return None
            return state
        except IOError:
            return None
        except Exception, e:
            raise OperationError(e)

    def set(self, key, value, merge=False):
        try:
            if merge and isinstance(value, dict):
                for key in value:
                    self._state[key] = value[key]
            else:
                self._state[key] = value
        except IOError, e:
            pass
        except Exception, e:
            raise OperationError(e)

    def write(self, file_name):
        f = None
        try:
            f = open(file_name, "w+")
            f.write(json.dumps(self._state))
        except Exception, e:
            logging.debug("Writing oplog state to file: '%s'! Error: %s" % (self.oplog_file, e))
            raise OperationError(e)
        finally:
            if f:
                f.close()
        return True
