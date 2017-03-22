import json
import logging


class OplogState:
    def __init__(self, manager, uri, oplog_file=None):
	self.uri = uri
        self.oplog_file = oplog_file

        self._state = manager.dict()
        self._state['uri'] = self.uri.str()
        self._state['file'] = self.oplog_file
        self._state['count'] = 0
        self._state['first_ts'] = None
        self._state['last_ts'] = None
        self._state['running'] = False

    def state(self):
        return self._state

    def get(self, key=None):
        state = self._state.copy()
        if key:
            if key in state:
                return state[key]
            else:
                return None
        return state 

    def set(self, key, value):
        self._state[key] = value

    def write(self, file_name):
        f = None
        try:
            f = open(file_name, "w+")
            f.write(json.dumps(self._state))
        except Exception, e:
            logging.debug("Writing oplog state to file: '%s'! Error: %s" % (self.oplog_file, e))
            raise e 
        finally:
            if f:
                f.close()
        return True
