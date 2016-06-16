import logging

from pymongo import MongoClient


class DB:
    def __init__(self, host='localhost', port=27017, username=None, password=None, authdb="admin", conn_timeout=5000):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.authdb = authdb
        self.conn_timeout = conn_timeout
        self._conn = None
        self.connect()
        self.auth_if_required()

    def connect(self):
        try:
            logging.debug("Getting MongoDB connection to %s:%s" % (self.host, self.port))
            conn = MongoClient(
                host=self.host,
                port=int(self.port),
                connectTimeoutMS=int(self.conn_timeout)
            )
        except Exception, e:
            logging.fatal("Unable to connect to %s:%s! Error: %s" % (self.host, self.port, e))
            raise e
        if conn is not None:
            self._conn = conn
        return self._conn

    def auth_if_required(self):
        if self.username is not None and self.password is not None:
            try:
                logging.debug("Authenticating MongoDB connection")
                self._conn[self.authdb].authenticate(self.username, self.password)
            except Exception, e:
                logging.fatal("Unable to authenticate with host %s:%s: %s" % (self.host, self.port, e))
                raise e
        else:
            pass

    def connection(self):
        return self._conn

    def close(self):
        return self._conn.close()
