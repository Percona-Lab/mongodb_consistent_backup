import logging

from pymongo import MongoClient
from time import sleep


class DB:
    def __init__(self, host='localhost', port=27017, username=None, password=None, authdb="admin", conn_timeout=5000, retries=5):
        self.host         = host
        self.port         = port
        self.username     = username
        self.password     = password
        self.authdb       = authdb
        self.conn_timeout = conn_timeout
        self.retries      = retries

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
                logging.debug("Authenticating connection with username: %s" % self.username)
                self._conn[self.authdb].authenticate(self.username, self.password)
            except Exception, e:
                logging.fatal("Unable to authenticate with host %s:%s: %s" % (self.host, self.port, e))
                raise e
        else:
            pass

    def admin_command(self, admin_command, retry=True, quiet=False):
        tries  = 0
        status = None
        while not status and tries < self.retries:
            try:
                status = self._conn['admin'].command(admin_command)
                if not status:
                    raise e
            except Exception, e:
                if not retry:
                    tries = self.retries
                if not quiet:
                    logging.error("Error running admin command '%s': %s" % (admin_command, e))
                tries += 1
                sleep(1)
        if not status:
            raise Exception, "Could not get output from command: '%s' after %i retries!" % (admin_command, retries), None
        return status

    def server_version(self):
        status  = self.admin_command('serverStatus')
        if 'version' in status:
            version = status['version'].split('-')[0]
            return tuple(version.split('.'))
        else:
            raise Exception, "Could not get server version using admin command 'serverStatus'! Error: %s"  % e, None

    def connection(self):
        return self._conn

    def close(self):
        return self._conn.close()
