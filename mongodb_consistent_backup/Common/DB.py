import logging
import pymongo

from time import sleep

from mongodb_consistent_backup.Errors import DBAuthenticationError, DBConnectionError, DBOperationError


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
            conn = pymongo.MongoClient(
                host=self.host,
                port=int(self.port),
                connectTimeoutMS=int(self.conn_timeout)
            )
            conn['admin'].command({"ping":1})
        except (pymongo.errors.ConnectionFailure, pymongo.errors.OperationFailure, pymongo.errors.ServerSelectionTimeoutError), e:
            logging.fatal("Unable to connect to %s:%s! Error: %s" % (self.host, self.port, e))
            raise DBConnectionError(e)
        if conn is not None:
            self._conn = conn
        return self._conn

    def auth_if_required(self):
        if self.username is not None and self.password is not None:
            try:
                logging.debug("Authenticating connection with username: %s" % self.username)
                self._conn[self.authdb].authenticate(self.username, self.password)
            except pymongo.errors.OperationFailure, e:
                logging.fatal("Unable to authenticate with host %s:%s: %s" % (self.host, self.port, e))
                raise DBAuthenticationError(e)
            except Exception, e:
                raise e
        else:
            pass

    def admin_command(self, admin_command, quiet=False):
        tries  = 0
        status = None
        while not status and tries < self.retries:
            try:
                status = self._conn['admin'].command(admin_command)
            except pymongo.errors.OperationFailure, e:
                if not quiet:
                    logging.error("Error running admin command '%s': %s" % (admin_command, e))
                tries += 1
                sleep(1)
            except Exception, e:
                raise e
        if not status:
            raise DBOperationError("Could not get output from command: '%s' after %i retries!" % (admin_command, self.retries))
        return status

    def server_version(self):
        status  = self.admin_command('serverStatus')
        try:
            if 'version' in status:
                version = status['version'].split('-')[0]
                return tuple(version.split('.'))
        except pymongo.errors.OperationFailure, e:
            raise DBOperationError("Unable to determine version from serverStatus! Error: %s" % e)

    def connection(self):
        return self._conn

    def close(self):
        if self._conn:
            return self._conn.close()
