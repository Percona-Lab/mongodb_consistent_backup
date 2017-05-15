import logging

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
from time import sleep

from mongodb_consistent_backup.Errors import DBAuthenticationError, DBConnectionError, DBOperationError, Error, OperationError


class DB:
    def __init__(self, uri, config, do_replset=False, read_pref='primaryPreferred', do_connect=True, conn_timeout=5000, retries=5):
        self.uri          = uri
        self.username     = config.username
        self.password     = config.password
        self.authdb       = config.authdb
        self.do_replset   = do_replset
        self.read_pref    = read_pref
        self.do_connect   = do_connect
        self.conn_timeout = conn_timeout
        self.retries      = retries

        self.replset    = None
        self._conn      = None
        self._is_master = None
        self.connect()
        self.auth_if_required()

    def connect(self):
        try:
            if self.do_replset:
                self.replset = self.uri.replset
            logging.debug("Getting MongoDB connection to %s (replicaSet=%s, readPreference=%s)" % 
                         (self.uri, self.replset, self.read_pref))
            conn = MongoClient(
                connect=self.do_connect,
                host=self.uri.hosts(),
                replicaSet=self.replset,
                readPreference=self.read_pref,
                connectTimeoutMS=self.conn_timeout,
                serverSelectionTimeoutMS=self.conn_timeout,
                maxPoolSize=1,
                w="majority"
            )
            if self.do_connect:
                conn['admin'].command({"ping":1})
        except (ConnectionFailure, OperationFailure, ServerSelectionTimeoutError), e:
            logging.error("Unable to connect to %s! Error: %s" % (self.uri, e))
            raise DBConnectionError(e)
        if conn is not None:
            self._conn = conn
        return self._conn

    def auth_if_required(self):
        if self.username is not None and self.password is not None:
            try:
                logging.debug("Authenticating connection with username: %s" % self.username)
                self._conn[self.authdb].authenticate(self.username, self.password)
            except OperationFailure, e:
                logging.fatal("Unable to authenticate with host %s: %s" % (self.uri, e))
                raise DBAuthenticationError(e)
        else:
            pass

    def admin_command(self, admin_command, quiet=False):
        tries  = 0
        status = None
        while not status and tries < self.retries:
            try:
                status = self._conn['admin'].command(admin_command)
            except OperationFailure, e:
                if not quiet:
                    logging.error("Error running admin command '%s': %s" % (admin_command, e))
                tries += 1
                sleep(1)
        if not status:
            raise DBOperationError("Could not get output from command: '%s' after %i retries!" % (admin_command, self.retries))
        return status

    def server_version(self):
        status = self.admin_command('serverStatus')
        try:
            if 'version' in status:
                version = status['version'].split('-')[0]
                return tuple(version.split('.'))
        except Exception, e:
            raise Error("Unable to determine version from serverStatus! Error: %s" % e)

    def connection(self):
        return self._conn

    def is_mongos(self):
        return self._conn.is_mongos

    def is_master(self, force=False):
        try:
            if force or not self._is_master:
                self._is_master = self.admin_command('isMaster', True)
        except OperationFailure, e:
            raise DBOperationError("Unable to run isMaster command! Error: %s" % e)
        return self._is_master

    def is_replset(self):
        isMaster = self.is_master()
        if 'setName' in isMaster and isMaster['setName'] != "":
            return True
        return False

    def is_configsvr(self):
        isMaster = self.is_master()
        if 'configsvr' in isMaster and isMaster['configsvr']:
            return True
        return False

    def replset(self):
        isMaster = self.is_master()
        if 'setName' in isMaster:
            return isMaster['setName']
        return None

    def close(self):
        if self._conn:
            logging.debug("Closing connection to: %s" % self.uri)
            return self._conn.close()
