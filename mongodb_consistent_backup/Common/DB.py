import logging

from bson.codec_options import CodecOptions
from inspect import currentframe, getframeinfo
from pymongo import DESCENDING, CursorType, MongoClient
from pymongo.errors import ConfigurationError, ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
from ssl import CERT_REQUIRED, CERT_NONE
from time import sleep

from mongodb_consistent_backup.Common import parse_config_bool
from mongodb_consistent_backup.Errors import DBAuthenticationError, DBConnectionError, DBOperationError, Error


def parse_read_pref_tags(tags_str):
    tags = {}
    for pair in tags_str.replace(" ", "").split(","):
        if ":" in pair:
            key, value = pair.split(":")
            tags[key] = str(value)
    return tags


class DB:
    def __init__(self, uri, config, do_replset=False, read_pref='primaryPreferred', do_rp_tags=False,
                 do_connect=True, conn_timeout=5000, retries=5):
        self.uri            = uri
        self.config         = config
        self.do_replset     = do_replset
        self.read_pref      = read_pref
        self.do_rp_tags     = do_rp_tags
        self.do_connect     = do_connect
        self.conn_timeout   = conn_timeout
        self.retries        = retries

        self.username             = self.config.username
        self.password             = self.config.password
        self.authdb               = self.config.authdb
        self.ssl_ca_file          = self.config.ssl.ca_file
        self.ssl_crl_file         = self.config.ssl.crl_file
        self.ssl_client_cert_file = self.config.ssl.client_cert_file
        self.read_pref_tags       = self.config.replication.read_pref_tags

        self.username             = self.config.username
        self.password             = self.config.password
        self.authdb               = self.config.authdb
        self.ssl_ca_file          = self.config.ssl.ca_file
        self.ssl_crl_file         = self.config.ssl.crl_file
        self.ssl_client_cert_file = self.config.ssl.client_cert_file

        self.replset    = None
        self._conn      = None
        self._is_master = None

        self.connect()
        self.auth_if_required()

    def do_ssl(self):
        return parse_config_bool(self.config.ssl.enabled)

    def do_ssl_insecure(self):
        return parse_config_bool(self.config.ssl.insecure)

    def client_opts(self):
        opts = {
            "connect":                  self.do_connect,
            "host":                     self.uri.hosts(),
            "connectTimeoutMS":         self.conn_timeout,
            "serverSelectionTimeoutMS": self.conn_timeout,
            "maxPoolSize":              1,
        }
        if self.do_replset:
            self.replset = self.uri.replset
            opts.update({
                "replicaSet":     self.replset,
                "readPreference": self.read_pref,
                "w":              "majority"
            })
            if self.do_rp_tags and self.read_pref_tags:
                logging.debug("Using read preference mode: %s, tags: %s" % (
                    self.read_pref,
                    parse_read_pref_tags(self.read_pref_tags)
                ))
                self.read_pref_tags = self.read_pref_tags.replace(" ", "")
                opts["readPreferenceTags"] = self.read_pref_tags

        if self.do_ssl():
            logging.debug("Using SSL-secured mongodb connection (ca_cert=%s, client_cert=%s, crl_file=%s, insecure=%s)" % (
                self.ssl_ca_file,
                self.ssl_client_cert_file,
                self.ssl_crl_file,
                self.do_ssl_insecure()
            ))
            opts.update({
                "ssl":           True,
                "ssl_ca_certs":  self.ssl_ca_file,
                "ssl_crlfile":   self.ssl_crl_file,
                "ssl_certfile":  self.ssl_client_cert_file,
                "ssl_cert_reqs": CERT_REQUIRED,
            })
            if self.do_ssl_insecure():
                opts["ssl_cert_reqs"] = CERT_NONE
        return opts

    def connect(self):
        try:
            logging.debug("Getting MongoDB connection to %s (replicaSet=%s, readPreference=%s, readPreferenceTags=%s, ssl=%s)" % (
                self.uri,
                self.replset,
                self.read_pref,
                self.do_rp_tags,
                self.do_ssl(),
            ))
            conn = MongoClient(**self.client_opts())
            if self.do_connect:
                conn['admin'].command({"ping": 1})
        except (ConfigurationError, ConnectionFailure, OperationFailure, ServerSelectionTimeoutError), e:
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

    def get_oplog_rs(self):
        if not self._conn:
            self.connect()
        db = self._conn['local']
        return db.oplog.rs.with_options(codec_options=CodecOptions(unicode_decode_error_handler="ignore"))

    def get_oplog_tail_ts(self):
        logging.debug("Gathering oldest 'ts' in %s oplog" % self.uri)
        return self.get_oplog_rs().find_one(sort=[('$natural', DESCENDING)])['ts']

    def get_oplog_cursor_since(self, caller, ts=None):
        frame   = getframeinfo(currentframe().f_back)
        comment = "%s:%s;%s:%i" % (caller.__name__, frame.function, frame.filename, frame.lineno)
        if not ts:
            ts = self.get_oplog_tail_ts()
        query = {'ts': {'$gte': ts}}
        logging.debug("Querying oplog on %s with query: %s" % (self.uri, query))
        # http://api.mongodb.com/python/current/examples/tailable.html
        return self.get_oplog_rs().find(query, cursor_type=CursorType.TAILABLE_AWAIT, oplog_replay=True).comment(comment)

    def close(self):
        if self._conn:
            logging.debug("Closing connection to: %s" % self.uri)
            return self._conn.close()
