import logging

from mongodb_consistent_backup.Common import DB, MongoUri
from mongodb_consistent_backup.Errors import DBConnectionError, Error
from mongodb_consistent_backup.Sharding import Sharding
from Replset import Replset


class ReplsetSharded:
    def __init__(self, config, sharding, db):
        self.config       = config
        self.sharding     = sharding
        self.db           = db
        self.user         = self.config.user
        self.password     = self.config.password
        self.authdb       = self.config.authdb
        self.max_lag_secs = self.config.replication.max_lag_secs

        self.replsets      = {} 
        self.replset_conns = {}

        # Check Sharding class:
        if not isinstance(self.sharding, Sharding):
            raise Error("'sharding' field is not an instance of class: 'Sharding'!")

        # Get a DB connection
        if isinstance(self.db, DB):
            self.connection = self.db.connection()
            if not self.connection.is_mongos:
                raise Error('MongoDB connection is not to a mongos!')
        else:
            raise Error("'db' field is not an instance of class: 'DB'!")

    def get_replset_connection(self, host, port, force=False):
        conn_name = "%s-%i" % (host, port)
        if force or not conn_name in self.replset_conns:
            self.replset_conns[conn_name] = DB(host, port, self.user, self.password, self.authdb)
        return self.replset_conns[conn_name]

    def get_replsets(self, force=False):
        for shard in self.sharding.shards():
            shard_uri = MongoUri(shard['host']).get()
            if force or not shard_uri.replset in self.replsets:
                rs_db = self.get_replset_connection(shard_uri.host, shard_uri.port)
                self.replsets[shard_uri.replset] = Replset(self.config, rs_db)

        configsvr = self.sharding.get_config_server()
        if configsvr and isinstance(configsvr, Replset):
            config_rs_name = configsvr.get_rs_name()
            self.replsets[config_rs_name] = configsvr

        return self.replsets

    def primary_optimes(self):
        primary_optimes = {}
        for rs_name in self.get_replsets():
            replset = self.replsets[rs_name]
            primary_optimes[rs_name] = replset.primary_optime()
        return primary_optimes

    def close(self):
        for rs_name in self.replsets:
            self.replsets[rs_name].close()
        for conn_name in self.replset_conns:
            self.replset_conns[conn_name].close()
