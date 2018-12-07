from mongodb_consistent_backup.Common import DB, MongoUri
from mongodb_consistent_backup.Errors import Error
from mongodb_consistent_backup.Sharding import Sharding
from Replset import Replset


class ReplsetSharded:
    def __init__(self, config, sharding, db):
        self.config       = config
        self.sharding     = sharding
        self.db           = db
        self.max_lag_secs = self.config.replication.max_lag_secs

        self.replsets      = {}
        self.replset_conns = {}

        # Check Sharding class:
        if not isinstance(self.sharding, Sharding):
            raise Error("'sharding' field is not an instance of class: 'Sharding'!")

        # Get a DB connection
        if isinstance(self.db, DB):
            self.connection = self.db.connection()
            if not self.db.is_mongos() and not self.db.is_configsvr():
                raise Error('MongoDB connection is not to a mongos or configsvr!')
        else:
            raise Error("'db' field is not an instance of class: 'DB'!")

    def summary(self):
        summary = {}
        for rs_name in self.get_replsets():
            summary[rs_name] = self.replsets[rs_name].summary()
        return summary

    def get_replset_connection(self, uri, force=False):
        if force or uri.replset not in self.replset_conns:
            self.replset_conns[uri.replset] = DB(uri, self.config, True)
        return self.replset_conns[uri.replset]

    def get_replsets(self, force=False):
        for shard in self.sharding.shards():
            shard_uri = MongoUri(shard['host'])
            if force or shard_uri.replset not in self.replsets:
                rs_db = self.get_replset_connection(shard_uri)
                self.replsets[shard_uri.replset] = Replset(self.config, rs_db)

        configsvr = self.sharding.get_config_server()
        if configsvr:
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
