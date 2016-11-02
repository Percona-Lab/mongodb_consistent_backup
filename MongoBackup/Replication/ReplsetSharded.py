import logging

from MongoBackup.Common import DB
from MongoBackup.Sharding import Sharding
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
            raise Exception, "'sharding' field is not an instance of class: 'Sharding'!", None

        # Get a DB connection
        try:
            if isinstance(self.db, DB):
                self.connection = self.db.connection()
                if not self.connection.is_mongos:
                    raise Exception, 'MongoDB connection is not to a mongos!', None
            else:
                raise Exception, "'db' field is not an instance of class: 'DB'!", None
        except Exception, e:
            logging.fatal("Could not get DB connection! Error: %s" % e)
            raise e

    def get_replset_connection(self, host, port, force=False):
        conn_name = "%s-%i" % (host, port)
        if force or not conn_name in self.replset_conns:
            try:
                self.replset_conns[conn_name] = DB(host, port, self.user, self.password, self.authdb)
            except Exception, e:
                logging.fatal("Could not get DB connection to %s:%i! Error: %s" % (host, port, e))
                raise e
        return self.replset_conns[conn_name]

    def get_replsets(self, force=False):
        for shard in self.sharding.shards():
            shard_name, members = shard['host'].split('/')
            host, port = members.split(',')[0].split(":")
            port       = int(port)
            if force or not shard_name in self.replsets:
                try:
                    rs_db = self.get_replset_connection(host, port)
                    self.replsets[shard_name] = Replset(rs_db, self.config)
                except Exception, e:
                    logging.fatal("Could not get Replset class object for replset %s! Error: %s" % (shard_name, e))
                    raise e

        configsvr = self.sharding.get_config_server()
        if configsvr and isinstance(configsvr, Replset):
            config_rs_name = configsvr.get_rs_name()
            self.replsets[config_rs_name] = configsvr

        return self.replsets

    def find_secondaries(self):
        shard_secondaries = {}
        for rs_name in self.get_replsets():
            replset = self.replsets[rs_name]
            shard_secondaries[rs_name] = replset.find_secondary()
        return shard_secondaries

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
