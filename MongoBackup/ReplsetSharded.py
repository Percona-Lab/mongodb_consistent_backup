import logging

from Common import DB
from Sharding import Sharding


class ReplsetSharded:
    def __init__(self, sharding, db, user=None, password=None, authdb='admin', max_lag_secs=5):
        self.sharding     = sharding
        self.db           = db
        self.user         = user
        self.password     = password
        self.authdb       = authdb
        self.max_lag_secs = max_lag_secs

        self.replsets      = {} 
        self.replset_conns = {}

        # Check Sharding class:
        if not self.sharding.__class__.__name__ == "Sharding":
            raise Exception, "'sharding' field is an instance of %s, not 'Sharding'!" % self.sharding.__class__.__name__, None

        # Get a DB connection
        try:
            if self.db.__class__.__name__ == "DB":
                self.connection = self.db.connection()
                if not self.connection.is_mongos:
                    raise Exception, 'MongoDB connection is not to a mongos!', None
            else:
                raise Exception, "'db' field is an instance of %s, not 'DB'!" % self.db.__class__.__name__, None
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
                    self.replsets[shard_name] = Replset(rs_db, self.user, self.password, self.authdb, self.max_lag_secs)
                except Exception, e:
                    logging.fatal("Could not get Replset class object for replset %s! Error: %s" % (rs_name, e))
                    raise e
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
