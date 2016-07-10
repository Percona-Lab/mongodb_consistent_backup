import logging

from math import ceil
from time import mktime

from Common import DB
from Sharding import Sharding


class Replset:
    def __init__(self, db, user, password, authdb, max_lag_secs):
        self.db           = db
        self.user         = user
        self.password     = password
        self.authdb       = authdb
        self.max_lag_secs = max_lag_secs

        self.rs_status = None
        self.primary   = None
        self.secondary = None

        # Get a DB connection
        try:
            if self.db.__class__.__name__ == "DB":
                self.connection = self.db.connection()
            else:
                raise Exception, "'db' field is an instance of %s, not 'DB'!" % self.db.__class__.__name__, None
        except Exception, e:
            logging.fatal("Could not get DB connection! Error: %s" % e)
            raise e

    def close(self):
        pass

    def get_rs_status(self, force=False):
        try:
            if force or not self.rs_status:
                self.rs_status = self.db.admin_command('replSetGetStatus')
            return self.rs_status
        except Exception, e:
            raise Exception, "Error getting replica set status! Error: %s" % e, None

    def get_rs_config(self):
        try:
            if self.db.server_version() >= tuple("3.0.0".split(".")):
                output = self.db.admin_command('replSetGetConfig')
                return output['config']
            else:
                return self.connection['local'].system.replset.find_one()
        except Exception, e:
            raise Exception, "Error getting replica set config! Error: %s" % e, None

    def find_primary(self):
        rs_status = self.get_rs_status()
        rs_name   = rs_status['set']
        for member in rs_status['members']:
            if member['stateStr'] == 'PRIMARY' and member['health'] > 0:
                self.primary = {
                    'host': member['name'],
                    'optime': member['optimeDate']
                }
                optime = member['optime']
                if isinstance(member['optime'], dict) and 'ts' in member['optime']:
                    optime = member['optime']['ts']
                logging.info("Found PRIMARY: %s/%s with optime %s" % (
                    rs_name,
                    member['name'],
                    str(optime)
                ))
        if self.primary is None:
            logging.fatal("Unable to locate a PRIMARY member for replset %s, giving up" % rs_name)
            raise Exception, "Unable to locate a PRIMARY member for replset %s, giving up" % rs_name, None
        return self.primary

    def find_secondary(self):
        rs_status    = self.get_rs_status()
        rs_config    = self.get_rs_config()
        rs_name      = rs_status['set']
        quorum_count = ceil(len(rs_status['members']) / 2.0)

        for member in rs_status['members']:
            if member['stateStr'] == 'SECONDARY' and member['health'] > 0:
                score       = self.max_lag_secs * 10
                score_scale = 100 / score
                log_data    = {}

                hidden_weight = 0.20
                for member_config in rs_config['members']:
                    if member_config['host'] == member['name']:
                        if 'hidden' in member_config and member_config['hidden'] == True:
                            score += (score * hidden_weight)
                            log_data['hidden'] = True
                        if 'priority' in member_config:
                            log_data['priority'] = int(member_config['priority'])
                            if member_config['priority'] > 0:
                                score = score - member_config['priority']
                        break

                rep_lag = (mktime(self.primary_optime().timetuple()) - mktime(member['optimeDate'].timetuple()))
                score = ceil((score - rep_lag) * score_scale)
                if rep_lag < self.max_lag_secs:
                    if self.secondary is None or score > self.secondary['score']:
                        self.secondary = {
                            'replSet': rs_name,
                            'count': 1 if self.secondary is None else self.secondary['count'] + 1,
                            'host': member['name'],
                            'optime': member['optimeDate'],
                            'score': score
                        }
                    log_msg = "Found SECONDARY %s/%s" % (rs_name, member['name'])
                else:
                    log_msg = "Found SECONDARY %s/%s with too-high replication lag! Skipping" % (rs_name, member['name'])

                log_data['optime'] = member['optime']
                if isinstance(member['optime'], dict) and 'ts' in member['optime']:
                    log_data['optime'] = member['optime']['ts']
                log_data['score']  = int(score)
                logging.info("%s: %s" % (log_msg, str(log_data)))
        if self.secondary is None or (self.secondary['count'] + 1) < quorum_count:
            logging.fatal("Not enough secondaries in replset %s to take backup! Num replset members: %i, required quorum: %i" % (
                rs_name,
                self.secondary['count'] + 1,
                quorum_count
            ))
            raise Exception, "Not enough secondaries in replset %s to safely take backup!" % rs_name, None

        logging.info("Choosing SECONDARY %s for replica set %s (score: %i)" % (self.secondary['host'], rs_name, self.secondary['score']))
        return self.secondary

    def primary_optime(self):
        rs_status  = self.get_rs_status()
        rs_primary = self.find_primary()
        if 'optime' in rs_primary:
            return rs_primary['optime']


class ReplsetSharded:
    def __init__(self, sharding, db, user, password, authdb, max_lag_secs):
        self.sharding     = sharding
        self.db           = db
        self.user         = user
        self.password     = password
        self.authdb       = authdb
        self.max_lag_secs = max_lag_secs

        self.replset = None

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

    def find_secondaries(self):
        shard_secondaries = {}
        if self.sharding:
            for shard in self.sharding.shards():
                shard_name, members = shard['host'].split('/')
                host, port          = members.split(',')[0].split(":")

                replset_db   = DB(host, port, self.user, self.password, self.authdb) 
                self.replset = Replset(replset_db, self.user, self.password, self.authdb, self.max_lag_secs)
                secondary    = self.replset.find_secondary()
                shard_secondaries[shard_name] = secondary

                self.replset.close()
                replset_db.close()
        return shard_secondaries

    def close(self):
        if self.replset:
            self.replset.close()
