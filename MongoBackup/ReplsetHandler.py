import logging

from math import ceil
from time import mktime, sleep

from DB import DB
from ShardingHandler import ShardingHandler


class ReplsetHandler:
    def __init__(self, host, port, user, password, authdb, max_lag_secs, retries=5):
        self.host         = host
        self.port         = port
        self.user         = user
        self.password     = password
        self.authdb       = authdb
        self.max_lag_secs = max_lag_secs
        self.retries      = retries

        try:
            self.connection = DB(self.host, self.port, self.user, self.password, self.authdb).connection()
        except Exception, e:
            logging.fatal("Could not get DB connection! Error: %s" % e)
            raise e

    def close(self):
        return self.connection.close()

    def get_rs_status(self):
        try:
            tries  = 0
            status = None
            while not status and tries < self.retries:
                status = self.connection['admin'].command("replSetGetStatus")
                tries  = tries + 1
                sleep(1)
            if not status:
                raise Exception, "Could not get output from command: 'replSetGetStatus' after %i retries!" % self.retries, None
            return status
        except Exception, e:
            logging.fatal("Failed to execute command! Error: %s" % e)
            raise e

    def find_desirable_secondary(self):
        rs_status    = self.get_rs_status()
        rs_name      = rs_status['set']
        quorum_count = ceil(len(rs_status['members']) / 2.0)
        secondary    = None
        primary      = None
        for member in rs_status['members']:
            if 'health' in member and member['health'] > 0:
                logging.info("Found %s: %s/%s with optime %s" % (
                    member['stateStr'],
                    rs_name,
                    member['name'],
                    str(member['optime'])
                ))
    
                if member['stateStr'] == 'PRIMARY':
                    primary = {
                        'host': member['name'],
                        'optime': member['optimeDate']
                    }
                elif member['stateStr'] == 'SECONDARY':
                    if secondary is None or secondary['optime'] < member['optimeDate']:
                        secondary = {
                            'replSet': rs_status['set'],
                            'count': 1 if secondary is None else secondary['count'] + 1,
                            'host': member['name'],
                            'optime': member['optimeDate']
                        }

        if primary is None:
            logging.fatal("Unable to locate a PRIMARY member for replset %s, giving up" % rs_name)
            raise Exception, "Unable to locate a PRIMARY member for replset %s, giving up" % rs_name, None

        if secondary is None or (secondary['count'] + 1) < quorum_count:
            logging.fatal("Not enough secondaries in replset %s to safely take backup!" % rs_name)
            raise Exception, "Not enough secondaries in replset %s to safely take backup!" % rs_name, None

        rep_lag = (mktime(primary['optime'].timetuple()) - mktime(secondary['optime'].timetuple()))
        if rep_lag > self.max_lag_secs:
            logging.fatal("No secondary found in replset %s within %s lag time!" % (rs_name, self.max_lag_secs))
            raise Exception, "No secondary found in replset %s within %s lag time!" % (rs_name, self.max_lag_secs), None

        logging.info("Choosing SECONDARY %s for replica set %s" % (secondary['host'], rs_name))

        return secondary


class ReplsetHandlerSharded:
    def __init__(self, host, port, user, password, authdb, max_lag_secs):
        self.host         = host
        self.port         = port
        self.user         = user
        self.password     = password
        self.authdb       = authdb
        self.max_lag_secs = max_lag_secs

        self.replset = None

        try:
            self.sharding = ShardingHandler(self.host, self.port, self.user, self.password, self.authdb)
        except Exception, e:
            logging.fatal("Cannot get sharding connection! Error: %s" % e)
            raise e

    def find_desirable_secondaries(self):
        shard_secondaries = {}
        if self.sharding:
            for shard in self.sharding.shards():
                shard_name, members = shard['host'].split('/')
                host, port          = members.split(',')[0].split(":")

                self.replset = ReplsetHandler(host, port, self.user, self.password, self.authdb, self.max_lag_secs)
                secondary    = self.replset.find_desirable_secondary()
                shard_secondaries[shard_name] = secondary
                self.replset.close()
        return shard_secondaries

    def close(self):
        if self.replset:
            self.replset.close()
        self.sharding.close()
