import logging

from bson.timestamp import Timestamp
from math import ceil
from time import mktime, time

from mongodb_consistent_backup.Common import DB, MongoUri
from mongodb_consistent_backup.Errors import OperationError

class Replset:
    def __init__(self, config, db):
        self.config       = config
        self.db           = db
        self.max_lag_secs = self.config.replication.max_lag_secs
        self.min_priority = self.config.replication.min_priority
        self.max_priority = self.config.replication.max_priority
        self.hidden_only  = self.config.replication.hidden_only

        self.hidden_weight = 0.20
        self.pri0_weight   = 0.10

        self.replset      = True
        self.rs_config    = None
        self.rs_status    = None
        self.primary      = None
        self.secondary    = None
        self.mongo_config = None

        self.replset_summary = {}

        # Get a DB connection
        try:
            if isinstance(self.db, DB):
                self.connection = self.db.connection()
            else:
                raise Error("'db' field is not an instance of class: 'DB'!")
        except Exception, e:
            logging.fatal("Could not get DB connection! Error: %s" % e)
            raise OperationError(e)

    def close(self):
        pass

    def summary(self):
        return self.replset_summary

    def get_rs_status(self, force=False, quiet=False):
        try:
            if force or not self.rs_status:
                self.rs_status = self.db.admin_command('replSetGetStatus', quiet)
                self.replset_summary['status'] = self.rs_status
            return self.rs_status
        except Exception, e:
            logging.fatal("Error getting replica set status! Error: %s" % e)
            raise OperationError(e)

    def get_rs_config(self, force=False, quiet=False):
        if force or not self.rs_config:
            try:
                if self.db.server_version() >= tuple("3.0.0".split(".")):
                    output = self.db.admin_command('replSetGetConfig', quiet)
                    self.rs_config = output['config']
                else:
                    self.rs_config = self.connection['local'].system.replset.find_one()
                self.replset_summary['config'] = self.rs_config
            except pymongo.errors.OperationFailure, e:
                raise OperationError("Error getting replica set config! Error: %s" % e)
        return self.rs_config

    def get_rs_config_member(self, member, force=False, quiet=False):
        rs_config = self.get_rs_config(force, quiet)
        if 'name' in member:
            for cnf_member in rs_config['members']:
                if member['name'] == cnf_member['host']:
                    return cnf_member
        raise OperationError("Member does not exist in mongo config!")

    def get_rs_name(self):
        return self.get_rs_status()['set']

    def get_mongo_config(self, force=False, quiet=False):
        try:
            if force or not self.mongo_config:
                cmdline_opts = self.db.admin_command('getCmdLineOpts', quiet)
                if 'parsed' in cmdline_opts:
                    self.mongo_config = cmdline_opts['parsed']
                    self.replset_summary['mongo_config'] = self.mongo_config
            return self.mongo_config
        except pymongo.errors.OperationFailure, e:
            raise OperationError("Error getting mongo config! Error: %s" % e)

    def get_mongo_config_member(self, member, force=False, quiet=False):
        rs_config = self.get_mongo_config(force, quiet)
        if 'members' in rs_config and 'name' in member:
            for cnf_member in rs_config:
                if member['name'] == cnf_member['name']:
                    return cnf_member
        raise OperationError("Member does not exist in mongo config!")

    def get_repl_op_lag(self, rs_status, rs_member):
        op_lag = 0
        if 'date' in rs_status and 'lastHeartbeat' in rs_member:
            op_lag = mktime(rs_status['date'].timetuple()) - mktime(rs_member['lastHeartbeat'].timetuple())
        return op_lag

    def get_repl_lag(self, rs_member):
        rs_status         = self.get_rs_status(False, True)
        rs_primary        = self.find_primary(False, True)
        member_optime_ts  = rs_member['optime']
        primary_optime_ts = self.primary_optime(False, True)
        if isinstance(rs_member['optime'], dict) and 'ts' in rs_member['optime']:
            member_optime_ts = rs_member['optime']['ts']
        op_lag  = self.get_repl_op_lag(rs_status, rs_member)
        rep_lag = (primary_optime_ts.time - member_optime_ts.time) - op_lag
        if rep_lag < 0:
            rep_lag = 0
        return rep_lag, member_optime_ts

    def find_primary(self, force=False, quiet=False):
        if force or not self.primary:
             rs_status = self.get_rs_status(force, quiet)
             rs_name   = rs_status['set']
             for member in rs_status['members']:
                 if member['stateStr'] == 'PRIMARY' and member['health'] > 0:
                     member_uri = MongoUri(member['name'], 27017, rs_name)
                     optime_ts  = member['optime']
                     if isinstance(member['optime'], dict) and 'ts' in member['optime']:
                         optime_ts = member['optime']['ts']
                     if quiet == False or not self.primary:
                         logging.info("Found PRIMARY: %s with optime %s" % (
                             member_uri,
                             str(optime_ts)
                         )) 
                     self.primary = {
                         'uri': member_uri,
                         'optime': optime_ts
                     }
                     self.replset_summary['primary'] = { "member": member, "uri": member_uri.str() }
             if self.primary is None:
                 logging.error("Unable to locate a PRIMARY member for replset %s, giving up" % rs_name)
                 raise OperationError("Unable to locate a PRIMARY member for replset %s, giving up" % rs_name)
        return self.primary

    def find_secondary(self, force=False, quiet=False):
        rs_status = self.get_rs_status(force, quiet)
        rs_config = self.get_rs_config(force, quiet)
        db_config = self.get_mongo_config(force, quiet)
        rs_name   = rs_status['set']
        quorum    = ceil(len(rs_status['members']) / 2.0)

        if self.secondary and not force:
            return self.secondary

        secondary_count = 0
        for member in rs_status['members']:
            member_uri = MongoUri(member['name'], 27017, rs_name)
            if member['state'] == 7:
                logging.info("Found ARBITER %s, skipping" % member_uri)
            elif member['state'] > 2:
                logging.warning("Found down or unhealthy SECONDARY %s with state: %s" % (member_uri, member['stateStr']))
            elif member['state'] == 2 and member['health'] > 0:
                log_data      = {}
                score         = self.max_lag_secs * 10
                score_scale   = 100 / score
                priority      = 0
                member_config = self.get_rs_config_member(member)
                if 'hidden' in member_config and member_config['hidden']:
                    score += (score * self.hidden_weight)
                    log_data['hidden'] = True
                if 'priority' in member_config:
                    priority = int(member_config['priority'])
                    log_data['priority'] = priority
                    if member_config['priority'] > 1:
                        score -= priority - 1
                    elif member_config['priority'] == 0:
                        score += (score * self.pri0_weight)            
                    if priority < self.min_priority or priority > self.max_priority:
                        logging.info("Found SECONDARY %s with out-of-bounds priority! Skipping" % member_uri)
                        continue
                elif self.hidden_only and not 'hidden' in log_data:
                    logging.info("Found SECONDARY %s that is non-hidden and hidden-only mode is enabled! Skipping" % member_uri)
                    continue

                rep_lag, optime_ts = self.get_repl_lag(member)
                score = ceil((score - rep_lag) * score_scale)
                if rep_lag < self.max_lag_secs:
                    if self.secondary is None or score > self.secondary['score']:
                        self.secondary = {
                            'replSet': rs_name,
                            'uri': member_uri,
                            'optime': optime_ts,
                            'score': score
                        }
                    log_msg = "Found SECONDARY %s" % member_uri
                    secondary_count += 1
                else:
                    log_msg = "Found SECONDARY %s with too high replication lag! Skipping" % member_uri

                if 'configsvr' in rs_status and rs_status['configsvr']:
                    log_data['configsvr'] = True

                log_data['lag']    = rep_lag
                log_data['optime'] = optime_ts
                log_data['score']  = int(score)
                logging.info("%s: %s" % (log_msg, str(log_data)))
                self.replset_summary['secondary'] = { "member": member, "uri": member_uri.str(), "data": log_data }
        if self.secondary is None or (secondary_count + 1) < quorum:
            logging.error("Not enough valid secondaries in replset %s to take backup! Num replset members: %i, required quorum: %i" % (
                rs_name,
                secondary_count,
                quorum
            ))
            raise OperationError("Not enough secondaries in replset %s to safely take backup!" % rs_name)

        logging.info("Choosing SECONDARY %s for replica set %s (score: %i)" % (self.secondary['uri'], rs_name, self.secondary['score']))
        return self.secondary

    def primary_optime(self, force=False, quiet=False):
        rs_primary = self.find_primary(force, quiet)
        if 'optime' in rs_primary:
            return rs_primary['optime']
