import logging

from math import ceil

from mongodb_consistent_backup.Common import DB, validate_hostname
from bson.timestamp import Timestamp
from time import time


class Replset:
    def __init__(self, config, db):
        self.config       = config
        self.db           = db
        self.user         = self.config.user
        self.password     = self.config.password
        self.authdb       = self.config.authdb
        self.max_lag_secs = self.config.replication.max_lag_secs
        self.min_priority = self.config.replication.min_priority
        self.max_priority = self.config.replication.max_priority

        self.replset      = True
        self.rs_config    = None
        self.rs_status    = None
        self.primary      = None
        self.secondary    = None
        self.mongo_config = None

        # Get a DB connection
        try:
            if isinstance(self.db, DB):
                self.connection = self.db.connection()
            else:
                raise Exception, "'db' field is not an instance of class: 'DB'!", None
        except Exception, e:
            logging.fatal("Could not get DB connection! Error: %s" % e)
            raise e

    def close(self):
        pass

    def get_rs_status(self, force=False, quiet=False):
        try:
            if force or not self.rs_status:
                self.rs_status = self.db.admin_command('replSetGetStatus', quiet)
            return self.rs_status
        except Exception, e:
            raise Exception, "Error getting replica set status! Error: %s" % e, None

    def get_rs_config(self, force=False, quiet=False):
        if force or not self.rs_config:
            try:
                if self.db.server_version() >= tuple("3.0.0".split(".")):
                    output = self.db.admin_command('replSetGetConfig', quiet)
                    self.rs_config = output['config']
                else:
                    self.rs_config = self.connection['local'].system.replset.find_one()
            except Exception, e:
                raise Exception, "Error getting replica set config! Error: %s" % e, None
        return self.rs_config

    def get_rs_name(self):
        return self.get_rs_status()['set']

    def get_mongo_config(self, force=False, quiet=False):
        try:
            if force or not self.mongo_config:
                cmdline_opts = self.db.admin_command('getCmdLineOpts', quiet)
                if 'parsed' in cmdline_opts:
                    self.mongo_config = cmdline_opts['parsed']
            return self.mongo_config
        except Exception, e:
            raise Exception, "Error getting mongo config! Error: %s" % e, None

    def find_primary(self, force=False, quiet=False):
        rs_status = self.get_rs_status(force, quiet)
        rs_name   = rs_status['set']
        for member in rs_status['members']:
            if member['stateStr'] == 'PRIMARY' and member['health'] > 0:
                optime_ts = member['optime']
                if isinstance(member['optime'], dict) and 'ts' in member['optime']:
                    optime_ts = member['optime']['ts']
                validate_hostname(member['name'])
                self.primary = {
                    'host': member['name'],
                    'optime': optime_ts
                }
                logging.info("Found PRIMARY: %s/%s with optime %s" % (
                    rs_name,
                    member['name'],
                    str(optime_ts)
                ))
        if self.primary is None:
            logging.error("Unable to locate a PRIMARY member for replset %s, giving up" % rs_name)
            raise Exception, "Unable to locate a PRIMARY member for replset %s, giving up" % rs_name, None
        return self.primary

    def find_secondary(self, force=False, quiet=False):
        rs_status    = self.get_rs_status(force, quiet)
        rs_config    = self.get_rs_config(force, quiet)
        rs_name      = rs_status['set']
        quorum_count = ceil(len(rs_status['members']) / 2.0)

        if self.secondary and not force:
            return self.secondary

        for member in rs_status['members']:
            if member['stateStr'] == 'SECONDARY' and member['health'] > 0:
                score       = self.max_lag_secs * 10
                score_scale = 100 / score
                log_data    = {}

                priority = 0
                hidden_weight = 0.20
                for member_config in rs_config['members']:
                    if member_config['host'] == member['name']:
                        if 'hidden' in member_config and member_config['hidden']:
                            score += (score * hidden_weight)
                            log_data['hidden'] = True
                        if 'priority' in member_config:
                            priority = int(member_config['priority'])
                            log_data['priority'] = priority
                            if member_config['priority'] > 0:
                                score -= priority
                        break

                optime_ts = member['optime']
                if isinstance(member['optime'], dict) and 'ts' in member['optime']:
                    optime_ts = member['optime']['ts']

                if priority < self.min_priority or priority > self.max_priority:
                    # TODO-timv With out try blocks the log_msg is confused and may not be used, shouldn't we log immediately?
                    log_msg = "Found SECONDARY %s/%s with out-of-bounds priority! Skipping" % (rs_name, member['name'])
                rep_lag = (self.primary_optime().time - optime_ts.time)
                score = ceil((score - rep_lag) * score_scale)
                if rep_lag < self.max_lag_secs:
                    if self.secondary is None or score > self.secondary['score']:
                        self.secondary = {
                            'replSet': rs_name,
                            'count': 1 if self.secondary is None else self.secondary['count'] + 1,
                            'host': member['name'],
                            'optime': optime_ts,
                            'score': score
                        }
                        validate_hostname(self.secondary['host'])
                    log_msg = "Found SECONDARY %s/%s" % (rs_name, member['name'])
                else:
                    rep_lag = (self.primary_optime().time - optime_ts.time)
                    score = ceil((score - rep_lag) * score_scale)
                    if rep_lag < self.max_lag_secs:
                        if self.secondary is None or score > self.secondary['score']:
                            self.secondary = {
                                'replSet': rs_name,
                                'count': 1 if self.secondary is None else self.secondary['count'] + 1,
                                'host': member['name'],
                                'optime': optime_ts,
                                'score': score
                            }
                        log_msg = "Found SECONDARY %s/%s" % (rs_name, member['name'])
                    else:
                        log_msg = "Found SECONDARY %s/%s with too-high replication lag! Skipping" % (rs_name, member['name'])

                if 'configsvr' in rs_status and rs_status['configsvr']:
                    log_data['configsvr'] = True

                log_data['lag']    = rep_lag
                log_data['optime'] = optime_ts
                log_data['score']  = int(score)
                logging.info("%s: %s" % (log_msg, str(log_data)))
        if self.secondary is None or (self.secondary['count'] + 1) < quorum_count:
            secondary_count = self.secondary['count'] + 1 if self.secondary else 0
            logging.error("Not enough secondaries in replset %s to take backup! Num replset members: %i, required quorum: %i" % (
                rs_name,
                secondary_count,
                quorum_count
            ))
            raise Exception, "Not enough secondaries in replset %s to safely take backup!" % rs_name, None

        logging.info("Choosing SECONDARY %s for replica set %s (score: %i)" % (self.secondary['host'], rs_name, self.secondary['score']))
        return self.secondary

    def primary_optime(self):
        rs_primary = self.find_primary(True, True)
        if 'optime' in rs_primary:
            return rs_primary['optime']
