import logging

from time import sleep

from Common import DB
from Replset import Replset


class Sharding:
    def __init__(self, db, user=None, password=None, authdb='admin', balancer_wait_secs=300, balancer_sleep=5):
        self.db                 = db
        self.user               = user
        self.password           = password
        self.authdb             = authdb
        self.balancer_wait_secs = balancer_wait_secs
        self.balancer_sleep     = balancer_sleep

        self.config_server         = None
        self.config_db             = None
        self._balancer_state_start = None

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

    def close(self):
        if self.config_db:
            self.config_db.close()
        return self.restore_balancer_state()

    def get_start_state(self):
        self._balancer_state_start = self.get_balancer_state()
        logging.info("Began with balancer state running: %s" % str(self._balancer_state_start))
        return self._balancer_state_start

    def shards(self):
        return self.connection['config'].shards.find()

    def check_balancer_running(self):
        config = self.connection['config']
        lock   = config['locks'].find_one({'_id': 'balancer'})
        if 'state' in lock and int(lock['state']) == 0:
            return False
        return True

    def get_balancer_state(self):
        config = self.connection['config']
        state  = config['settings'].find_one({'_id': 'balancer'})

        if not state:
            return True
        elif 'stopped' in state and state.get('stopped') is True:
            return False
        else:
            return True

    def set_balancer(self, value):
        try:
            if value is True:
                set_value = False
            elif value is False:
                set_value = True
            else:
                set_value = True
            config = self.connection['config']
            config['settings'].update_one({'_id': 'balancer'}, {'$set': {'stopped': set_value}})
        except Exception, e:
            logging.fatal("Failed to set balancer state! Error: %s" % e)
            raise e

    def restore_balancer_state(self):
        if self._balancer_state_start is not None:
            try:
                logging.info("Restoring balancer state to: %s" % str(self._balancer_state_start))
                self.set_balancer(self._balancer_state_start)
            except Exception, e:
                logging.fatal("Failed to set balancer state! Error: %s" % e)
                raise e

    def stop_balancer(self):
        logging.info("Stopping the balancer and waiting a max of %i sec" % self.balancer_wait_secs)
        wait_cnt = 0
        self.set_balancer(False)
        while wait_cnt < self.balancer_wait_secs:
            if self.check_balancer_running():
                wait_cnt += self.balancer_sleep
                logging.info("Balancer is still running, waiting a max of %i sec" % self.balancer_sleep)
                sleep(self.balancer_sleep)
            else:
                sleep(self.balancer_sleep)
                logging.info("Balancer is now stopped")
                return
        logging.fatal("Could not stop balancer: %s:%i!" % (self.host, self.port))
        raise Exception, "Could not stop balancer: %s:%i" % (self.host, self.port), None

    def get_configdb_hosts(self):
        try:
            cmdlineopts = self.db.admin_command("getCmdLineOpts")
            config_string = None
            if cmdlineopts.get('parsed').get('configdb'):
                config_string = cmdlineopts.get('parsed').get('configdb')
            elif cmdlineopts.get('parsed').get('sharding').get('configDB'):
                config_string = cmdlineopts.get('parsed').get('sharding').get('configDB')
            if config_string:
                # noinspection PyBroadException
                try:
                    return config_string.split(',')
                except Exception:
                    return [config_string]
            else:
                logging.fatal("Unable to locate config servers for %s:%i!" % (self.host, self.port))
                raise Exception, "Unable to locate config servers for %s:%i!" % (self.host, self.port), None
        except Exception, e:
            raise e

    def get_config_server(self, force=False):
        if force or not self.config_server:
            configdb_hosts = self.get_configdb_hosts()
            try:
                config_host, config_port = configdb_hosts[0].split(":")
                logging.info("Found sharding config server: %s" % (config_host))

                self.config_db = DB(config_host, config_port, self.user, self.password, self.authdb)
                rs = Replset(self.config_db, self.user, self.password, self.authdb)
                try:
                    rs_status = rs.get_rs_status(False, True)
                    self.config_server = rs
                except Exception:
                    self.config_server = {'host': configdb_hosts[0]}
                finally:
                    return self.config_server
            except Exception, e:
                raise e
            else:
                logging.fatal("Unable to locate config servers for %s:%i!" % (self.host, self.port))
                raise Exception, "Unable to locate config servers for %s:%i!" % (self.host, self.port), None
        return self.config_server
