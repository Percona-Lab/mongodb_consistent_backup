import logging

from time import sleep

from Common import DB


class ShardingHandler:
    def __init__(self, host, port, user, password, authdb='admin', balancer_wait_secs=300, balancer_sleep=10):
        self.host               = host
        self.port               = port
        self.user               = user
        self.password           = password
        self.authdb             = authdb
        self.balancer_wait_secs = balancer_wait_secs
        self.balancer_sleep     = balancer_sleep

        self._balancer_state_start = None

        try:
            self.connection = DB(self.host, self.port, self.user, self.password, self.authdb).connection()
        except Exception, e:
            logging.fatal("Could not get DB connection! Error: %s" % e)
            raise e

    def close(self):
        self.restore_balancer_state()
        return self.connection.close()

    def get_start_state(self):
        self._balancer_state_start = self.get_balancer_state()
        logging.info("Began with balancer state: %s" % str(self._balancer_state_start))
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

    def get_configserver(self):
        cmdlineopts = self.connection['admin'].command("getCmdLineOpts")
        config_string = None
        if cmdlineopts.get('parsed').get('configdb'):
            config_string = cmdlineopts.get('parsed').get('configdb')
        elif cmdlineopts.get('parsed').get('sharding').get('configDB'):
            config_string = cmdlineopts.get('parsed').get('sharding').get('configDB')
        if config_string:
            # noinspection PyBroadException
            try:
                config_list = config_string.split(",")
            except Exception:
                config_list = [config_string]
            return config_list[0]
        else:
            logging.fatal("Unable to locate config servers for %s:%i!" % (self.host, self.port))
            raise Exception, "Unable to locate config servers for %s:%i!" % (self.host, self.port), None
