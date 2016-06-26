import logging

from fabric.api import hide, settings, local
from multiprocessing import Process, Queue
from time import sleep


from DB import DB
from Mongodump import Mongodump
from ReplsetHandler import ReplsetHandler, ReplsetHandlerSharded


class Mongodumper:
    def __init__(self, host, port, user, password, authdb, base_dir, binary, dump_gzip, max_repl_lag_secs,
                 config_server, verbose=False):
        self.host              = host
        self.port              = port
        self.user              = user
        self.password          = password
        self.authdb            = authdb
        self.base_dir          = base_dir
        self.binary            = binary
        self.dump_gzip         = dump_gzip
        self.max_repl_lag_secs = max_repl_lag_secs
        self.config_server     = config_server
        self.verbose           = verbose

        self.response_queue = Queue()
        self.replset        = None
        self.threads        = []
        self._summary       = {}

        with hide('running', 'warnings'), settings(warn_only=True):
            self.version = local("%s --version|awk 'NR >1 {exit}; /version/{print $NF}'" % self.binary, capture=True)

        # Get a DB connection
        try:
            self.connection = DB(self.host, self.port, self.user, self.password, self.authdb).connection()
        except Exception, e:
            logging.fatal("Could not get DB connection! Error: %s" % e)
            raise e

    def summary(self):
        return self._summary

    def run(self):
        if not self.connection.is_mongos:
            # single node/replset backup mode:
            self.replset = ReplsetHandler(self.host, self.port, self.user, self.password, self.authdb,
                                     self.max_repl_lag_secs)
            secondary = self.replset.find_desirable_secondary()
            if 'host' in secondary and 'replSet' in secondary:
                logging.info("Found replset %s, using secondary instance %s for backup" % (secondary['replSet'], secondary['host']))
                thread = Process(target=Mongodump(
                    self.response_queue,
                    secondary['replSet'],
                    secondary['host'],
                    self.user,
                    self.password,
                    self.authdb,
                    self.base_dir,
                    self.binary,
                    self.dump_gzip,
                    self.verbose
                ).run)
                self.threads.append(thread)
            else:
                logging.error("Found no secondary for backup!")
            self.replset.close()
        else:
            # backup a secondary from each shard:
            self.replset = ReplsetHandlerSharded(self.host, self.port, self.user, self.password, self.authdb,
                                                    self.max_repl_lag_secs)
            secondaries = self.replset.find_desirable_secondaries()
            for shard in secondaries:
                secondary = secondaries[shard]
                thread = Mongodump(
                    self.response_queue,
                    secondary['replSet'],
                    secondary['host'],
                    self.user,
                    self.password,
                    self.authdb,
                    self.base_dir,
                    self.binary,
                    self.dump_gzip,
                    self.verbose
                )
                self.threads.append(thread)
            self.replset.close()

            # backup a single config server:
            if self.config_server:
                thread = Mongodump(
                    self.response_queue,
                    'config',
                    self.config_server,
                    self.user,
                    self.password,
                    self.authdb,
                    self.base_dir,
                    self.binary,
                    self.dump_gzip,
                    self.verbose
                )
                self.threads.append(thread)
            else:
                logging.warning("No config server found! This backup won't be consistent!")

        # start all threads
        logging.info(
            "Starting backups in threads using mongodump %s (inline gzip: %s)" % (self.version, str(self.dump_gzip)))
        for thread in self.threads:
            thread.start()

        # wait for all threads to finish
        for thread in self.threads:
            thread.join()

        # sleep for 3 sec to fix logging order
        sleep(3)

        # get oplog summaries from the queue
        completed = 0
        while not self.response_queue.empty():
            backup = self.response_queue.get()
            host = backup['host']
            port = backup['port']
            if host not in self._summary:
                self._summary[host] = {}
            self._summary[host][port] = backup
            if backup['completed']:
                completed += 1

        # fail if all threads did not complete
        if not completed == len(self.threads):
            raise Exception, "Not all mongodump threads completed successfully!", None

        logging.info("All mongodump backups completed")

        return self._summary

    def close(self):
        logging.info("Killing all mongodump threads...")
        if len(self.threads) > 0:
            for thread in self.threads:
                thread.terminate()
        logging.info("Killed all mongodump threads")

	if self.replset:
		self.replset.close()
