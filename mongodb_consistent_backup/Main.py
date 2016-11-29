import sys
import logging

from datetime import datetime
from multiprocessing import current_process
from signal import signal, SIGINT, SIGTERM
from time import time

from Archive import Archive
from Backup import Backup
from Common import DB, Lock, validate_hostname
from Notify import Notify
from Oplog import Tailer, Resolver
from Replication import Replset, ReplsetSharded
from Sharding import Sharding
from Upload import Upload


class MongodbConsistentBackup(object):
    def __init__(self, config, prog_name="mongodb-consistent-backup"):
        self.config          = config
        self.program_name    = prog_name
        self.backup          = None
        self.archive         = None
        self.sharding        = None
        self.replset         = None
        self.replset_sharded = None
        self.notify          = None
        self.oplogtailer     = None
        self.oplog_resolver  = None
        self.upload          = None
        self.lock            = None
        self.start_time      = time()
        self.end_time        = None
        self.backup_duration = None
        self.connection      = None
        self.db              = None
        self.is_sharded      = False
        self.secondaries     = {}
        self.oplog_summary   = {}
        self.backup_summary  = {}

        self.setup_signal_handlers()
        self.setup_logger()
        self.set_backup_dirs()
        self.get_db_conn()

    def setup_logger(self):
        self.log_level = logging.INFO
        if self.config.verbose:
            self.log_level = logging.DEBUG
        logging.basicConfig(level=self.log_level,
                            format='[%(asctime)s] [%(levelname)s] [%(processName)s] [%(module)s:%(funcName)s:%(lineno)d] %(message)s')

    def setup_signal_handlers(self):
        try:
            signal(SIGINT, self.cleanup_and_exit)
            signal(SIGTERM, self.cleanup_and_exit)
        except Exception, e:
            logger.fatal("Cannot setup signal handlers, error: %s" % e)
            sys.exit(1)

    def set_backup_dirs(self):
        self.backup_time = datetime.now().strftime("%Y%m%d_%H%M")
        self.backup_root_subdirectory = "%s/%s" % (self.config.backup.name, self.backup_time)
        self.backup_root_directory = "%s/%s" % (self.config.backup.location, self.backup_root_subdirectory)

    def get_db_conn(self):
        try:
            validate_hostname(self.config.host)
            self.db         = DB(self.config.host, self.config.port, self.config.user, self.config.password, self.config.authdb)
            self.connection = self.db.connection()
            self.is_sharded = self.connection.is_mongos
        except Exception, e:
            raise e

    def get_lock(self):
        # noinspection PyBroadException
        try:
            if not self.config.lockfile:
                self.config.lockfile = '/tmp/%s.lock' % self.program_name
            self.lock = Lock(self.config.lockfile)
        except Exception:
            logging.fatal("Could not acquire lock: '%s'! Is another %s process running? Exiting" % (self.config.lockfile, self.program_name))
            self.cleanup_and_exit(None, None)

    def release_lock(self):
        if self.lock:
            self.lock.release()

    # TODO Rename class to be more exact as this assumes something went wrong
    # noinspection PyUnusedLocal
    def cleanup_and_exit(self, code, frame):
        if current_process().name == "MainProcess":
            logging.info("Starting cleanup and exit procedure! Killing running threads")

            # TODO Move submodules into self that populates as used?
            submodules = ['replset', 'sharding', 'backup', 'oplogtailer', 'archive', 'upload']
            for submodule_name in submodules:
                submodule = getattr(self, submodule_name)
                if submodule:
                    submodule.close()

            self.notify.notify("%s: backup '%s' failed!" % (
                self.config,
                self.program_name
            ), False)

            if self.db:
                self.db.close()

            self.release_lock()

            logging.info("Cleanup complete. Exiting")

            sys.exit(1)

    def exception(self, error_message):
        logging.exception(error_message)
        return self.cleanup_and_exit(None, None)

    def run(self):
        # TODO would be nice to have this code  look like: (functions do the work) and its readable
        """
        self.log(version_message,INFO)
        self.lock()
        self.start_timer()
        if not self.is_sharded():
            self.exec_unsharded()
        else
            self.exec_sharded()
        self.stopTimer()
        self.archive()
        self.upload()
        self.notify()
        if self.db:
            self.db.close()
        self.log(backup_complete_message,INFO)
        """
        logging.info("Starting %s version %s (git commit hash: %s)" % (self.program_name, self.config.version, self.config.git_commit))

        self.get_lock()

        # Setup the notifier
        try:
            self.notify = Notify(self.config)
        except Exception, e:
            raise e

        if not self.is_sharded:
            logging.info("Running backup of %s:%s in replset mode" % (self.config.host, self.config.port))

            self.config.archive.threads = 1

            # get shard secondary
            try:
                self.replset = Replset(
                    self.config,
                    self.db
                )
                secondary    = self.replset.find_secondary()
                replset_name = secondary['replSet']

                self.secondaries[replset_name] = secondary
            except Exception, e:
                self.exception("Problem getting shard secondaries! Error: %s" % e)

            # run backup
            try:
                self.backup = Backup(
                    self.config,
                    self.backup_root_directory,
                    self.secondaries
                )
                self.backup.backup()
                if self.backup.is_gzip():
                    logging.info("Backup method supports gzip compression, setting config overrides: { archive.compression: 'none' }")
                    self.config.archive.compression = 'none'
                    self.config.oplog.compression = 'gzip'
            except Exception, e:
                self.exception("Problem performing replset mongodump! Error: %s" % e)

        else:
            logging.info("Running backup of %s:%s in sharded mode" % (self.config.host, self.config.port))

            # connect to balancer and stop it
            try:
                self.sharding = Sharding(
                    self.config,
                    self.db
                )
                self.sharding.get_start_state()
            except Exception, e:
                self.exception("Problem connecting to the balancer! Error: %s" % e)

            # get shard secondaries
            try:
                self.replset_sharded = ReplsetSharded(
                    self.config,
                    self.sharding,
                    self.db
                )
                self.secondaries = self.replset_sharded.find_secondaries()
            except Exception, e:
                self.exception("Problem getting shard secondaries! Error: %s" % e)

            # stop the balancer
            try:
                self.sharding.stop_balancer()
            except Exception, e:
                self.exception("Problem stopping the balancer! Error: %s" % e)

            # init the backup
            try:
                self.backup = Backup(
                    self.config,
                    self.backup_root_directory,
                    self.secondaries, 
                    self.sharding.get_config_server()
                )
                if self.backup.is_gzip():
                    logging.info("Backup method supports gzip compression, setting config overrides: { archive.compression: 'none', oplog.compression: 'gzip' }")
                    self.config.archive.compression = 'none'
                    self.config.oplog.compression = 'gzip'
            except Exception, e:
                self.exception("Problem initializing backup! Error: %s" % e)

            # start the oplog tailer(s)
            try:
                self.oplogtailer = Tailer(
                    self.config,
                    self.secondaries,
                    self.backup_root_directory
                )
                self.oplogtailer.run()
            except Exception, e:
                self.exception("Failed to start oplog tailing threads! Error: %s" % e)

            # run the backup(s)
            try:
                self.backup_summary = self.backup.backup()
            except Exception, e:
                self.exception("Problem performing backup! Error: %s" % e)

            # stop the oplog tailer(s)
            if self.oplogtailer:
                self.oplog_summary = self.oplogtailer.stop()

            # set balancer back to original value
            try:
                self.sharding.restore_balancer_state()
            except Exception, e:
                self.exception("Problem restoring balancer lock! Error: %s" % e)

            # resolve/merge tailed oplog into mongodump oplog.bson to a consistent point for all shards
            if self.config.backup.method == "mongodump" and self.oplogtailer:
                self.oplog_resolver = Resolver(self.config, self.oplog_summary, self.backup_summary)
                self.oplog_resolver.run()

        # archive backup directories
        try:
            self.archive = Archive(
                self.config,
                self.backup_root_directory, 
            )
            self.archive.archive()
        except Exception, e:
            self.exception("Problem performing archiving! Error: %s" % e)

        self.end_time = time()
        self.backup_duration = self.end_time - self.start_time

        # upload backup
        try:
            self.upload = Upload(
                self.config,
                self.backup_root_directory,
                self.backup_root_subdirectory
            )
            self.upload.upload()
        except Exception, e:
            self.exception("Problem performing upload of backup! Error: %s" % e)

        # send notifications of backup state
        try:
            self.notify.notify("%s: backup '%s' succeeded in %s secs" % (
                self.program_name,
                self.config.backup.name,
                self.backup_duration
            ), True)
        except Exception, e:
            self.exception("Problem running Notifier! Error: %s" % e)

        if self.db:
            self.db.close()

        self.release_lock()

        logging.info("Backup completed in %s sec" % self.backup_duration)
