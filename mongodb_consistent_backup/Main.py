import sys
import logging

from datetime import datetime
from multiprocessing import current_process
from signal import signal, SIGINT, SIGTERM

from Archive import Archive
from Backup import Backup
from Common import Config, DB, Lock, Timer, validate_hostname
from Errors import OperationError
from Notify import Notify
from Oplog import Tailer, Resolver
from Replication import Replset, ReplsetSharded
from Sharding import Sharding
from State import State
from Upload import Upload


class MongodbConsistentBackup(object):
    def __init__(self, prog_name="mongodb-consistent-backup"):
        self.program_name             = prog_name
        self.backup                   = None
        self.archive                  = None
        self.sharding                 = None
        self.replset                  = None
        self.replset_sharded          = None
        self.notify                   = None
        self.oplogtailer              = None
        self.oplog_resolver           = None
        self.upload                   = None
        self.lock                     = None
        self.backup_time              = None
        self.backup_root_directory    = None
        self.backup_root_subdirectory = None
        self.db                       = None
        self.is_sharded               = False
        self.log_level                = None
        self.timer                    = Timer()
        self.replsets                 = {}
        self.oplog_summary            = {}
        self.backup_summary           = {}

        self.setup_config()
        self.setup_logger()
        self.setup_signal_handlers()
        self.set_backup_dirs()
        #self.setup_state()
        self.get_db_conn()

    def setup_config(self):
        try:
            self.config = Config()
        except Exception, e:
            print "Error setting up configuration: '%s'!" % e
            sys.exit(1)

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
            logging.fatal("Cannot setup signal handlers, error: %s" % e)
            sys.exit(1)

    def set_backup_dirs(self):
        self.backup_time = datetime.now().strftime("%Y%m%d_%H%M")
        self.backup_root_subdirectory = "%s/%s" % (self.config.backup.name, self.backup_time)
        self.backup_root_directory = "%s/%s" % (self.config.backup.location, self.backup_root_subdirectory)

    def setup_state(self):
        self.state = State(self.config.backup.location, self.backup_time)
        self.state.add_config(self.config)

    def get_db_conn(self):
        try:
            validate_hostname(self.config.host)
            self.db = DB(self.config.host, self.config.port, self.config.user, self.config.password, self.config.authdb)
            self.is_sharded = self.db.is_mongos()
            if not self.is_sharded:
                self.is_sharded = self.db.is_configsvr()
            if not self.is_sharded and not self.db.is_replset():
                raise OperationError("Host %s:%i is not part of a replset and is not a sharding config/mongos server!")
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
            logging.info("Starting cleanup procedure! Stopping running threads")

            # TODO Move submodules into self that populates as used?
            submodules = ['replset', 'sharding', 'backup', 'oplogtailer', 'archive', 'upload']
            for submodule_name in submodules:
                submodule = getattr(self, submodule_name)
                if submodule:
                    submodule.close()

            if self.notify:
                self.notify.notify("%s: backup '%s' failed!" % (
                    self.config,
                    self.program_name
                ), False)

            if self.db:
                self.db.close()

            self.release_lock()

            logging.info("Cleanup complete. Exiting")

            sys.exit(1)

    def exception(self, error_message, error):
        if isinstance(error, OperationError):
            logging.fatal(error_message)
        else:
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
        logging.info("Starting %s version %s (git commit: %s)" % (self.program_name, self.config.version, self.config.git_commit))
        logging.info("Loaded config: %s" % self.config.json())

        self.get_lock()
        self.timer.start()

        # Setup the archiver
        try:
            self.archive = Archive(
                self.config,
                self.backup_root_directory, 
            )
        except Exception, e:
            self.exception("Problem starting archiver! Error: %s" % e, e)

        # Setup the notifier
        try:
            self.notify = Notify(self.config)
        except Exception, e:
            self.exception("Problem starting notifier! Error: %s" % e, e)

        # Setup the uploader
        try:
            self.upload = Upload(
                self.config,
                self.backup_root_directory,
                self.backup_root_subdirectory
            )
        except Exception, e:
            self.exception("Problem starting uploader! Error: %s" % e, e)

        if not self.is_sharded:
            logging.info("Running backup in replset mode using seed node: %s:%i" % (self.config.host, self.config.port))

            # get shard secondary
            try:
                self.replset = Replset(
                    self.config,
                    self.db
                )
                replset_name = self.replset.get_rs_name()
                self.replsets[replset_name] = self.replset
            except Exception, e:
                self.exception("Problem getting shard secondaries! Error: %s" % e, e)

            # run backup
            try:
                self.backup = Backup(
                    self.config,
                    self.backup_root_directory,
                    self.replsets
                )
                if self.backup.is_compressed():
                    logging.info("Backup method supports gzip compression, disabling compression in archive step")
                    self.archive.compression('none')
                self.backup.backup()
            except Exception, e:
                self.exception("Problem performing replset mongodump! Error: %s" % e, e)

            # use 1 archive thread for single replset
            self.archive.threads(1)
        else:
            logging.info("Running backup in sharding mode using seed node: %s:%i" % (self.config.host, self.config.port))

            # connect to balancer and stop it
            try:
                self.sharding = Sharding(
                    self.config,
                    self.db
                )
                self.sharding.get_start_state()
            except Exception, e:
                self.exception("Problem connecting to the balancer! Error: %s" % e, e)

            # get shard replsets
            try:
                self.replset_sharded = ReplsetSharded(
                    self.config,
                    self.sharding,
                    self.db
                )
                self.replsets = self.replset_sharded.get_replsets()
            except Exception, e:
                self.exception("Problem getting shard/replica set info! Error: %s" % e, e)

            # stop the balancer
            try:
                self.sharding.stop_balancer()
            except Exception, e:
                self.exception("Problem stopping the balancer! Error: %s" % e, e)

            # init the oplogtailers
            try:
                self.oplogtailer = Tailer(
                    self.config,
                    self.replsets,
                    self.backup_root_directory
                )
            except Exception, e:
                self.exception("Problem initializing oplog tailer! Error: %s" % e, e)

            # init the backup
            try:
                self.backup = Backup(
                    self.config,
                    self.backup_root_directory,
                    self.replsets, 
                    self.sharding
                )
                if self.backup.is_compressed():
                    logging.info("Backup method supports gzip compression, disabling compression in archive step and enabling oplog compression")
                    self.archive.compression('none')
                    self.oplogtailer.compression('gzip')
            except Exception, e:
                self.exception("Problem initializing backup! Error: %s" % e, e)

            # start the oplog tailers, before the backups start
            try:
                self.oplogtailer.run()
            except Exception, e:
                self.exception("Failed to start oplog tailing threads! Error: %s" % e, e)

            # run the backup(s)
            try:
                self.backup_summary = self.backup.backup()
            except Exception, e:
                self.exception("Problem performing backup! Error: %s" % e, e)

            # stop the oplog tailer(s)
            if self.oplogtailer:
                self.oplog_summary = self.oplogtailer.stop()

            # set balancer back to original value
            try:
                self.sharding.restore_balancer_state()
            except Exception, e:
                self.exception("Problem restoring balancer lock! Error: %s" % e, e)

            # resolve/merge tailed oplog into mongodump oplog.bson to a consistent point for all shards
            if self.backup.method == "mongodump" and self.oplogtailer:
                self.oplog_resolver = Resolver(self.config, self.oplog_summary, self.backup_summary)
                self.oplog_resolver.compression(self.oplogtailer.compression())
                self.oplog_resolver.run()

        # archive backup directories
        try:
            self.archive.archive()
        except Exception, e:
            self.exception("Problem performing archiving! Error: %s" % e, e)

        # upload backup
        try:
            self.upload.upload()
        except Exception, e:
            self.exception("Problem performing upload of backup! Error: %s" % e, e)

        self.timer.stop()

        # send notifications of backup state
        try:
            self.notify.notify("%s: backup '%s' succeeded in %s secs" % (
                self.program_name,
                self.config.backup.name,
                self.timer.duration()
            ), True)
        except Exception, e:
            self.exception("Problem running Notifier! Error: %s" % e, e)

        if self.db:
            self.db.close()

        self.release_lock()

        logging.info("Completed %s in %.2f sec" % (self.program_name, self.timer.duration()))
