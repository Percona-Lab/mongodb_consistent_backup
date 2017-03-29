import logging
import os
import signal
import sys

from datetime import datetime
from multiprocessing import current_process, Manager

from Archive import Archive
from Backup import Backup
from Common import Config, DB, Lock, MongoUri, Timer
from Errors import Error, OperationError
from Notify import Notify
from Oplog import Tailer, Resolver
from Replication import Replset, ReplsetSharded
from Sharding import Sharding
from State import StateRoot, StateBackup, StateBackupReplset, StateDoneStamp
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
        self.backup_directory         = None
        self.backup_root_subdirectory = None
        self.uri                      = None
        self.db                       = None
        self.is_sharded               = False
        self.log_level                = None
        self.timer                    = Timer()
        self.replsets                 = {}
        self.oplog_summary            = {}
        self.backup_summary           = {}
        self.manager                  = Manager()

        self.setup_config()
        self.setup_logger()
        self.setup_signal_handlers()
        self.get_lock()
        self.init()
        self.set_backup_dirs()
        self.get_db_conn()
        self.setup_state()

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
            signal.signal(signal.SIGINT, self.cleanup_and_exit)
            signal.signal(signal.SIGTERM, self.cleanup_and_exit)
        except Exception, e:
            logging.fatal("Cannot setup signal handlers, error: %s" % e)
            sys.exit(1)

    def set_backup_dirs(self):
        self.backup_time = datetime.now().strftime("%Y%m%d_%H%M")
        self.backup_root_directory = os.path.join(self.config.backup.location, self.config.backup.name)
        self.backup_root_subdirectory = os.path.join(self.config.backup.name, self.backup_time)
        self.backup_directory = os.path.join(self.config.backup.location, self.backup_root_subdirectory)

    def setup_state(self):
        self.root_state   = StateRoot(self.backup_root_directory, self.config)
	self.backup_state = StateBackup(self.backup_directory, self.config, self.backup_time, self.uri, sys.argv)
        self.root_state.write()
	self.backup_state.write()

    def get_db_conn(self):
        self.uri = MongoUri(self.config.host, self.config.port)
        self.db  = DB(self.uri, self.config, True, 'secondaryPreferred')
        self.is_sharded = self.db.is_mongos()
        if not self.is_sharded:
            self.is_sharded = self.db.is_configsvr()
        if not self.is_sharded and not self.db.is_replset():
            raise OperationError("Host %s is not part of a replset and is not a sharding config/mongos server!" % self.uri.get())

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

    def init(self):
        logging.info("Starting %s version %s (git commit: %s)" % (self.program_name, self.config.version, self.config.git_commit))
        logging.info("Loaded config: %s" % self.config)

    # TODO Rename class to be more exact as this assumes something went wrong
    # noinspection PyUnusedLocal
    def cleanup_and_exit(self, code, frame):
        if not current_process().name == "MainProcess":
            return
        logging.info("Starting cleanup procedure! Stopping running threads")

        # TODO Move submodules into self that populates as used?
        submodules = ['replset', 'sharding', 'backup', 'oplogtailer', 'archive', 'upload']
        for submodule_name in submodules:
            try:
                submodule = getattr(self, submodule_name)
                if submodule:
                    submodule.close()
            except:
                continue

        if self.manager:
            self.manager.shutdown()

        if self.notify:
            try:
                self.notify.notify("%s: backup '%s' failed!" % (
                    self.config,
                    self.program_name
                ), False)
            except:
                pass

        if self.db:
            self.db.close()

        logging.info("Cleanup complete, exiting")
        self.release_lock()
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
        self.timer.start()

        # Setup the archiver
        try:
            self.archive = Archive(
                self.config,
                self.backup_directory, 
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
                self.backup_directory,
                self.backup_root_subdirectory
            )
        except Exception, e:
            self.exception("Problem starting uploader! Error: %s" % e, e)

        if not self.is_sharded:
            logging.info("Running backup in replset mode using seed node(s): %s" % self.uri)

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
                    self.manager,
                    self.config,
                    self.backup_directory,
                    self.replsets
                )
                if self.backup.is_compressed():
                    logging.info("Backup method supports gzip compression, disabling compression in archive step")
                    self.archive.compression('none')
                self.backup.backup()
            except Exception, e:
                self.exception("Problem performing replset mongodump! Error: %s" % e, e)

            # close master db connection:
            if self.db:
                self.db.close()

            # use 1 archive thread for single replset
            self.archive.threads(1)
        else:
            logging.info("Running backup in sharding mode using seed node(s): %s" % self.uri)

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
                    self.manager,
                    self.config,
                    self.replsets,
                    self.backup_directory
                )
            except Exception, e:
                self.exception("Problem initializing oplog tailer! Error: %s" % e, e)

            # init the backup
            try:
                self.backup = Backup(
                    self.manager,
                    self.config,
                    self.backup_directory,
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
		self.backup_state.backup_oplog_summary(self.backup_summary)
            except Exception, e:
                self.exception("Problem performing backup! Error: %s" % e, e)

            # stop the oplog tailer(s)
            if self.oplogtailer:
                self.oplog_summary = self.oplogtailer.stop()
		self.backup_state.tailer_summary(self.oplog_summary)
                self.oplogtailer.close()

            # set balancer back to original value
            try:
                self.sharding.restore_balancer_state()
                self.sharding.close()
            except Exception, e:
                self.exception("Problem restoring balancer lock! Error: %s" % e, e)

            # close replset_sharded:
            try:
		rs_sharded_summary = self.replset_sharded.summary()
		for shard in rs_sharded_summary:
                    state = StateBackupReplset(self.backup_directory, self.config, self.backup_time, shard)
                    state.load_state(rs_sharded_summary[shard]) 
		    state.write()
                self.replset_sharded.close()
            except Exception, e:
                self.exception("Problem closing replsets! Error: %s" % e, e)

            # close master db connection:
            if self.db:
                self.db.close()

            # resolve/merge tailed oplog into mongodump oplog.bson to a consistent point for all shards
            if self.backup.method == "mongodump" and self.oplogtailer:
                self.oplog_resolver = Resolver(self.config, self.manager, self.oplog_summary, self.backup_summary)
                self.oplog_resolver.compression(self.oplogtailer.compression())
                resolver_summary = self.oplog_resolver.run()
		self.backup_state.resolver_summary(resolver_summary)

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
	self.backup_state.timer_summary(self.timer.dump())

        # send notifications of backup state
        try:
            self.notify.notify("%s: backup '%s' succeeded in %s secs" % (
                self.program_name,
                self.config.backup.name,
                self.timer.duration()
            ), True)
        except Exception, e:
            self.exception("Problem running Notifier! Error: %s" % e, e)

	StateDoneStamp(self.backup_directory, self.config).write()
        logging.info("Completed %s in %.2f sec" % (self.program_name, self.timer.duration()))
        self.release_lock()
