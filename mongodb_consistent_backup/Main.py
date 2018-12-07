import logging
import os
import signal
import sys

from multiprocessing import current_process, Event, Manager

from Archive import Archive
from Backup import Backup
from Common import Config, DB, Lock, MongoUri, Timer
from Errors import NotifyError, OperationError
from Logger import Logger
from Notify import Notify
from Oplog import Tailer, Resolver
from Replication import Replset, ReplsetSharded
from Rotate import Rotate
from Sharding import Sharding
from State import StateRoot, StateBackup, StateBackupReplset, StateOplog
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
        self.resolver                 = None
        self.upload                   = None
        self.lock                     = None
        self.backup_time              = None
        self.backup_directory         = None
        self.backup_root_subdirectory = None
        self.backup_stop              = Event()
        self.uri                      = None
        self.db                       = None
        self.is_sharded               = False
        self.log_level                = None
        self.replsets                 = {}
        self.oplog_summary            = {}
        self.backup_summary           = {}
        self.manager                  = Manager()
        self.timer                    = Timer(self.manager)
        self.timer_name               = "%s.%s" % (self.program_name, self.__class__.__name__)
        self.logger                   = None
        self.current_log_file         = None
        self.backup_log_file          = None
        self.last_error_msg           = ''

        try:
            self.setup_config()
            self.setup_logger()
            self.setup_signal_handlers()
            self.get_lock()
            self.logger.update_symlink()
            self.init()
            self.setup_notifier()
            self.set_backup_dirs()
            self.get_db_conn()
            self.setup_state()
        except OperationError, e:
            self.exception("Error setting up %s: %s" % (self.program_name, e), e)

    def setup_config(self):
        try:
            self.config = Config()
            self.backup_time = self.config.backup_time
        except Exception, e:
            print "Error setting up configuration: '%s'!" % e
            sys.exit(1)

    def setup_logger(self):
        try:
            self.logger = Logger(self.config, self.backup_time)
            self.logger.start()
            self.logger.start_file_logger()
        except Exception, e:
            self.exception("Could not start logger: %s" % e, e)

    def setup_signal_handlers(self):
        try:
            signal.signal(signal.SIGINT, self.cleanup_and_exit)
            signal.signal(signal.SIGTERM, self.cleanup_and_exit)
        except Exception, e:
            logging.fatal("Cannot setup signal handlers, error: %s" % e)
            sys.exit(1)

    def set_backup_dirs(self):
        self.backup_root_directory    = os.path.join(self.config.backup.location, self.config.backup.name)
        self.backup_root_subdirectory = os.path.join(self.config.backup.name, self.backup_time)
        self.backup_directory         = os.path.join(self.config.backup.location, self.backup_root_subdirectory)

    def setup_state(self):
        self.state_root = StateRoot(self.backup_root_directory, self.config)
        self.state      = StateBackup(self.backup_directory, self.config, self.backup_time, self.uri, sys.argv)
        self.state_root.write(True)
        self.state.write()

    def setup_notifier(self):
        try:
            self.notify = Notify(
                self.manager,
                self.config,
                self.timer,
                self.backup_root_subdirectory,
                self.backup_directory
            )
        except Exception, e:
            self.exception("Problem starting notifier! Error: %s" % e, e)

    def get_db_conn(self):
        self.uri = MongoUri(self.config.host, self.config.port)
        try:
            self.db = DB(self.uri, self.config, True, 'secondaryPreferred')
        except OperationError, e:
            return self.exception("Cannot connect to seed host(s): %s" % self.uri, e)
        self.is_sharded = self.db.is_mongos()
        if not self.is_sharded:
            self.is_sharded = self.db.is_configsvr()
        if not self.is_sharded and not self.db.is_replset() and not self.db.is_configsvr():
            raise OperationError("Host %s is not part of a replset and is not a sharding config/mongos server!" % self.uri.get())

    def get_lock(self):
        # noinspection PyBroadException
        try:
            if not self.config.lock_file:
                self.config.lock_file = '/tmp/%s.lock' % self.program_name
            self.lock = Lock(self.config.lock_file)
        except Exception:
            logging.fatal("Could not acquire lock: '%s'! Is another %s process running? Exiting" % (self.config.lock_file, self.program_name))
            self.logger.compress(True)
            sys.exit(1)

    def release_lock(self):
        if self.lock:
            self.lock.release()

    def init(self):
        logging.info("Starting %s version %s (git commit: %s)" % (self.program_name, self.config.version, self.config.git_commit))
        logging.info("Loaded config: %s" % self.config)

    def start_timer(self):
        self.timer.start(self.timer_name)

    def stop_timer(self):
        self.timer.stop(self.timer_name)
        self.state.set('timers', self.timer.dump())

    def rotate_backups(self):
        rotater = Rotate(self.config, self.state_root, self.state)
        rotater.run()

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
            except Exception:
                continue

        if self.manager:
            self.manager.shutdown()
        if self.db:
            self.db.close()

        if self.notify:
            try:
                self.notify.notify("%s: backup '%s/%s' failed! Error: '%s'" % (
                    self.program_name,
                    self.config.backup.name,
                    self.backup_time,
                    self.last_error_msg
                ))
                self.notify.run()
                self.notify.close()
            except Exception, e:
                logging.error("Error from notifier: %s" % e)

        logging.info("Cleanup complete, exiting")
        if self.logger:
            self.logger.rotate()
            self.logger.close()

        self.release_lock()
        sys.exit(1)

    def exception(self, error_message, error):
        self.last_error_msg = error_message
        if isinstance(error, NotifyError):
            logging.error(error_message)
        else:
            if isinstance(error, OperationError):
                logging.fatal(error_message)
            else:
                logging.exception(error_message)
            return self.cleanup_and_exit(None, None)

    def run(self):
        # TODO would be nice to have this code look like: (functions do the work) and its readable
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
        self.start_timer()

        # Setup the archiver
        try:
            self.archive = Archive(
                self.manager,
                self.config,
                self.timer,
                self.backup_root_subdirectory,
                self.backup_directory
            )
        except Exception, e:
            self.exception("Problem starting archiver! Error: %s" % e, e)

        # Setup the uploader
        try:
            self.upload = Upload(
                self.manager,
                self.config,
                self.timer,
                self.backup_root_subdirectory,
                self.backup_directory
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
                replset_dir  = os.path.join(self.backup_directory, replset_name)
                self.replsets[replset_name] = self.replset
                state = StateBackupReplset(replset_dir, self.config, self.backup_time, replset_name)
                state.load_state(self.replset.summary())
                state.write()
            except Exception, e:
                self.exception("Problem getting shard secondaries! Error: %s" % e, e)

            # run backup
            try:
                self.backup = Backup(
                    self.manager,
                    self.config,
                    self.timer,
                    self.backup_root_subdirectory,
                    self.backup_directory,
                    self.replsets
                )
                if self.backup.is_compressed():
                    logging.info("Backup method supports compression, disabling compression in archive step")
                    self.archive.compression('none')
                self.backup_summary = self.backup.run()
                self.state.set('backup_oplog', self.backup_summary)
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
                    self.timer,
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
                    self.timer,
                    self.backup_root_subdirectory,
                    self.backup_directory,
                    self.replsets,
                    self.backup_stop
                )
            except Exception, e:
                self.exception("Problem initializing oplog tailer! Error: %s" % e, e)

            # init the backup
            try:
                self.backup = Backup(
                    self.manager,
                    self.config,
                    self.timer,
                    self.backup_root_subdirectory,
                    self.backup_directory,
                    self.replsets,
                    self.backup_stop,
                    self.sharding
                )
                if self.backup.is_compressed():
                    logging.info("Backup method supports compression, disabling compression in archive step and enabling oplog compression")
                    self.archive.compression('none')
                    self.oplogtailer.compression(self.backup.compression())
            except Exception, e:
                self.exception("Problem initializing backup! Error: %s" % e, e)

            # start the oplog tailers, before the backups start
            try:
                self.oplogtailer.run()
            except Exception, e:
                self.exception("Failed to start oplog tailing threads! Error: %s" % e, e)

            # run the backup(s)
            try:
                self.backup_summary = self.backup.run()
                self.state.set('backup_oplog', self.backup_summary)
            except Exception, e:
                self.exception("Problem performing backup! Error: %s" % e, e)

            # stop the oplog tailer(s)
            if self.oplogtailer:
                self.oplog_summary = self.oplogtailer.stop()
                self.state.set('tailer_oplog', self.oplog_summary)
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
                    shard_dir = os.path.join(self.backup_directory, shard)
                    state = StateBackupReplset(shard_dir, self.config, self.backup_time, shard)
                    state.load_state(rs_sharded_summary[shard])
                    state.write()
                self.replset_sharded.close()
            except Exception, e:
                self.exception("Problem closing replsets! Error: %s" % e, e)

            # close master db connection:
            if self.db:
                self.db.close()

            # resolve/merge tailed oplog into mongodump oplog.bson to a consistent point for all shards
            if self.backup.task.lower() == "mongodump" and self.oplogtailer.enabled():
                self.resolver = Resolver(
                    self.manager,
                    self.config,
                    self.timer,
                    self.backup_root_subdirectory,
                    self.backup_directory,
                    self.oplog_summary,
                    self.backup_summary
                )
                self.resolver.compression(self.oplogtailer.compression())
                resolver_summary = self.resolver.run()
                for shard in resolver_summary:
                    shard_dir = os.path.join(self.backup_directory, shard)
                    state = StateOplog(shard_dir, self.config, self.backup_time, shard)
                    state.load_state(resolver_summary[shard])
                    state.write()
                self.resolver.close()

        # archive backup directories
        try:
            self.archive.run()
            self.archive.close()
        except Exception, e:
            self.archive.close()
            self.exception("Problem performing archiving! Error: %s" % e, e)

        # upload backup
        try:
            self.upload.run()
            self.upload.close()
        except Exception, e:
            self.upload.close()
            self.exception("Problem performing upload of backup! Error: %s" % e, e)

        # stop timer
        self.stop_timer()
        self.state.set("completed", True)

        # send notifications of backup state
        try:
            self.notify.notify("%s: '%s/%s' succeeded in %.2f secs" % (
                self.program_name,
                self.config.backup.name,
                self.backup_time,
                self.timer.duration(self.timer_name)
            ), True)
            self.notify.run()
            self.notify.close()
        except Exception, e:
            self.notify.close()
            self.exception("Problem running Notifier! Error: %s" % e, e)

        self.rotate_backups()

        self.logger.rotate()
        logging.info("Completed %s in %.2f sec" % (self.program_name, self.timer.duration(self.timer_name)))
        self.release_lock()
