import os
import sys
import logging

from datetime import datetime
from fabric.api import local, hide, settings
from multiprocessing import current_process
from signal import signal, SIGINT, SIGTERM
from time import time

from Archive import ArchiverTar
from Common import DB, Lock, validate_hostname
from Backup import Dumper
from Notify import NotifyNSCA
from Oplog import OplogTailer, OplogResolver
from Replication import Replset, ReplsetSharded
from Sharding import Sharding
from Upload import UploadS3


class MongodbConsistentBackup(object):
    def __init__(self, config):
        # TODO-timv
        """
        We should move the layout to look like

            self.options : {
                "program_name" : None,
                ...
                "backup_options": {
                    "host": localhost,
                    "port": 27017,
                },
                "uploader": {},
                "notifier": {},

            }
        Also all options should have defaults  for example program_name should always be something
        """
        self.config = config
        self.program_name = "mongodb_consistent_backup"
        self.archiver = None
        self.sharding = None
        self.replset  = None
        self.replset_sharded = None
        self.notify = None
        self.mongodumper = None
        self.oplogtailer = None
        self.oplog_resolver = None
        self.backup_duration = None
        self.end_time = None
        self.uploader = None
        self._lock = None
        self.start_time = time()
        self.oplog_threads = []
        self.oplog_summary = {}
        self.secondaries   = {}
        self.mongodumper_summary = {}

        self.setup_signal_handlers()
        self.setup_logger()
        self.set_backup_dirs()

        # TODO Move any reference to the actual dumping into dumper classes
        # Check mongodump binary and set version + dump_gzip flag if 3.2+
        self.dump_gzip = False
        if os.path.isfile(self.config.backup.mongodump.binary) and os.access(self.config.backup.mongodump.binary, os.X_OK):
            with hide('running', 'warnings'), settings(warn_only=True):
                self.mongodump_version = tuple(
                    local("%s --version|awk 'NR >1 {exit}; /version/{print $NF}'" % self.config.backup.mongodump.binary,
                          capture=True).split("."))
                if tuple("3.2.0".split(".")) < self.mongodump_version:
                    self.dump_gzip = True
                    self.no_archiver_gzip = True
        else:
            logging.fatal("Cannot find or execute the mongodump binary file %s!" % self.config.backup.mongodump.binary)
            sys.exit(1)

        #TODO should this be in init or a sub-function?
        # Get a DB connection
        try:
            validate_hostname(self.config.host)
            self.db         = DB(self.config.host, self.config.port, self.config.user, self.config.password, self.config.authdb)
            self.connection = self.db.connection()
            self.is_sharded = self.connection.is_mongos
        except Exception, e:
            raise e

        # TODO Move to notifier module called NSCA
        # Setup the notifier:
        if self.config.notify.method == "none":
            logger.info("Notifying disabled! Skipping.")
        #elif self.config.notify.method == "nsca":
        #    if self.config.notify.nsca.server and self.config.notify.nsca.check_name:
        #        try:
        #            self.notify = NotifyNSCA(
        #                self.config.notify.nsca.server,
        #                self.config.notify.nsca.check_name,
        #                self.config.notify.nsca.check_host,
        #                self.config.notify.nsca.password
        #            )
        #        except Exception, e:
        #            raise e

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

    def get_lock(self):
        # noinspection PyBroadException
        try:
            if not self.config.lockfile:
                self.config.lockfile = '/tmp/%s.lock' % self.program_name
            self._lock = Lock(self.config.lockfile)
        except Exception:
            logging.fatal("Could not acquire lock: '%s'! Is another %s process running? Exiting" % (self.config.lockfile, self.program_name))
            self.cleanup_and_exit(None, None)

    def release_lock(self):
        if self._lock:
            self._lock.release()

    # TODO Rename class to be more exact as this assumes something went wrong
    # noinspection PyUnusedLocal
    def cleanup_and_exit(self, code, frame):
        if current_process().name == "MainProcess":
            logging.info("Starting cleanup and exit procedure! Killing running threads")

            # TODO Rename the mongodumper module to just "backup" then have submodule in it for the backup type
            # TODO Move submodules into self that populates as used?
            submodules = ['replset', 'sharding', 'mongodumper', 'oplogtailer', 'archiver', 'uploader']
            for submodule_name in submodules:
                submodule = getattr(self, submodule_name)
                if submodule:
                    submodule.close()

            # TODO Pass to notifier  Notifier(level,mesg) and it will pick the medium
            if self.notify:
                self.notify.notify(self.notify.critical, "%s: backup '%s' failed!" % (
                    self.config,
                    self.program_name
                ))

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

            try:
                self.mongodumper = Dumper(
                    self.config,
                    self.secondaries,
                    self.backup_root_directory,
                    self.dump_gzip
                )
                self.mongodumper.run()
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

            # Stop the balancer:    
            try:
                self.sharding.stop_balancer()
            except Exception, e:
                self.exception("Problem stopping the balancer! Error: %s" % e)

            # start the oplog tailer threads
            #if self.no_oplog_tailer:
            #    logging.warning("Oplog tailing disabled! Skipping")
            #else:
            try:
                self.oplogtailer = OplogTailer(
                    self.config,
                    self.secondaries,
                    self.backup_root_directory,
                    self.dump_gzip
                )
                self.oplogtailer.run()
            except Exception, e:
                self.exception("Failed to start oplog tailing threads! Error: %s" % e)

            # start the mongodumper threads
            try:
                self.mongodumper = Dumper(
                    self.config,
                    self.secondaries, 
                    self.backup_root_directory,
                    self.dump_gzip,
                    self.sharding.get_config_server()
                )
                self.mongodumper_summary = self.mongodumper.run()
            except Exception, e:
                self.exception("Problem performing mongodumps! Error: %s" % e)

            # stop the oplog tailing threads:
            if self.oplogtailer:
                self.oplog_summary = self.oplogtailer.stop()

            # set balancer back to original value
            try:
                self.sharding.restore_balancer_state()
            except Exception, e:
                self.exception("Problem restoring balancer lock! Error: %s" % e)

            # resolve/merge tailed oplog into mongodump oplog.bson to a consistent point for all shards
            if self.oplogtailer:
                self.oplog_resolver = OplogResolver(self.config, self.oplog_summary, self.mongodumper_summary, self.dump_gzip)
                self.oplog_resolver.run()

        # archive (and optionally compress) backup directories to archive files (threaded)
        if self.config.archive.method == "none":
            logging.warning("Archiving disabled! Skipping")
        elif self.config.archive.method == "tar":
            try:
                self.archiver = ArchiverTar(
                    self.config,
                    self.backup_root_directory, 
                )
                self.archiver.run()
            except Exception, e:
                self.exception("Problem performing archiving! Error: %s" % e)

        self.end_time = time()
        self.backup_duration = self.end_time - self.start_time

        # uploader
        if self.config.upload.method == "none":
            logging.info("Uploading disabled! Skipping")
        if self.config.upload.method == "s3" and self.config.upload.s3.bucket_name and self.config.upload.s3.bucket_prefix and self.config.upload.s3.access_key and self.config.upload.s3.secret_key:
            # AWS S3 secure multipart uploader
            try:
                self.uploader = UploadS3(
                    self.config,
                    self.backup_root_directory,
                    self.backup_root_subdirectory
                )
                self.uploader.run()
            except Exception, e:
                self.exception("Problem performing AWS S3 multipart upload! Error: %s" % e)

        # send notifications of backup state
        if self.config.notify.method == "none":
            logging.info("Notifying disabled! Skipping")
        #elif self.config.notify.method == "nsca":
        #    try:
        #        self.notify.notify(self.notify.success, "%s: backup '%s' succeeded in %s secs" % (
        #            self.program_name,
        #            self.config.backup.name,
        #            self.backup_duration
        #        ))
        #    except Exception, e:
        #        self.exception("Problem running NSCA notifier! Error: %s" % e)

        if self.db:
            self.db.close()

        self.release_lock()

        logging.info("Backup completed in %s sec" % self.backup_duration)