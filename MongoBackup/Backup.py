import os
import sys
import logging

from datetime import datetime
from fabric.api import local, hide, settings
from multiprocessing import current_process
from signal import signal, SIGINT, SIGTERM
from time import time

from Archive import ArchiverTar
from Common import DB, Lock
from Methods import Dumper
from Notify import NotifyNSCA
from Oplog import OplogTailer, OplogResolver
from Replication import Replset, ReplsetSharded
from Sharding import Sharding
from Upload import UploadS3


class Backup(object):
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

        # Setup signal handler:
        signal(SIGINT, self.cleanup_and_exit)
        signal(SIGTERM, self.cleanup_and_exit)

        # TODO Move to function
        # Set default lock file:
        if not self.config.lockfile:
            self.config.lockfile = '/tmp/%s.lock' % self.program_name

        # TODO Move to function
        # Setup backup dir name:
        time_string = datetime.now().strftime("%Y%m%d_%H%M")
        self.backup_root_subdirectory = "%s/%s" % (self.config.name, time_string)
        self.backup_root_directory = "%s/%s" % (self.config.location, self.backup_root_subdirectory)

        # TODO Move below to actual functions called by a master run function
        # Setup logging
        self.log_level = logging.INFO
        if self.config.verbose:
            self.log_level = logging.DEBUG
        logging.basicConfig(level=self.log_level,
                            format='[%(asctime)s] [%(levelname)s] [%(processName)s] [%(module)s:%(funcName)s:%(lineno)d] %(message)s')

        # TODO Move any reference to the actual dumping into dumper classes
        # Check mongodump binary and set version + dump_gzip flag if 3.2+
        if os.path.isfile(self.config.method.mongodump.binary) and os.access(self.config.method.mongodump.binary, os.X_OK):
            with hide('running', 'warnings'), settings(warn_only=True):
                self.mongodump_version = tuple(
                    local("%s --version|awk 'NR >1 {exit}; /version/{print $NF}'" % self.config.method.mongodump.binary,
                          capture=True).split("."))
                if tuple("3.2.0".split(".")) < self.mongodump_version:
                    self.dump_gzip = True
                    self.no_archiver_gzip = True
        else:
            logging.fatal("Cannot find or execute the mongodump binary file %s!" % self.config.method.mongodump.binary)
            sys.exit(1)

        #TODO should this be in init or a sub-function?
        # Get a DB connection
        try:
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
                    self.program_name,
                    self.config.name
                ))

            if self.db:
                self.db.close()

            if self._lock:
                self._lock.release()

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

        # noinspection PyBroadException
        try:
            self._lock = Lock(self.config.lockfile)
        except Exception:
            logging.fatal("Could not acquire lock: '%s'! Is another %s process running? Exiting" % (self.config.lockfile, self.program_name))
            sys.exit(1)

        if not self.is_sharded:
            logging.info("Running backup of %s:%s in replset mode" % (self.config.host, self.config.port))

            self.archiver_threads = 1

            # get shard secondary
            try:
                self.replset = Replset(
                    self.db,
                    self.config.user,
                    self.config.password,
                    self.config.authdb,
                    self.config.replication.max_lag_secs,
                    self.config.replication.min_priority,
                    self.config.replication.max_priority
                )
                secondary    = self.replset.find_secondary()
                replset_name = secondary['replSet']

                self.secondaries[replset_name] = secondary
            except Exception, e:
                self.exception("Problem getting shard secondaries! Error: %s" % e)

            try:
                self.mongodumper = Dumper(
                    self.secondaries,
                    self.backup_root_directory,
                    self.config.method.mongodump.binary,
                    self.config.method.mongodump.gzip,
                    self.config.user,
                    self.config.password,
                    self.config.authdb,
                    None,
                    self.config.verbose
                )
                self.mongodumper.run()
            except Exception, e:
                self.exception("Problem performing replset mongodump! Error: %s" % e)

        else:
            logging.info("Running backup of %s:%s in sharded mode" % (self.host, self.port))

            # connect to balancer and stop it
            try:
                self.sharding = Sharding(
                    self.db,
                    self.user,
                    self.password,
                    self.authdb,
                    self.balancer_wait_secs,
                    self.balancer_sleep
                )
                self.sharding.get_start_state()
            except Exception, e:
                self.exception("Problem connecting to the balancer! Error: %s" % e)

            # get shard secondaries
            try:
                self.replset_sharded = ReplsetSharded(
                    self.sharding,
                    self.db,
                    self.user,
                    self.password,
                    self.authdb,
                    self.max_repl_lag_secs
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
            if self.no_oplog_tailer:
                logging.warning("Oplog tailing disabled! Skipping")
            else:
                try:
                    self.oplogtailer = OplogTailer(
                        self.secondaries,
                        self.backup_name,
                        self.backup_root_directory,
                        self.dump_gzip,
                        self.user,
                        self.password,
                        self.authdb
                    )
                    self.oplogtailer.run()
                except Exception, e:
                    self.exception("Failed to start oplog tailing threads! Error: %s" % e)

            # start the mongodumper threads
            try:
                self.mongodumper = Dumper(
                    self.secondaries, 
                    self.backup_root_directory,
                    self.backup_binary,
                    self.dump_gzip,
                    self.user,
                    self.password,
                    self.authdb,
                    self.sharding.get_config_server(),
                    self.verbose
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
                self.oplog_resolver = OplogResolver(self.oplog_summary, self.mongodumper_summary, self.dump_gzip,
                                                    self.resolver_threads)
                self.oplog_resolver.run()

        # archive (and optionally compress) backup directories to archive files (threaded)
        if self.config.archive.method == "none":
            logging.warning("Archiving disabled! Skipping")
        elif self.config.archive.method == "tar":
            try:
                self.archiver = ArchiverTar(
                    self.backup_root_directory, 
                    self.config.archive.compression,
                    self.config.archive.threads,
                    self.config.verbose
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
                    self.backup_root_directory,
                    self.backup_root_subdirectory,
                    self.config.upload.s3.bucket_name,
                    self.config.upload.s3.bucket_prefix,
                    self.config.upload.s3.access_key,
                    self.config.upload.s3.secret_key,
                    self.config.upload.s3.remove_uploaded,
                    self.config.upload.s3.url,
                    self.config.upload.s3.threads,
                    self.config.upload.s3.chunk_size_mb
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
        #            self.config.name,
        #            self.backup_duration
        #        ))
        #    except Exception, e:
        #        self.exception("Problem running NSCA notifier! Error: %s" % e)

        if self.db:
            self.db.close()

        self._lock.release()

        logging.info("Backup completed in %s sec" % self.backup_duration)
