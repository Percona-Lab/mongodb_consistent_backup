import os
import sys
import logging

from datetime import datetime
from fabric.api import local, hide, settings
from multiprocessing import current_process
from signal import signal, SIGINT, SIGTERM
from time import time

from Archiver import Archiver
from Common import DB, Lock
from Methods import Dumper
from Notify import NotifyNSCA
from Oplog import OplogTailer, OplogResolver
from Replset import Replset
from ReplsetSharded import ReplsetSharded
from Sharding import Sharding
from Upload import UploadS3


class Backup(object):
    def __init__(self, options):
        self.program_name = None
        self.version = None
        self.git_commit = None
        self.host = 'localhost'
        self.port = 27017
        self.authdb = 'admin'
        self.password = None
        self.user = None
        self.connection = None
        self.max_repl_lag_secs = 5
        self.backup_name = None
        self.backup_binary = None
        self.backup_location = None
        self.dump_gzip = False
        self.balancer_wait_secs = 300
        self.balancer_sleep = 5 
        self.archiver_threads = 1
        self.resolver_threads = 1
        self.notify_nsca = None
        self.nsca_server = None
        self.nsca_password = None
        self.nsca_check_name = None
        self.nsca_check_host = None
        self.no_archiver = False
        self.no_archiver_gzip = False
        self.no_oplog_tailer = False
        self.verbose = False
        self.oplog_tail_extra = 5
        self.uploader_s3 = None
        self.upload_s3_url = None
        self.upload_s3_threads = None
        self.upload_s3_bucket_name = None
        self.upload_s3_bucket_prefix = None
        self.upload_s3_access_key = None
        self.upload_s3_secret_key = None
        self.upload_s3_remove_uploaded = None
        self.upload_s3_chunk_size_mb = None
        self.archiver = None
        self.sharding = None
        self.replset  = None
        self.mongodumper = None
        self.oplogtailer = None
        self.oplog_resolver = None
        self.backup_duration = None
        self.end_time = None

        self._lock = None
        self.log_level = logging.INFO
        self.start_time = time()
        self.oplog_threads = []
        self.oplog_summary = {}
        self.secondaries   = {}
        self.mongodumper_summary = {}

        # Setup options are properies and connection to node
        for option in vars(options):
            setattr(self, option, getattr(options, option))

        # Check for required fields:
        required = ['program_name', 'version', 'git_commit', 'backup_name', 'backup_binary', 'backup_location'] 
        for field in required:
            try:
                getattr(self, field)
            except Exception:
                raise Exception, 'Field: %s is required by %s!' % (field, __name__), None

        # Set default lock file:
	if not self.lock_file:
            self.lock_file = '/tmp/%s.lock' % self.program_name

        # Setup logging
        if self.verbose:
            self.log_level = logging.DEBUG
        logging.basicConfig(level=self.log_level,
                            format='[%(asctime)s] [%(levelname)s] [%(processName)s] [%(module)s:%(funcName)s:%(lineno)d] %(message)s')

        # Check mongodump binary and set version + dump_gzip flag if 3.2+
        if os.path.isfile(self.backup_binary) and os.access(self.backup_binary, os.X_OK):
            with hide('running', 'warnings'), settings(warn_only=True):
                self.mongodump_version = tuple(
                    local("%s --version|awk 'NR >1 {exit}; /version/{print $NF}'" % self.backup_binary,
                          capture=True).split("."))
                if tuple("3.2.0".split(".")) < self.mongodump_version:
                    self.dump_gzip = True
                    self.no_archiver_gzip = True
        else:
            logging.fatal("Cannot find or execute the mongodump binary file %s!" % self.backup_binary)
            sys.exit(1)

        # Get a DB connection
        try:
            self.db         = DB(self.host, self.port, self.user, self.password, self.authdb)
            self.connection = self.db.connection()
            self.is_sharded = self.connection.is_mongos
        except Exception, e:
            raise e

        # Setup backup dir name:
        time_string = datetime.now().strftime("%Y%m%d_%H%M")
        self.backup_root_subdirectory = "%s/%s" % (self.backup_name, time_string)
        self.backup_root_directory    = "%s/%s" % (self.backup_location, self.backup_root_subdirectory)

        # Setup the notifier:
        try:
            if self.nsca_server and self.nsca_check_name:
                self.notify_nsca = NotifyNSCA(
                    self.nsca_server,
                    self.nsca_check_name,
                    self.nsca_check_host,
                    self.nsca_password
                )
        except Exception, e:
            raise e

        # Setup signal handler:
        signal(SIGINT, self.cleanup_and_exit)
        signal(SIGTERM, self.cleanup_and_exit)

    # noinspection PyUnusedLocal
    def cleanup_and_exit(self, code, frame):
        if current_process().name == "MainProcess":
            logging.info("Starting cleanup and exit procedure! Killing running threads")

            submodules = ['replset', 'sharding', 'mongodumper', 'oplogtailer', 'archiver', 'uploader_s3']
            for submodule_name in submodules:
                submodule = getattr(self, submodule_name)
                if submodule:
                    submodule.close()

            if self.notify_nsca:
                self.notify_nsca.notify(self.notify_nsca.critical, "%s: backup '%s' failed!" % (
                    self.program_name,
                    self.backup_name
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
        logging.info("Starting %s version %s (git commit hash: %s)" % (self.program_name, self.version, self.git_commit))

        # noinspection PyBroadException
        try:
            self._lock = Lock(self.lock_file)
        except Exception:
            logging.fatal("Could not acquire lock! Is another %s process running? Exiting" % self.program_name)
            sys.exit(1)

        if not self.is_sharded:
            logging.info("Running backup of %s:%s in replset mode" % (self.host, self.port))

            self.archiver_threads = 1

            # get shard secondary
            try:
                self.replset = Replset(
                    self.db,
                    self.user,
                    self.password,
                    self.authdb,
                    self.max_repl_lag_secs
                )
                secondary    = self.replset.find_secondary()
                replset_name = secondary['replSet']

                self.secondaries[replset_name] = secondary
                self.replset.close()
            except Exception, e:
                self.exception("Problem getting shard secondaries! Error: %s" % e)

            try:
                self.mongodumper = Dumper(
                    self.secondaries,
                    self.backup_root_directory,
                    self.backup_binary,
                    self.dump_gzip,
                    self.user,
                    self.password,
                    self.authdb,
                    None,
                    self.verbose
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
                self.replset = ReplsetSharded(
                    self.sharding,
                    self.db,
                    self.user,
                    self.password,
                    self.authdb,
                    self.max_repl_lag_secs
                )
                self.secondaries = self.replset.find_secondaries()
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
        if self.no_archiver:
            logging.warning("Archiving disabled! Skipping")
        else:
            try:
                self.archiver = Archiver(self.backup_root_directory, self.no_archiver_gzip, self.archiver_threads, self.verbose)
                self.archiver.run()
            except Exception, e:
                self.exception("Problem performing archiving! Error: %s" % e)

        self.end_time = time()
        self.backup_duration = self.end_time - self.start_time

        # AWS S3 secure multipart uploader (optional)
        if self.upload_s3_bucket_name and self.upload_s3_bucket_prefix and self.upload_s3_access_key and self.upload_s3_secret_key:
            try:
                self.uploader_s3 = UploadS3(
                    self.backup_root_directory,
                    self.backup_root_subdirectory,
                    self.upload_s3_bucket_name,
                    self.upload_s3_bucket_prefix,
                    self.upload_s3_access_key,
                    self.upload_s3_secret_key,
                    self.upload_s3_remove_uploaded,
                    self.upload_s3_url,
                    self.upload_s3_threads,
                    self.upload_s3_chunk_size_mb
                )
                self.uploader_s3.run()
            except Exception, e:
                self.exception("Problem performing AWS S3 multipart upload! Error: %s" % e)

        # send notifications of backup state
        if self.notify_nsca:
            try:
                self.notify_nsca.notify(self.notify_nsca.success, "%s: backup '%s' succeeded in %s secs" % (
                    self.program_name,
                    self.backup_name,
                    self.backup_duration
                ))
            except Exception, e:
                self.exception("Problem running NSCA notifier! Error: %s" % e)

        if self.db:
            self.db.close()

        self._lock.release()

        logging.info("Backup completed in %s sec" % self.backup_duration)
