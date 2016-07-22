import os
import sys

from optparse import OptionParser
from yaml import load
from Backup import Backup

__version__ = '#.#.#'
git_commit  = 'GIT_COMMIT_HASH'
prog_name   = os.path.basename(sys.argv[0])


def handle_options(parser):
    (options, args) = parser.parse_args()
    if len(sys.argv[1:]) == 0:
        print "No options parsed!"
        parser.print_help()
        sys.exit(1)

    # if a yaml config file is provided, re-parse the options with file-based defaults
    if options.config and os.path.isfile(options.config):
        try:
            f = open(options.config)
            config = load(f.read())
            f.close()
            for option in parser.option_list:
                dest = option.dest
                if dest is not None and dest in config:
                    parser.set_default(dest, config[dest])
            (options, args) = parser.parse_args()
        except Exception, e:
            print "Cannot load and/or parse config file %s! Error: %s" % (options.config, e)
            sys.exit(1)

    return options

def print_python_versions():
    import platform
    print "Python version: %s" % platform.python_version()
    print "Python modules:"

    import fabric.version
    print "\t%s: %s" % ('Fabric', fabric.version.get_version())
 
    modules = ['pymongo', 'multiprocessing', 'yaml', 'boto', 'filechunkio']
    for module_name in modules:
        module = __import__(module_name)
        if hasattr(module, '__version__'):
            print "\t%s: %s" % (module_name, module.__version__)


# noinspection PyUnusedLocal
def run():
    parser = OptionParser()
    parser.add_option("--version", dest="print_version", help="Display version number and exit", action="store_true", default=False)
    parser.add_option("-v", "--verbose", dest="verbose", help="Increase verbosity", action="store_true", default=False)
    parser.add_option("-c", "--config", dest="config", help="Use YAML config file as defaults")
    parser.add_option("-l", "--location", dest="backup_location", help="Directory to save the backup(s) to (required)")
    parser.add_option("-n", "--name", dest="backup_name", help="Backup name for this cluster/replset (required)")
    parser.add_option("-H", "--host", dest="host", help="MongoDB host to connect to (default: localhost)", default="localhost")
    parser.add_option("-P", "--port", dest="port", type="int", help="MongoDB port to connect to (default: 27017)", default=27017)
    parser.add_option("-u", "--user", dest="user", help="MongoDB user name to authenticate with (on all connections)")
    parser.add_option("-p", "--password", dest="password", help="MongoDB password to authenticate with (on all connections)")
    parser.add_option("-a", "--authdb", dest="authdb", help="MongoDB database name to authenticate against (on all connections)", default='admin')
    parser.add_option("-B", "--backup_binary", dest="backup_binary", help="Location of mongodump binary (default: /usr/bin/mongodump)", default="/usr/bin/mongodump")
    parser.add_option("-m", "--maxlag", dest="max_repl_lag_secs", type="int", help="Maximum MongoDB replication secondary slave drift in seconds (default: 5)", default=5)
    parser.add_option("-b", "--balancer_wait", dest="balancer_wait_secs", type="int", help="Maximum time in seconds to wait for MongoDB balancer to stop (default: 300)", default=300)
    parser.add_option("-R", "--resolver-threads", dest="resolver_threads", type="int", help="The number of threads to use for resolving oplogs (default: 2 per CPU)", default=None)
    parser.add_option("-A", "--archiver-threads", dest="archiver_threads", type="int", help="The number of threads to use for archiving/compressing (default: 1 per CPU)", default=None)
    parser.add_option("--nsca-server", dest="nsca_server", help="The host/port of the Nagios NSCA server (enables NSCA notifications)")
    parser.add_option("--nsca-password", dest="nsca_password", help="The password to use with the Nagios NSCA server")
    parser.add_option("--nsca-check-name", dest="nsca_check_name", help="The Nagios NSCA check name to report to")
    parser.add_option("--nsca-check-host", dest="nsca_check_host", help="The Nagios NSCA check hostname to report to")
    parser.add_option("--s3-bucket-name", dest="upload_s3_bucket_name", help="The AWS S3 Bucket name to upload backups to (enables S3 backups)")
    parser.add_option("--s3-bucket-prefix", dest="upload_s3_bucket_prefix", help="The AWS S3 Bucket prefix to upload backups to (default: /)", default="/")
    parser.add_option("--s3-access-key", dest="upload_s3_access_key", help="The AWS S3 Access Key to use for upload")
    parser.add_option("--s3-secret-key", dest="upload_s3_secret_key", help="The AWS S3 Secret Key to use for upload")
    parser.add_option("--s3-url", dest="upload_s3_url", help="The AWS S3 host/url to use for upload (default: s3.amazonaws.com)", default="s3.amazonaws.com")
    parser.add_option("--s3-threads", dest="upload_s3_threads", help="The number of threads to use for AWS S3 uploads (default: 4)", type="int", default=4)
    parser.add_option("--s3-chunk-mb", dest="upload_s3_chunk_size_mb", help="The size of multipart chunks for AWS S3 uploads (default: 50)", type="int", default=50)
    parser.add_option("--s3-remove-uploaded", dest="upload_s3_remove_uploaded", help="Remove local files after successful upload (default: false)", action="store_true", default=False)
    parser.add_option("--no-archive", dest="no_archiver", help="Disable archiving of backups directories post-resolving", action="store_true", default=False)
    parser.add_option("--no-archive-gzip", dest="no_archiver_gzip", help="Disable gzip compression of archive files", action="store_true", default=False)
    parser.add_option("--lazy", dest="no_oplog_tailer", help="Disable tailing/resolving of clusterwide oplogs. This makes a shard-consistent, not cluster-consistent backup", action="store_true", default=False)
    parser.add_option("--lock-file", dest="lock_file", help="Location of lock file (default: /tmp/%s.lock)" % prog_name, default="/tmp/%s.lock" % prog_name)
    parser.set_defaults()

    options = handle_options(parser)

    options.program_name = prog_name
    options.version      = __version__
    options.git_commit   = git_commit

    if options.print_version:
        print "%s version: %s, git commit hash: %s" % (prog_name, __version__, git_commit)
        if options.verbose:
            print_python_versions()
        sys.exit(0)
    if not options.backup_name:
        parser.error('-n/--name flag is required!')
    if not options.backup_location:
        parser.error('-l/--location flag is required!')

    if options.nsca_server and not options.config:
        req_attrs = ['nsca_check_name', 'nsca_check_host']
        for attr in req_attrs:
            if not getattr(options, attr):
                parser.error('--%s is a required field when using --nsca-server!' % attr)

    try:
        v = Backup(options)
        v.run()
    except Exception, e:
        # noinspection PyUnusedLocal
        print "Backup '%s' failed for mongodb instance %s:%s : %s" % (options.backup_name, options.host, options.port, e)
        sys.exit(1)
