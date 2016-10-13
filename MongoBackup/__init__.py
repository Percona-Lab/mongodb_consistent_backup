import os
import sys

from Backup import Backup
from Config import Config

__version__ = '#.#.#'
git_commit  = 'GIT_COMMIT_HASH'
prog_name   = os.path.basename(sys.argv[0])

# noinspection PyUnusedLocal
def run():
    config = Config()
    #config.program_name = prog_name
    #config.version      = __version__
    #config.git_commit   = git_commit

    if config.print_version:
        print "%s version: %s, git commit hash: %s" % (prog_name, __version__, git_commit)
        if config.verbose:
            print_python_versions()
        sys.exit(0)
    #if not config.backup.name:
    #    print('-n/-backup.name flag is required!')
    #if not config.backup_location:
    #    print('-l/-location flag is required!')
    #
    #if config.nsca_server and not config.config:
    #    req_attrs = ['nsca_check_name', 'nsca_check_host']
    #    for attr in req_attrs:
    #        if not getattr(options, attr):
    #            parser.error('--%s is a required field when using --nsca-server!' % attr)

    try:
        Backup(config).run()
    except Exception, e:
        # noinspection PyUnusedLocal
        print "Backup '%s' failed for mongodb instance %s:%s : %s" % (options.backup_name, options.host, options.port, e)
        sys.exit(1)
