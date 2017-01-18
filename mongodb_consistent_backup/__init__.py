# TODO-timv Can we remove this?
import os
import sys

from Common import Config
from Main import MongodbConsistentBackup

__version__ = '#.#.#'
git_commit  = 'GIT_COMMIT_HASH'


# noinspection PyUnusedLocal
def run():
    try:
        config = Config()
    except Exception, e:
        print "Error setting up configuration: '%s'!" % e
        sys.exit(1)

    try:
        MongodbConsistentBackup(config).run()
    except Exception, e:
        # noinspection PyUnusedLocal
        print "Backup '%s' failed for mongodb instance %s:%s : %s" % (config.name, config.host, config.port, e)
        sys.exit(1)
