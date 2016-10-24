import os
import sys

from Backup import Backup
from Common import Config


__version__ = '#.#.#'
git_commit  = 'GIT_COMMIT_HASH'


# noinspection PyUnusedLocal
def run():
    try:
        config = Config(submodules=[
		Archive,
		Notify,
		Oplog,
		Replication,
		Upload
	])
    except Exception, e:
        print "Error setting up configuration: '%s'!" % e
        sys.exit(1)

    try:
        Backup(config).run()
    except Exception, e:
        # noinspection PyUnusedLocal
        print "Backup '%s' failed for mongodb instance %s:%s : %s" % (config.name, config.host, config.port, e)
        sys.exit(1)
