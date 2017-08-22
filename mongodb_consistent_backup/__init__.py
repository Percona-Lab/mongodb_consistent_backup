import sys
import traceback

from Main import MongodbConsistentBackup


__version__ = "#.#.#"
git_commit  = 'GIT_COMMIT_HASH'
prog_name   = 'mongodb-consistent-backup'


# noinspection PyUnusedLocal
def run():
    try:
        m = MongodbConsistentBackup()
        m.run()
    except Exception, e:
        # noinspection PyUnusedLocal
        print "Backup failed: %s" % e
        traceback.print_exc()
        sys.exit(1)
