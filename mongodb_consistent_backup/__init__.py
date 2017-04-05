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
        #print "Backup '%s' failed for mongodb instance %s:%s : %s" % (m.config.backup.name, m.config.host, m.config.port, e)
        print e
        traceback.print_exc()
        sys.exit(1)
