import sys
import traceback

from Main import MongodbConsistentBackup


# noinspection PyUnusedLocal
def run():
    try:
        m = MongodbConsistentBackup()
        m.run()
    except Exception, e:
        # noinspection PyUnusedLocal
        print "Backup '%s' failed for mongodb instance %s:%s : %s" % (m.config.name, m.config.host, m.config.port, e)
        traceback.print_exc()
        sys.exit(1)
