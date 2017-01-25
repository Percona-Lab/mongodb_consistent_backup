import sys

from Main import MongodbConsistentBackup


# noinspection PyUnusedLocal
def run():
    try:
        MongodbConsistentBackup().run()
    except Exception, e:
        # noinspection PyUnusedLocal
        print "Backup '%s' failed for mongodb instance %s:%s : %s" % (config.name, config.host, config.port, e)
        sys.exit(1)
