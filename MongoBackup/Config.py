import sys

from yconf import BaseConfiguration


class ConfigParser(BaseConfiguration):
	def makeParser(self):
		parser = super(ConfigParser, self).makeParser()
		parser.add_argument("-V", "-version", dest="print_version", help="Print version info and exit", default=False, action="store_true")
		parser.add_argument("-v", "-verbose", dest="verbose", help="Verbose output", default=False, action="store_true")
		parser.add_argument("-n", "-name", dest="name", help="Name of the backup set (required)", required=True, type=str)
		parser.add_argument("-l", "-location", dest="location", help="Base path to store the backup data (required)", required=True, type=str)
		parser.add_argument("-H", "-host", dest="host", help="MongoDB Hostname/IP", type=str)
		parser.add_argument("-P", "-port", dest="port", help="MongoDB Port", type=int)
		parser.add_argument("-u", "-user", dest="user", help="MongoDB Username (for optional auth)", type=str)
		parser.add_argument("-p", "-password", dest="password", help="MongoDB Password (for optional auth)", type=str)
		parser.add_argument("-a", "-authdb", dest="authdb", help="MongoDB Auth Database (default: admin)", default='admin', type=str)
		parser.add_argument("-t", "-backup.type", dest="backup.type", help="Backup method type (choice: mongodump, default: mongodump)", default='mongodump', choices=['mongodump'], type=str)
		parser.add_argument("-lockfile", dest="lockfile", help="Location of lock file (default: /tmp/mongodb_consistent_backup.lock)", default='/tmp/mongodb_consistent_backup.lock', type=str)
		parser.add_argument("-archiver.disabled", dest="archiver.disabled", help="Disable the archive step (default: false)", default=False, action="store_true")
		parser.add_argument("-archiver.disable_gzip", dest="archiver.disable_gzip", help="Disable gzip compression in the archive step (default: false)", default=False, action="store_true")
		parser.add_argument("-archiver.threads", dest="archiver.threads", help="Number of threads to use in archive phase (default: 1-per-CPU)", default=0, type=int)
		parser.add_argument("-replication.max_lag_secs", dest="replication.max_lag_secs", help="Maximum replication lag of chosen backup replica(s), in seconds (default: 5)", default=5, type=int)
		parser.add_argument("-replication.use_hidden", dest="replication.use_hidden", help="Use hidden secondary/replica members only for backup (default: false)", default=False, action="store_true")
		parser.add_argument("-resolver.threads", dest="resolver.threads", help="Number of threads to use during resolver step (default: 1-per-CPU)", default=0, type=int)
		parser.add_argument("-sharding.balancer_wait_secs", dest="sharding.balancer_wait_secs", help="Maximum time to wait for balancer to stop, in seconds (default: 300)", default=300, type=int)
		parser.add_argument("-sharding.balancer_ping_secs", dest="sharding.balancer_ping_secs", help="Interval to check balancer state, in seconds (default: 3)", default=3, type=int)
		return parser


class Config(object):
	def __init__(self, children, cmdline=None):
		self.cmdline = cmdline
		if not self.cmdline:
			self.cmdline = sys.argv[1:]
		self._config = ConfigParser(True, False)
		self.parse()

	def parse(self, children=[]):
		for child in children:
			child.parse_arguments(self._config)
		return self._config.parse(self.cmdline)

	def __getattr__(self, key):
		try:
			return self._config.get(key)
		except:
			return None
