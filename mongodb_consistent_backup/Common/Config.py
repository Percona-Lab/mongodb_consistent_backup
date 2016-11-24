import sys

from argparse import Action
from yconf import BaseConfiguration


__version__ = '#.#.#'
git_commit  = 'GIT_COMMIT_HASH'
prog_name   = 'mongodb_consistent_backup'


class PrintVersions(Action):
    def __init__(self, option_strings, dest, nargs=0, **kwargs):
        super(PrintVersions, self).__init__(option_strings=option_strings, dest=dest, nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        print "%s version: %s, git commit hash: %s" % (prog_name, __version__, git_commit)

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
        sys.exit(0)


class ConfigParser(BaseConfiguration):
    def makeParser(self):
        parser = super(ConfigParser, self).makeParser()
        parser.add_argument("-V", "-version", dest="version", help="Print mongodb_consistent_backup version info and exit", action=PrintVersions)
        parser.add_argument("-v", "-verbose", dest="verbose", help="Verbose output", default=False, action="store_true")
        parser.add_argument("-H", "-host", dest="host", help="MongoDB hostname/IP (default: localhost)", default="localhost", type=str)
        parser.add_argument("-P", "-port", dest="port", help="MongoDB port (default: 27017)", default=27017, type=int)
        parser.add_argument("-u", "-user", dest="user", help="MongoDB Authentication Username (for optional auth)", type=str)
        parser.add_argument("-p", "-password", dest="password", help="MongoDB Authentication Password (for optional auth)", type=str)
        parser.add_argument("-a", "-authdb", dest="authdb", help="MongoDB Auth Database (for optional auth - default: admin)", default='admin', type=str)
        parser.add_argument("-n", "-backup.name", dest="backup.name", help="Name of the backup set (required)", type=str)
        parser.add_argument("-l", "-backup.location", dest="backup.location", help="Base path to store the backup data (required)", type=str)
        parser.add_argument("-m", "-backup.method", dest="backup.method", help="Method to be used for backup (default: mongodump)", default='mongodump', choices=['mongodump'])
        parser.add_argument("-lockfile", dest="lockfile", help="Location of lock file (default: /tmp/mongodb_consistent_backup.lock)", default='/tmp/mongodb_consistent_backup.lock', type=str)
        parser.add_argument("-sharding.balancer.wait_secs", dest="sharding.balancer.wait_secs", help="Maximum time to wait for balancer to stop, in seconds (default: 300)", default=300, type=int)
        parser.add_argument("-sharding.balancer.ping_secs", dest="sharding.balancer.ping_secs", help="Interval to check balancer state, in seconds (default: 3)", default=3, type=int)
        return parser


class Config(object):
    def __init__(self, cmdline=None, **args):
        if not self.cmdline:
            self.cmdline = sys.argv[1:]
        self._config = ConfigParser()
        self.parse_submodules(args)
        self.parse()

        self.version    = __version__
        self.git_commit = git_commit

    def _get(self, keys, data=None):
        if not data:
            data = self._config
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self._get(rest, data[key])
        else:
            return data[keys]

    def parse_submodules(self, args):
        if 'submodules' in args:
            parser = self._config.parser
            for submodule in args['submodules']:
                submodule.config(parser)

    def check_required(self):
        required = [
            'backup.name',
            'backup.location'
        ]
        for key in required:
            try:
                self._get(key)
            except:
                raise Exception, 'Field "%s" must be set via command-line or config file!' % key, None

    def parse(self):
        self._config.parse(self.cmdline)
        self.check_required()

    def __getattr__(self, key):
        try:
            return self._config.get(key)
        except:
            return None
