import json
import mongodb_consistent_backup
import sys

from argparse import Action
from pkgutil import walk_packages
from yconf import BaseConfiguration
from yconf.util import NestedDict


class PrintVersions(Action):
    def __init__(self, option_strings, dest, nargs=0, **kwargs):
        super(PrintVersions, self).__init__(option_strings=option_strings, dest=dest, nargs=nargs, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        print "%s version: %s, git commit hash: %s" % (mongodb_consistent_backup.prog_name, mongodb_consistent_backup.__version__, mongodb_consistent_backup.git_commit)

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
    def makeParserLoadSubmodules(self, parser):
        for _, modname, ispkg in walk_packages(path=mongodb_consistent_backup.__path__, prefix=mongodb_consistent_backup.__name__+'.'):
            if not ispkg:
                continue
            try:
                components = modname.split('.')
                mod = __import__(components[0])
                for comp in components[1:]:
                    mod = getattr(mod, comp)
                parser = mod.config(parser)
            except AttributeError, e:
                continue
        return parser

    def makeParser(self):
        parser = super(ConfigParser, self).makeParser()
        parser.add_argument("-V", "--version", dest="version", help="Print mongodb_consistent_backup version info and exit", action=PrintVersions)
        parser.add_argument("-v", "--verbose", dest="verbose", help="Verbose output", default=False, action="store_true")
        parser.add_argument("-H", "--host", dest="host", help="MongoDB Hostname, IP address or '<replset>/<host:port>,<host:port>,..' URI (default: localhost)", default="localhost", type=str)
        parser.add_argument("-P", "--port", dest="port", help="MongoDB Port (default: 27017)", default=27017, type=int)
        parser.add_argument("-u", "--user", "--username", dest="username", help="MongoDB Authentication Username (for optional auth)", type=str)
        parser.add_argument("-p", "--password", dest="password", help="MongoDB Authentication Password (for optional auth)", type=str)
        parser.add_argument("-a", "--authdb", dest="authdb", help="MongoDB Auth Database (for optional auth - default: admin)", default='admin', type=str)
        parser.add_argument("-L", "--log-dir", dest="log_dir", help="Path to write log files to (default: disabled)", default='', type=str)
        parser.add_argument("--lock-file", dest="lock_file", help="Location of lock file (default: /tmp/mongodb-consistent-backup.lock)", default='/tmp/mongodb-consistent-backup.lock', type=str)
        parser.add_argument("--sharding.balancer.wait_secs", dest="sharding.balancer.wait_secs", help="Maximum time to wait for balancer to stop, in seconds (default: 300)", default=300, type=int)
        parser.add_argument("--sharding.balancer.ping_secs", dest="sharding.balancer.ping_secs", help="Interval to check balancer state, in seconds (default: 3)", default=3, type=int)
        return self.makeParserLoadSubmodules(parser)


class Config(object):
    # noinspection PyUnusedLocal
    def __init__(self):
        self._config = ConfigParser()
        self.parse()

        self.version    = mongodb_consistent_backup.__version__
        self.git_commit = mongodb_consistent_backup.git_commit

    def _get(self, keys, data=None):
        if not data:
            data = self._config
        if "." in keys:
            key, rest = keys.split(".", 1)
            return self._get(rest, data[key])
        else:
            return data[keys]

    def check_required(self):
        required = [
            'backup.name',
            'backup.location'
        ]
        for key in required:
            try:
                self._get(key)
            except:
                raise mongodb_consistent_backup.Errors.OperationError('Field "%s" must be set via command-line or config file!' % key)

    def parse(self):
        self._config.parse(self.cmdline)
        self.check_required()

    def to_dict(self, data):
        if isinstance(data, dict) or isinstance(data, NestedDict):
            ret = {}
            for key in data:
                value = self.to_dict(data[key])
                if value and key is not ('merge'):
                    if key == "password":
                        value = "******"
                    ret[key] = value
            return ret
        elif isinstance(data, (str, int, bool)): # or isinstance(data, int) or isinstance(data, bool):
            return data

    def dump(self):
        return self.to_dict(self._config)

    def to_json(self):
        return json.dumps(self.dump(), sort_keys=True)

    def __repr__(self):
        return self.to_json()

    def __getattr__(self, key):
        try:
            return self._config.get(key)
        # TODO-timv What can we do to make this better?
        except:
            return None
