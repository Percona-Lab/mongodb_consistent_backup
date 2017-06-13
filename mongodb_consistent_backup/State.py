import logging
import os
import platform

from bson import BSON, decode_all
from time import time

from mongodb_consistent_backup.Common import Lock
from mongodb_consistent_backup.Errors import OperationError


class StateBase(object):
    def __init__(self, base_dir, config, filename="meta.bson", state_version=1, meta_name="mongodb-consistent-backup_META"):
        self.meta_name  = meta_name
        self.state_dir  = os.path.join(base_dir, self.meta_name)
        self.state_lock = os.path.join(base_dir, "%s.lock" % self.meta_name)
        self.state_file = os.path.join(self.state_dir, filename)
        self.state = {
            "name":          config.backup.name,
            "path":          base_dir,
            "state_version": state_version
        }
        self.lock = Lock(self.state_lock, False)

        if not os.path.isdir(self.state_dir):
            # try normal mkdir first, fallback to recursive mkdir if there is an exception
            try:
                os.mkdir(self.state_dir)
            except:
                try:
                    os.makedirs(self.state_dir)
                except Exception, e:
                    raise OperationError(e)

    def merge(self, new, old):
        merged = old.copy()
        merged.update(new)
        return merged 

    def load(self, load_one=False):
         f = None
         try:
            f = open(self.state_file, "r")
            data = decode_all(f.read())
            if load_one and len(data) > 0:
                return data[0]
            return data
         except Exception, e:
            raise e
         finally:
            if f:
                f.close()

    def write(self, do_merge=False):
        f = None
        try:
            self.lock.acquire()
            if do_merge and os.path.isfile(self.state_file):
                curr = self.load(True)
                data = self.merge(self.state, curr)
            f = open(self.state_file, 'w+')
            logging.debug("Writing %s state file: %s" % (self.__class__.__name__, self.state_file))
            self.state['updated_at'] = int(time())
            f.write(BSON.encode(self.state))
        finally:
            if f:
                f.close()
            self.lock.release()

    
class StateBaseReplset(StateBase):
    def __init__(self, base_dir, config, backup_time, set_name, filename):
        StateBase.__init__(self, base_dir, config, filename)
        self.state['backup']      = True
        self.state['backup_name'] = backup_time
        self.state['replset']     = set_name

    def load_state(self, replset):
        self.state = self.merge(replset, self.state)


class StateBackupReplset(StateBaseReplset):
    def __init__(self, base_dir, config, backup_time, set_name):
        StateBaseReplset.__init__(self, base_dir, config, backup_time, set_name, "replset.bson")


class StateOplog(StateBaseReplset):
    def __init__(self, base_dir, config, backup_time, set_name):
        StateBaseReplset.__init__(self, base_dir, config, backup_time, set_name, "oplog.bson")


class StateBackup(StateBase):
    def __init__(self, base_dir, config, backup_time, seed_uri, argv=None):
        StateBase.__init__(self, base_dir, config)
        self.base_dir            = base_dir
        self.state['backup']     = True
        self.state['name']       = backup_time
        self.state['method']     = config.backup.method
        self.state['path']       = base_dir
        self.state['cmdline']    = argv
        self.state['config']     = config.dump()
        self.state['version']    = config.version
        self.state['git_commit'] = config.git_commit
        self.state['host']     = {
            'hostname': platform.node(),
            'uname':    platform.uname(),
            'python': {
                'build':   platform.python_build(),
                'version': platform.python_version()
            }
        }
        self.state['seed']       = {
            'uri':     seed_uri.str(),
            'replset': seed_uri.replset
        }
        self.init()

    def init(self):
        logging.info("Initializing backup state directory: %s" % self.base_dir)

    def set(self, name, summary):
        self.state[name] = summary
        self.write(True)        


class StateRoot(StateBase):
    def __init__(self, base_dir, config):
        StateBase.__init__(self, base_dir, config)
        self.base_dir = base_dir
        self.state['root'] = True

        self.init()

    def init(self):
        logging.info("Initializing root state directory %s" % self.base_dir)
        self.load_backups()

    def load_backups(self):
        backups = []
        if os.path.isdir(self.base_dir):
            for subdir in os.listdir(self.base_dir):
                try:
                    bkp_path = os.path.join(self.base_dir, subdir)
                    if subdir == self.meta_name or os.path.islink(bkp_path):
                        continue
                    state_path = os.path.join(bkp_path, self.meta_name)
                    state_file = os.path.join(state_path, "meta.bson")
                    done_path  = os.path.join(state_path, "done.bson")
                    if os.path.isdir(state_path) and os.path.isfile(state_file) and os.path.isfile(done_path):
                        backups.append(state_file)
                except:
                    continue
            logging.info("Found %i existing completed backups for set" % len(backups))
        return backups
            
class StateDoneStamp(StateBase):
    def __init__(self, base_dir, config):
        StateBase.__init__(self, base_dir, config, "done.bson")
        self.state = {'done': True}

