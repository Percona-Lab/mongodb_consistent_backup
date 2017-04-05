import logging
import sys

from mongodb_consistent_backup.Common import config_to_string, parse_method
from mongodb_consistent_backup.Errors import OperationError


class Stage(object):
    def __init__(self, stage_name, manager, config, timers, base_dir, backup_dir, **kwargs):
        self.stage_name = stage_name
        self.manager    = manager
        self.config     = config
        self.timers     = timers
        self.base_dir   = base_dir
        self.backup_dir = backup_dir
        self.args       = kwargs

        self.running   = False
        self.stopped   = False
        self.completed = False

        self.stage   = "mongodb_consistent_backup.%s" % self.stage_name
        self.module  = None
        self.method  = None
        self._method = None

    def init(self):
        mod_class = None
        if self.method == "none":
            logging.info("%s stage disabled, skipping" % self.stage_name)
            return
        try:
            module    = sys.modules["%s.%s" % (self.stage, self.method.capitalize())]
            mod_class = getattr(module, self.method.capitalize())
        except LookupError, e:
            raise OperationError('Could not load method: %s' % self.method)
        if mod_class:
            self._method = mod_class(
                self.manager,
                self.config,
                self.timers,
                self.base_dir,
                self.backup_dir,
                **self.args
            )
            logging.debug("Loaded stage %s with method %s" % (self.stage, self.method.capitalize()))

    def has_method(self):
        if self._method:
            return True
        return False

    def close(self):
        logging.debug("Calling close on backup stage %s with method %s" % (self.stage, self.method.capitalize()))
        if self.has_method():
            self._method.close()

    def is_compressed(self):
        if self.has_method() and hasattr(self._method, "is_compressed"):
            return self._method.is_compressed()

    def compression(self, method=None):
        if self.has_method() and hasattr(self._method, "compression"):
            return self._method.compression(method)

    def threads(self, threads=None):
        if self.has_method() and hasattr(self._method, "thread"):
            return self._method.threads(threads)

    def run(self):
        if self.has_method():
            data = None
            try:
                self.timers.start(self.stage)
                self.running = True
                logging.info("Running stage %s with method: %s" % (self.stage, self.method.capitalize()))
                data = self._method.run()
            except Exception, e:
                raise OperationError(e)
            finally:
                self.running = False
                self.stopped = True
                self.timers.stop(self.stage)
                if self._method.completed:
                    logging.info("Completed running stage %s with method %s in %.2f seconds" % (self.stage, self.method.capitalize(), self.timers.duration(self.stage)))
                    self.completed = True
                else:
                    logging.error("Stage %s did not complete!" % self.stage)
                    raise OperationError("Stage %s did not complete!" % self.stage)
                self.close()
            return data
