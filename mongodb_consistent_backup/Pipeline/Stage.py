import logging
import sys

from mongodb_consistent_backup.Errors import Error, OperationError

from Task import Task


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

        self.stage  = "mongodb_consistent_backup.%s" % self.stage_name
        self.module = None
        self.task   = "none"
        self._task  = None

    def init(self):
        mod_class = None
        if self.task == "none":
            logging.info("%s stage disabled, skipping" % self.stage_name)
            return
        try:
            module    = sys.modules["%s.%s" % (self.stage, self.task.capitalize())]
            mod_class = getattr(module, self.task.capitalize())
        except LookupError, e:
            raise OperationError('Could not load task %s: %s' % (self.task, e))
        if mod_class:
            self._task = mod_class(
                self.manager,
                self.config,
                self.timers,
                self.base_dir,
                self.backup_dir,
                **self.args
            )
            if isinstance(self._task, Task):
                logging.debug("Loaded stage %s with task %s" % (self.stage, self.task.capitalize()))
            else:
                raise Error("Loaded class must be child of mongodb_consistent_backup.Pipeline.Task!")

    def has_task(self):
        if self._task:
            return True
        return False

    def close(self):
        if self.has_task() and not self.stopped:
            logging.debug("Calling close on backup stage %s with task %s" % (self.stage, self.task.capitalize()))
            self._task.close()
            self.running = False
            self.stopped = True

    def is_compressed(self):
        if self.has_task() and hasattr(self._task, "is_compressed"):
            return self._task.is_compressed()
        return False

    def compression(self, task=None):
        if self.has_task() and hasattr(self._task, "compression"):
            return self._task.compression(task)

    def threads(self, threads=None):
        if self.has_task() and hasattr(self._task, "thread"):
            return self._task.threads(threads)

    def run(self):
        if self.has_task():
            data = None
            try:
                self.timers.start(self.stage)
                self.running = True
                logging.info("Running stage %s with task: %s" % (self.stage, self.task.capitalize()))
                data = self._task.run()
                self.stopped = True
            except Exception, e:
                logging.error("State %s returned error: %s" % (self.stage, e))
                raise OperationError(e)
            finally:
                self.running = False
                self.timers.stop(self.stage)
                if self._task.completed:
                    logging.info("Completed running stage %s with task %s in %.2f seconds" % (
                        self.stage,
                        self.task.capitalize(),
                        self.timers.duration(self.stage))
                    )
                    self.completed = True
                else:
                    logging.error("Stage %s did not complete!" % self.stage)
                    raise OperationError("Stage %s did not complete!" % self.stage)
                self.close()
            return data
