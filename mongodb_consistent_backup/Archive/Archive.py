import logging

from Tar import Tar


class Archive:
    def __init__(self, config, backup_dir):
        self.config     = config
        self.backup_dir = backup_dir

        self.method    = None
        self._archiver = None
        self.init()

    def init(self):
        archive_method = self.config.archive.method
        if not archive_method or archive_method.lower() == "none":
            logging.info("Archiving disabled, skipping")
        else:
            self.method = archive_method.lower()
            logging.info("Using archiving method: %s" % self.method)
            try:
                self._archiver = globals()[self.method.capitalize()](
                    self.config,
                    self.backup_dir
                )
            except Exception, e:
                raise e

    def compression(self, method=None):
	if self._archiver:
	    return self._archiver.compression(method)

    def threads(self, threads=None):
	if self._archiver:
	    return self._archiver.threads(threads)

    def archive(self):
        if self._archiver:
            config_vars = ""
            for key in self.config.archive:
                config_vars += "%s=%s," % (key, self.config.archive[key])
            logging.info("Archiving with method: %s (options: %s)" % (self.method, str(config_vars[:-1])))
            return self._archiver.run()

    def close(self):
        if self._archiver:
            return self._archiver.close()
