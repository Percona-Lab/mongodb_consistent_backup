from Tar import ArchiverTar


class Archive:
    def __init__(self, config):
        self.config = config
        self._archiver = None

        self.init()

    def init(self):
        # archive (and optionally compress) backup directories to archive files (threaded)
        if self.config.archive.method == "none":
            logging.warning("Archiving disabled! Skipping")
        elif self.config.archive.method == "tar":
            try:
                self._archiver = Archive(
                    self.config,
                    self.backup_root_directory
                )
            except Exception, e:
                raise e

    def archive(self):
        if self._archiver:
            return self._archiver.run()

