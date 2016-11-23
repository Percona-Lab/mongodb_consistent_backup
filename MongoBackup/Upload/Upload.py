from UploadS3 import UploadS3


class Upload:
    def __init__(self, config):
        self.config = config
        self._uploader = None

    def upload(self):
        pass
