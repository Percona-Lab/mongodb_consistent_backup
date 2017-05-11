import boto
import logging
import os
import time

from mongodb_consistent_backup.Errors import OperationError
from mongodb_consistent_backup.Pipeline import Task
from mongodb_consistent_backup.Upload.GS import GSUploadThread


class GS(Task):
    def __init__(self, config):
        self.config = config
