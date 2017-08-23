import logging
import sys

from pynsca import NSCANotifier

from mongodb_consistent_backup.Errors import NotifyError, OperationError
from mongodb_consistent_backup.Pipeline import Task


class Nsca(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(Nsca, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.server     = self.config.notify.nsca.server
        self.check_name = self.config.notify.nsca.check_name
        self.check_host = self.config.notify.nsca.check_host
        self.password   = self.config.notify.nsca.password
        self.success    = 0
        self.warning    = 1
        self.critical   = 2
        self.failed     = self.critical
        self.notifier   = None

        self.mode_type  = ''
        self.encryption = 1
        if self.password:
            self.mode_type  = 'Secure '
            self.encryption = 16

        req_attrs = ['server', 'check_name', 'check_host']
        for attr in req_attrs:
            if not getattr(self, attr):
                raise OperationError('NSCA notifier module requires attribute: %s!' % attr)

        self.server_name = self.server
        self.server_port = 5667
        if ':' in self.server:
            self.server_name, port = self.server.split(":")
            self.server_port = int(port)
        self.server = "%s:%i" % (self.server_name, self.server_port)

        try:
            self.notifier = NSCANotifier(
                monitoring_server=self.server_name,
                monitoring_port=self.server_port,
                encryption_mode=self.encryption,
                password=self.password
            )
        except Exception, e:
            logging.error('Error initiating NSCANotifier! Error: %s' % e)
            raise OperationError(e)

    def close(self):
        pass

    def run(self, ret_code, output):
        if self.notifier:
            logging.info("Sending %sNSCA report to check host/name '%s/%s' at NSCA host %s" % (
                self.mode_type,
                self.check_host,
                self.check_name,
                self.server
            ))
            logging.debug('NSCA report message: "%s", return code: %i, check host/name: "%s/%s"' % (output, ret_code, self.check_host, self.check_name))
            # noinspection PyBroadException
            try:
                self.notifier.svc_result(self.check_host, self.check_name, ret_code, str(output))
                logging.debug('Sent %sNSCA report to host %s' % (self.mode_type, self.server))
            except Exception, e:
                logging.error('Failed to send %sNSCA report to host %s: %s' % (self.mode_type, self.server, sys.exc_info()[1]))
                raise NotifyError(e)
