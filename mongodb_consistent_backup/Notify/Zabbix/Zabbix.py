import logging
import sys

from pyzabbix import ZabbixMetric, ZabbixSender

from mongodb_consistent_backup.Errors import NotifyError, OperationError
from mongodb_consistent_backup.Pipeline import Task


class Zabbix(Task):
    def __init__(self, manager, config, timer, base_dir, backup_dir, **kwargs):
        super(Zabbix, self).__init__(self.__class__.__name__, manager, config, timer, base_dir, backup_dir, **kwargs)
        self.zabbix_server     = self.config.notify.zabbix.server
        self.zabbix_port       = self.config.notify.zabbix.port
        self.zabbix_use_config = self.config.notify.zabbix.use_config
        self.zabbix_key        = self.config.notify.zabbix.key
        self.zabbix_nodename   = self.config.notify.zabbix.node

        req_attrs = ['zabbix_key']
        for attr in req_attrs:
            if not getattr(self, attr):
                raise OperationError('Zabbix notifier module requires attribute: %s!' % attr)

        try:
            self.notifier = ZabbixSender(
                use_config=self.zabbix_use_config,
                zabbix_server=self.zabbix_server,
                zabbix_port=self.zabbix_port,
            )
        except Exception, e:
            logging.error("Error initiating ZabbixSender! Error: %s" % e)
            raise OperationError(e)

    def close(self):
        pass

    def run(self, ret_code):
        if self.notifier:
            logging.info("Sending Zabbix metric to item '%s:%s' to Zabbix Server" % (
                self.zabbix_nodename,
                self.zabbix_key,
            ))
            logging.debug("Zabbix metric return code: '%s', item key: '%s', node name: '%s'" % (ret_code, self.zabbix_key, self.zabbix_nodename))

            try:
                metrics = [ZabbixMetric(self.zabbix_nodename, self.zabbix_key, ret_code)]
                response = self.notifier.send(metrics)

                if response.failed > 0:
                    raise NotifyError("%s metric(s) failed out of %s" % (response.failed, response.total))
            except Exception, e:
                logging.error("Failed to send Zabbix metric to host" % (sys.exc_info()[1]))
                raise NotifyError(e)
            finally:
                logging.info("Zabbix report processed. Processed: %s, Failed: %s, Total: %s, Seconds spent: %s" % (
                    response.processed,
                    response.failed,
                    response.total,
                    response.time
                ))
