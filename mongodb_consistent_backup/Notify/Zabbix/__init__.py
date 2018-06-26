import socket


def config(parser):
    parser.add_argument("--notify.zabbix.use_config", dest="notify.zabbix.use_config",
                        help="Use Zabbix Agent configuration (default: True)",
                        default=True, choices=[True, False])
    parser.add_argument("--notify.zabbix.server", dest="notify.zabbix.server",
                        help="Zabbix Server hostname/ip address, not used if notify.zabbix.use_config is True (default: none)",
                        default='127.0.0.1', type=str)
    parser.add_argument("--notify.zabbix.port", dest="notify.zabbix.port",
                        help="Zabbix Server port, not used if notify.zabbix.use_config is True (default: none)",
                        default=10051, type=int)
    parser.add_argument("--notify.zabbix.key", dest="notify.zabbix.key",
                        help="Zabbix Server item key", default=None, type=str)
    parser.add_argument("--notify.zabbix.node", dest="notify.zabbix.node",
                        help="Node name monitored by Zabbix Server (default: nodename)",
                        default=socket.gethostname(), type=str)
    return parser
