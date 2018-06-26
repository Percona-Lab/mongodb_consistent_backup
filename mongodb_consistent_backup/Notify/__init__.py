from Notify import Notify  # NOQA


def config(parser):
    parser.add_argument("--notify.method", dest="notify.method", help="Notifier method (default: none)", default='none', choices=['nsca', 'zabbix', 'none'])
    return parser
