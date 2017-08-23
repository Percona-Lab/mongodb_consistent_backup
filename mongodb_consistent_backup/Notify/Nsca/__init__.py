from Nsca import Nsca  # NOQA


def config(parser):
    parser.add_argument("--notify.nsca.server", dest="notify.nsca.server",
                        help="Notifier NSCA server hostname and port", default=None, type=str)
    parser.add_argument("--notify.nsca.password", dest="notify.nsca.password", help="Notifier NSCA server password",
                        default=None, type=str)
    parser.add_argument("--notify.nsca.check_name", dest="notify.nsca.check_name",
                        help="Notifier NSCA Nagios check name", default=None, type=str)
    parser.add_argument("--notify.nsca.check_host", dest="notify.nsca.check_host",
                        help="Notifier NSCA Nagios check host", default=None, type=str)
    return parser
