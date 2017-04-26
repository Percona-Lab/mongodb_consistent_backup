from Notify import Notify


def config(parser):
    parser.add_argument("--notify.method", dest="notify.method", help="Notifier method (default: none)", default='none', choices=['nsca','none'])
    return parser
