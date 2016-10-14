from NSCA import NotifyNSCA

def config(parser):
	parser.add_argument("-notifier.method", dest="notifier.method", help="Notifier method (default: nsca)", default='nsca', choices=['nsca','none'])
	parser.add_argument("-notifier.nsca.server", dest="notifier.nsca.server", help="Notifier NSCA server hostname and port", type=str)
	parser.add_argument("-notifier.nsca.password", dest="notifier.nsca.password", help="Notifier NSCA server password", type=str)
	parser.add_argument("-notifier.nsca.check_name", dest="notifier.nsca.check_name", help="Notifier NSCA Nagios check name", type=str)
	parser.add_argument("-notifier.nsca.check_host", dest="notifier.nsca.check_host", help="Notifier NSCA Nagios check host", type=str)
	return parser
