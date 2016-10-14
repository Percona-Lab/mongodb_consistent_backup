from Tar import ArchiverTar

def config(parser):
	parser.add_argument("-archiver.method", dest="archiver.method", help="Archive method (default: tar)", default='tar', choices=['tar','none'])
	parser.add_argument("-archiver.disable_compression", dest="archiver.disable_compression", help="Disable compression in the archive step (default: false)", default=False, action="store_true")
	parser.add_argument("-archiver.threads", dest="archiver.threads", help="Number of threads to use in archive phase (default: 1-per-CPU)", default=0, type=int)
	return parser
