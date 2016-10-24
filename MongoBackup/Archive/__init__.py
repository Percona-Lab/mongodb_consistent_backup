from Tar import ArchiveTar

def config(parser):
	print parser
	parser.add_argument("-archiver.method", dest="archiver.method", help="Archive method (default: tar)", default='tar', choices=['tar','none'])
	parser.add_argument("-archiver.compression", dest="archiver.compression", help="Archive compression method (default: gzip)", default='gzip', choices=['gzip','none'])
	parser.add_argument("-archiver.threads", dest="archiver.threads", help="Number of threads to use in archive phase (default: 1-per-CPU)", default=0, type=int)
	return parser
