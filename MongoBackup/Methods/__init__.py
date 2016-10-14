from Dump import Dump
from Dumper import Dumper

def config(parser):
	parser.add_argument("-backup.method", dest="backup.method", help="Backup method type (default: mongodump)", default='mongodump', choices=['mongodump'])
	return parser

