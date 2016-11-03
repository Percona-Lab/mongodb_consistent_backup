from Dump import Dump
from Dumper import Dumper

def config(parser):
        parser.add_argument("-backup.mongodump.binary", dest="backup.mongodump.binary", help="Path to 'mongodump' binary (default: /usr/bin/mongodump)", default='/usr/bin/mongodump')
        parser.add_argument("-backup.mongodump.gzip", dest="backup.mongodump.gzip", help="Enable gzip compression on backup (default: true)", default=True, action="store_false")
        return parser
