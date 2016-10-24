from Dump import Dump
from Dumper import Dumper

def config(parser):
        parser.add_argument("-method.mongodump.binary", dest="method.mongodump.binary", help="Path to 'mongodump' binary (default: /usr/bin/mongodump)", default='/usr/bin/mongodump')
        parser.add_argument("-method.mongodump.gzip", dest="method.mongodump.gzip", help="Enable gzip compression on backup (default: true)", default=True, action="store_false")
        return parser
