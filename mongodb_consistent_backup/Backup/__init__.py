from Backup import Backup


def config(parser):
    parser.add_argument("--backup.mongodump.binary", dest="backup.mongodump.binary", help="Path to 'mongodump' binary (default: /usr/bin/mongodump)", default='/usr/bin/mongodump')
    parser.add_argument("--backup.mongodump.compression", dest="backup.mongodump.compression", help="Compression method to use on backup (default: gzip)", default="gzip", choices=["none","gzip"])
    return parser
