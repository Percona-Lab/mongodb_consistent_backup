from Backup import Backup  # NOQA


def config(parser):
    parser.add_argument("-n", "--backup.name", dest="backup.name", help="Name of the backup set (default: default)", default='default', type=str)
    parser.add_argument("-l", "--backup.location", dest="backup.location", help="Base path to store the backup data (default: /var/lib/mongodb-consistent-backup)", default='/var/lib/mongodb-consistent-backup', type=str)
    parser.add_argument("-m", "--backup.method", dest="backup.method", help="Method to be used for backup (default: mongodump)", default='mongodump', choices=['mongodump'])
    return parser
