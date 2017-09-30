from Backup import Backup  # NOQA


def config(parser):
    parser.add_argument("-n", "--backup.name", dest="backup.name", default='default', type=str,
                        help="Name of the backup set (default: default)")
    parser.add_argument("-l", "--backup.location", dest="backup.location", default='/var/lib/mongodb-consistent-backup', type=str,
                        help="Base path to store the backup data (default: /var/lib/mongodb-consistent-backup)")
    parser.add_argument("-m", "--backup.method", dest="backup.method", default='mongodump', choices=['mongodump'],
                        help="Method to be used for backup (default: mongodump)")
    return parser
