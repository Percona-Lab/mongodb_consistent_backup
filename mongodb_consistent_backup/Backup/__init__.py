from Backup import Backup


def config(parser):
    parser.add_argument("-n", "--backup.name", dest="backup.name", help="Name of the backup set (required)", type=str)
    parser.add_argument("-l", "--backup.location", dest="backup.location", help="Base path to store the backup data (required)", type=str)
    parser.add_argument("-m", "--backup.method", dest="backup.method", help="Method to be used for backup (default: mongodump)", default='mongodump', choices=['mongodump'])
    return parser
