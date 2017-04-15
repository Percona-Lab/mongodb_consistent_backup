from Zbackup import Zbackup


def config(parser):
    parser.add_argument("--archive.zbackup.binary", dest="archive.zbackup.binary", help="Path to ZBackup binary (default: /usr/bin/zbackup)", default='/usr/bin/zbackup', type=str)
    parser.add_argument("--archive.zbackup.password_file", dest="archive.zbackup.password_file", help="Path to ZBackup backup password file, enables AES encryption (default: none)", default=None, type=str)
    parser.add_argument("--archive.zbackup.threads", dest="archive.zbackup.threads", help="Number of threads to use for ZBackup (default: 1-per-CPU)", default=0, type=int)
    return parser
