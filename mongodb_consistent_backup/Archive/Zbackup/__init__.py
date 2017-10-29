from Zbackup import Zbackup  # NOQA


def config(parser):
    parser.add_argument("--archive.zbackup.binary", dest="archive.zbackup.binary", default='/usr/bin/zbackup', type=str,
                        help="Path to ZBackup binary (default: /usr/bin/zbackup)")
    parser.add_argument("--archive.zbackup.cache_mb", dest="archive.zbackup.cache_mb", default=128, type=int,
                        help="Megabytes of RAM to use as a cache for ZBackup (default: 128)")
    parser.add_argument("--archive.zbackup.compression", dest="archive.zbackup.compression", default='lzma', choices=['lzma'], type=str,
                        help="Type of compression to use with ZBackup (default: lzma)")
    parser.add_argument("--archive.zbackup.password_file", dest="archive.zbackup.password_file", default=None, type=str,
                        help="Path to ZBackup backup password file, enables AES encryption (default: none)")
    parser.add_argument("--archive.zbackup.threads", dest="archive.zbackup.threads", default=0, type=int,
                        help="Number of threads to use for ZBackup (default: 1-per-CPU)")
    return parser
