def config(parser):
    parser.add_argument("--archive.zbackup.binary", dest="archive.zbackup.binary", help="Path to ZBackup binary (default: /usr/bin/zbackup)", default='/usr/bin/zbackup', type=str)
    parser.add_argument("--archive.zbackup.dir", dest="archive.zbackup.dir", help="Path to ZBackup backup store (default: <backup-path>/zbackup)", type=str)
    parser.add_argument("--archive.zbackup.password_file", dest="archive.zbackup.password_file", help="Path to ZBackup backup password file (enables AES encryption of backups, default: none)", type=str)
    return parser
