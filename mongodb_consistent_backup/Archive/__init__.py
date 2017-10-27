from Archive import Archive  # NOQA


def config(parser):
    parser.add_argument("--archive.method", dest="archive.method", default='tar', choices=['tar', 'zbackup', 'none'],
                        help="Archiver method (default: tar)")
    return parser
