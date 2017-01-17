from Archive import Archive


def config(parser):
    parser.add_argument("--archive.method", dest="archive.method", help="Archiver method (default: tar)", default='tar', choices=['tar','none'])
    parser.add_argument("--archive.threads", dest="archive.threads", help="Number of threads to use in archive phase (default: 1-per-CPU)", default=0, type=int)
    return parser
