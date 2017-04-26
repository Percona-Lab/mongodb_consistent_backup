from Archive import Archive


def config(parser):
    parser.add_argument("--archive.method", dest="archive.method", help="Archiver method (default: tar)", default='tar', choices=['tar','zbackup','none'])
    return parser
