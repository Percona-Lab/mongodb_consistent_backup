from Tar import Tar  # NOQA


def config(parser):
    parser.add_argument("--archive.tar.binary", dest="archive.tar.binary", default='tar', type=str,
                        help="Path to tar binary (default: tar)")
    parser.add_argument("--archive.tar.compression", dest="archive.tar.compression",
                        help="Tar archiver compression method (default: gzip)", default='gzip', choices=['gzip', 'none'])
    parser.add_argument("--archive.tar.threads", dest="archive.tar.threads",
                        help="Number of Tar archiver threads to use (default: 1-per-CPU)", default=0, type=int)
    return parser
