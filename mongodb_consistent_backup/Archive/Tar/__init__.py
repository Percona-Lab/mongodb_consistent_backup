from Tar import Tar


def config(parser):
    parser.add_argument("--archive.tar.compression", dest="archive.tar.compression",
                        help="Archiver compression method (default: gzip)", default='gzip', choices=['gzip', 'none'])
    parser.add_argument("--archive.tar.threads", dest="archive.tar.threads", 
                        help="Number of threads to use in archive phase (default: 1-per-CPU)", default=0, type=int)
    return parser
