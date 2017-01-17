from Tar import Tar


def config(parser):
    parser.add_argument("--archive.compression", dest="archive.compression",
                        help="Archiver compression method (default: gzip)", default='gzip', choices=['gzip', 'none'])
    return parser
