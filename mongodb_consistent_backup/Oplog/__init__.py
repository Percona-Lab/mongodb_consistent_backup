from File import OplogFile
from Resolver import Resolver
from Tailer import Tailer


def config(parser):
    parser.add_argument("--oplog.resolver.threads", dest="oplog.resolver.threads", help="Number of threads to use during resolver step (default: 1-per-CPU)", default=0, type=int)
    parser.add_argument("--oplog.compression", dest="oplog.compression", help="Compression method to use on captured oplog file (default: none)", choices=["none","gzip"], default="none")
    return parser
