from Info import OplogInfo
from Resolve import OplogResolve
from Resolver import OplogResolver
from Tail import OplogTail
from Tailer import OplogTailer


def config(parser):
        parser.add_argument("-oplog.resolver.threads", dest="oplog.resolver.threads", help="Number of threads to use during resolver step (default: 1-per-CPU)", default=0, type=int)
        parser.add_argument("-oplog.gzip", dest="oplog.gzip", help="Compress the captured oplog file with gzip (default: false)", action="store_true", default=False)
        return parser
