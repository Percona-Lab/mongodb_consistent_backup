from Oplog import Oplog
from OplogState import OplogState
from Resolver import Resolver
from Tailer import Tailer


def config(parser):
    parser.add_argument("--oplog.compression", dest="oplog.compression", help="Compression method to use on captured oplog file (default: none)", choices=["none","gzip"], default="none")
    parser.add_argument("--oplog.resolver.threads", dest="oplog.resolver.threads", help="Number of threads to use during resolver step (default: 1-per-CPU)", default=0, type=int)
    parser.add_argument("--oplog.tailer.flush_interval", dest="oplog.tailer.flush_interval", help="Number of seconds to wait to flush the oplog if flush_max_writes is not reached (default: 1)", default=1, type=int)
    parser.add_argument("--oplog.tailer.flush_max_writes", dest="oplog.tailer.flush_max_writes", help="Maximum number of oplog writes to trigger a flush of the oplog (default: 1000)", default=1000, type=int)
    parser.add_argument("--oplog.tailer.status_interval", dest="oplog.tailer.status_interval", help="Number of seconds to wait between reporting oplog tailer status  (default: 30)", default=30, type=int)
    return parser
