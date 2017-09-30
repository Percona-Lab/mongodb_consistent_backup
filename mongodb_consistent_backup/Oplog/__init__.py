from Oplog import Oplog  # NOQA
from OplogState import OplogState  # NOQA
from Resolver import Resolver  # NOQA
from Tailer import Tailer  # NOQA


def config(parser):
    parser.add_argument("--oplog.compression", dest="oplog.compression", choices=["none", "gzip"], default="none",
                        help="Compression method to use on captured oplog file (default: none)")
    parser.add_argument("--oplog.flush.max_docs", dest="oplog.flush.max_docs", default=100, type=int,
                        help="Maximum number of oplog document writes to trigger a flush of the backup oplog file (default: 100)")
    parser.add_argument("--oplog.flush.max_secs", dest="oplog.flush.max_secs", default=1, type=int,
                        help="Number of seconds to wait to flush the backup oplog file, if 'max_docs' is not reached (default: 1)")
    parser.add_argument("--oplog.resolver.threads", dest="oplog.resolver.threads", default=0, type=int,
                        help="Number of threads to use during resolver step (default: 1-per-CPU)")
    parser.add_argument("--oplog.tailer.enabled", dest="oplog.tailer.enabled", default='true', type=str,
                        help="Enable/disable capturing of cluster-consistent oplogs, required for cluster-wide PITR (default: true)")
    parser.add_argument("--oplog.tailer.status_interval", dest="oplog.tailer.status_interval", default=30, type=int,
                        help="Number of seconds to wait between reporting oplog tailer status (default: 30)")
    return parser
