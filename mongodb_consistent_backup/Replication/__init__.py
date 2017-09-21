from Replset import Replset  # NOQA
from ReplsetSharded import ReplsetSharded  # NOQA


def config(parser):
    parser.add_argument("--replication.max_lag_secs", dest="replication.max_lag_secs", help="Max lag of backup replica(s) in seconds (default: 10)", default=10, type=int)
    parser.add_argument("--replication.min_priority", dest="replication.min_priority", help="Min priority of secondary members for backup (default: 0)", default=0, type=int)
    parser.add_argument("--replication.max_priority", dest="replication.max_priority", help="Max priority of secondary members for backup (default: 1000)", default=1000, type=int)
    parser.add_argument("--replication.hidden_only", dest="replication.hidden_only", help="Only use hidden secondary members for backup (default: false)", default=False, action="store_true")
    parser.add_argument("--replication.read_pref_tags", dest="replication.read_pref_tags", default=None, type=str,
                        help="Only use members that match replication tags in comma-separated key:value format (default: none)")
    return parser
