from Replset import Replset  # NOQA
from ReplsetSharded import ReplsetSharded  # NOQA


def config(parser):
    parser.add_argument("--replication.max_lag_secs", dest="replication.max_lag_secs", default=10, type=int,
                        help="Max lag of backup replica(s) in seconds (default: 10)")
    parser.add_argument("--replication.min_priority", dest="replication.min_priority", default=0, type=int,
                        help="Min priority of secondary members for backup (default: 0)")
    parser.add_argument("--replication.max_priority", dest="replication.max_priority", default=1000, type=int,
                        help="Max priority of secondary members for backup (default: 1000)")
    parser.add_argument("--replication.hidden_only", dest="replication.hidden_only", default=False, action="store_true",
                        help="Only use hidden secondary members for backup (default: false)")
    parser.add_argument("--replication.read_pref_tags", dest="replication.read_pref_tags", default=None, type=str,
                        help="Only use members that match replication tags in comma-separated key:value format (default: none)")
    parser.add_argument("--replication.preferred_members", dest="replication.preferred_members", default=None, type=str,
                        help="Prefer members with these names; comma-separated URIs in rs/host:port format (default: none)")

    return parser
