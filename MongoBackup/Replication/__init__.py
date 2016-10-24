from Replset import Replset
from ReplsetSharded import ReplsetSharded

def config(parser):
	parser.add_argument("-replication.max_lag_secs", dest="replication.max_lag_secs", help="Maximum replication lag of chosen backup replica(s), in seconds (default: 5)", default=5, type=int)
	parser.add_argument("-replication.use_hidden", dest="replication.use_hidden", help="Use hidden secondary/replica members only for backup (default: false)", default=False, action="store_true")
	return parser
