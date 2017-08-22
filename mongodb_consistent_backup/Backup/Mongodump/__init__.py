from Mongodump import Mongodump  # NOQA


def config(parser):
    parser.add_argument("--backup.mongodump.binary", dest="backup.mongodump.binary",
                        help="Path to 'mongodump' binary (default: /usr/bin/mongodump)", default='/usr/bin/mongodump')
    parser.add_argument("--backup.mongodump.compression", dest="backup.mongodump.compression",
                        help="Compression method to use on backup (default: auto)", default="auto",
                        choices=["auto", "none", "gzip"])
    parser.add_argument("--backup.mongodump.threads", dest="backup.mongodump.threads",
                        help="Number of threads to use for each mongodump process. There is 1 x mongodump per shard, be careful! (default: shards/CPUs)",
                        default=0, type=int)
    return parser
