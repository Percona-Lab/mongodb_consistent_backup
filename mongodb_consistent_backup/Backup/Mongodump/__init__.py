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
    # for a detailled explanation see
    # see https://jira.mongodb.org/browse/TOOLS-845?focusedCommentId=988298&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-988298
    # and https://jira.mongodb.org/browse/TOOLS-845?focusedCommentId=988414&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-988414
    parser.add_argument("--backup.mongodump.force_table_scan", dest="backup.mongodump.force_table_scan", default=False,
                        action="store_true", help="Enables mongodump's forceTableScan option. Useful if custom ids are "
                                                  "used or id values are large. ONLY use with WiredTiger! (default: false)")

    return parser
