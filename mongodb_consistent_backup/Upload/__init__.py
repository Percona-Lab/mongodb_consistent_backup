from Upload import Upload  # NOQA


def config(parser):
    parser.add_argument("--upload.method", dest="upload.method", default='none', choices=['gs', 'rsync', 's3', 'none'],
                        help="Uploader method (default: none)")
    parser.add_argument("--upload.remove_uploaded", dest="upload.remove_uploaded", default=False, action="store_true",
                        help="Remove source files after successful upload (default: false)")
    parser.add_argument("--upload.retries", dest="upload.retries", default=5, type=int,
                        help="Number of times to retry upload attempts (default: 5)")
    parser.add_argument("--upload.threads", dest="upload.threads", default=4, type=int,
                        help="Number of threads to use for upload (default: 4)")
    return parser
