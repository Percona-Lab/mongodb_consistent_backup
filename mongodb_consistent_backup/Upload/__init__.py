from Upload import Upload  # NOQA


def config(parser):
    parser.add_argument("--upload.method", dest="upload.method", help="Uploader method (default: none)", default='none', choices=['gs', 's3', 'none'])
    parser.add_argument("--upload.remove_uploaded", dest="upload.remove_uploaded", help="Remove source files after successful upload (default: false)", default=False, action="store_true")
    parser.add_argument("--upload.retries", dest="upload.retries", help="Number of times to retry upload attempts (default: 5)", default=5, type=int)
    parser.add_argument("--upload.threads", dest="upload.threads", help="Number of threads to use for upload (default: 4)", default=4, type=int)
    return parser
