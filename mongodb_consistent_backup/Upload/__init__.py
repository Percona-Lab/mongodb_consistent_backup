from Upload import Upload


def config(parser):
    parser.add_argument("--upload.method", dest="upload.method", help="Uploader method (default: none)", default='none', choices=['s3', 'none'])
    return parser
