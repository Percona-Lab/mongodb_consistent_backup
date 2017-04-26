from Upload import Upload


def config(parser):
    parser.add_argument("--upload.method", dest="upload.method", help="Uploader method (default: none)", default='none', choices=['s3', 'none'])
    parser.add_argument("--upload.remove_uploaded", dest="upload.remove_uploaded",help="Remove source files after successful upload (default: false)", default=False, action="store_true")
    return parser
