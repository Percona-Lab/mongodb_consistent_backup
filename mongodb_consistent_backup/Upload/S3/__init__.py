from S3 import S3

def config(parser):
    parser.add_argument("--upload.s3.access_key", dest="upload.s3.access_key", help="S3 Uploader AWS Access Key (required for S3 upload)", type=str)
    parser.add_argument("--upload.s3.secret_key", dest="upload.s3.secret_key", help="S3 Uploader AWS Secret Key (required for S3 upload)", type=str)
    parser.add_argument("--upload.s3.bucket_name", dest="upload.s3.bucket_name", help="S3 Uploader destination bucket name", type=str)
    parser.add_argument("--upload.s3.bucket_prefix", dest="upload.s3.bucket_prefix", help="S3 Uploader destination bucket path prefix", type=str)
    parser.add_argument("--upload.s3.threads", dest="upload.s3.threads", help="S3 Uploader upload worker threads (default: 4)", default=4, type=int)
    parser.add_argument("--upload.s3.chunk_size_mb", dest="upload.s3.chunk_size_mb", help="S3 Uploader upload chunk size, in megabytes (default: 50)", default=50, type=int)
    parser.add_argument("--upload.s3.remove_uploaded", dest="upload.s3.remove_uploaded",help="Remove source files after S3 Upload (default: false)", default=False, action="store_true")
    return parser
