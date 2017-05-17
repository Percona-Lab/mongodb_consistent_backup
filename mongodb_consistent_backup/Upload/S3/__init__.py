from S3 import S3

def config(parser):
    parser.add_argument("--upload.s3.region", dest="upload.s3.region", help="S3 Uploader AWS region to connect to (default: us-east-1)", default="us-east-1", type=str)
    parser.add_argument("--upload.s3.access_key", dest="upload.s3.access_key", help="S3 Uploader AWS Access Key (required for S3 upload)", type=str)
    parser.add_argument("--upload.s3.secret_key", dest="upload.s3.secret_key", help="S3 Uploader AWS Secret Key (required for S3 upload)", type=str)
    parser.add_argument("--upload.s3.bucket_name", dest="upload.s3.bucket_name", help="S3 Uploader destination bucket name", type=str)
    parser.add_argument("--upload.s3.bucket_prefix", dest="upload.s3.bucket_prefix", help="S3 Uploader destination bucket path prefix", type=str)
    parser.add_argument("--upload.s3.threads", dest="upload.s3.threads", help="S3 Uploader upload worker threads (default: 4)", default=4, type=int)
    parser.add_argument("--upload.s3.chunk_size_mb", dest="upload.s3.chunk_size_mb", help="S3 Uploader upload chunk size, in megabytes (default: 50)", default=50, type=int)
    parser.add_argument("--upload.s3.secure", dest="upload.s3.secure", help="S3 Uploader connect over SSL (default: true)", default=True, action="store_false")
    parser.add_argument("--upload.s3.retries", dest="upload.s3.retries", help="S3 Uploader retry times (default: 5)", default=5, type=int)
    parser.add_argument("--upload.s3.acl", dest="upload.s3.acl", help="S3 Uploader ACL associated with objects (default: none)", default=None, type=str)
    return parser
