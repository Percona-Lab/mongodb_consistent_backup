from S3 import S3  # NOQA


def config(parser):
    parser.add_argument("--upload.s3.region", dest="upload.s3.region", default="us-east-1", type=str,
                        help="S3 Uploader AWS region to connect to (default: us-east-1)")
    parser.add_argument("--upload.s3.access_key", dest="upload.s3.access_key", type=str,
                        help="S3 Uploader AWS Access Key (required for S3 upload)")
    parser.add_argument("--upload.s3.secret_key", dest="upload.s3.secret_key", type=str,
                        help="S3 Uploader AWS Secret Key (required for S3 upload)")
    parser.add_argument("--upload.s3.bucket_name", dest="upload.s3.bucket_name", type=str,
                        help="S3 Uploader destination bucket name")
    parser.add_argument("--upload.s3.bucket_prefix", dest="upload.s3.bucket_prefix", type=str,
                        help="S3 Uploader destination bucket path prefix")
    parser.add_argument("--upload.s3.bucket_explicit_key", dest="upload.s3.bucket_explicit_key", type=str,
                        help="S3 Uploader explicit storage key within the S3 bucket")
    parser.add_argument("--upload.s3.chunk_size_mb", dest="upload.s3.chunk_size_mb", default=50, type=int,
                        help="S3 Uploader upload chunk size, in megabytes (default: 50)")
    parser.add_argument("--upload.s3.secure", dest="upload.s3.secure", default=True, action="store_false",
                        help="S3 Uploader connect over SSL (default: true)")
    parser.add_argument("--upload.s3.acl", dest="upload.s3.acl", default=None, type=str,
                        help="S3 Uploader ACL associated with objects (default: none)")
    return parser
