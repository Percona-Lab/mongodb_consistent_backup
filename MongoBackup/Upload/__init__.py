from UploadS3 import UploadS3

def config(parser):
	parser.add_argument("-uploader.method", dest="uploader.method", help="Uploader method (default: none)", default='none', choices=['s3','none'])
	parser.add_argument("-uploader.s3.threads", dest="uploader.s3.threads", help="S3 Uploader upload worker threads (default: 4)", default=4, type=int)
	parser.add_argument("-uploader.s3.bucket_name", dest="uploader.s3.bucket_name", help="S3 Uploader destination bucket name", type=str)
	parser.add_argument("-uploader.s3.bucket_prefix", dest="uploader.s3.bucket_prefix", help="S3 Uploader destination bucket path prefix", type=str)
	parser.add_argument("-uploader.s3.access_key", dest="uploader.s3.access_key", help="S3 Uploader AWS Access Key", type=str)
	parser.add_argument("-uploader.s3.secret_key", dest="uploader.s3.secret_key", help="S3 Uploader AWS Secret Key", type=str)
	parser.add_argument("-uploader.s3.chunk_size_mb", dest="uploader.s3.chunk_size_mb", help="S3 Uploader upload chunk size, in megabytes (default: 50)", default=50, type=int)
	return parser
