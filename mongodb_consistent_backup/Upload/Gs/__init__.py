from Gs import Gs
from GsUploadThread import GsUploadThread


def config(parser):
    parser.add_argument("--upload.gs.project_id", dest="upload.gs.project_id", help="Google Cloud Storage Uploader Project ID (required for GS upload)", type=str)
    parser.add_argument("--upload.gs.access_key", dest="upload.gs.access_key", help="Google Cloud Storage Uploader Access Key (required for GS upload)", type=str)
    parser.add_argument("--upload.gs.secret_key", dest="upload.gs.secret_key", help="Google Cloud Storage Uploader Secret Key (required for GS upload)", type=str)
    parser.add_argument("--upload.gs.bucket_name", dest="upload.gs.bucket_name", help="Google Cloud Storage Uploader destination bucket name", type=str)
    parser.add_argument("--upload.gs.bucket_prefix", dest="upload.gs.bucket_prefix", help="Google Cloud Storage Uploader destination bucket path prefix", type=str)
    parser.add_argument("--upload.gs.threads", dest="upload.gs.threads", help="Google Cloud Storage Uploader worker threads (default: 4)", default=4, type=int)
    return parser
