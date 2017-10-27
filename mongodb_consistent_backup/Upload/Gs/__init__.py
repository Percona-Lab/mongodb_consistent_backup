from Gs import Gs  # NOQA


def config(parser):
    parser.add_argument("--upload.gs.project_id", dest="upload.gs.project_id", type=str,
                        help="Google Cloud Storage Project ID (required for GS upload)")
    parser.add_argument("--upload.gs.access_key", dest="upload.gs.access_key", type=str,
                        help="Google Cloud Storage Interoperability API Access Key (required for GS upload)")
    parser.add_argument("--upload.gs.secret_key", dest="upload.gs.secret_key", type=str,
                        help="Google Cloud Storage Interoperability API Secret Key (required for GS upload)")
    parser.add_argument("--upload.gs.bucket_name", dest="upload.gs.bucket_name", type=str,
                        help="Google Cloud Storage destination bucket name")
    parser.add_argument("--upload.gs.bucket_prefix", dest="upload.gs.bucket_prefix", type=str,
                        help="Google Cloud Storage destination bucket path prefix")
    return parser
