from Rsync import Rsync  # NOQA


def config(parser):
    parser.add_argument("--upload.rsync.path", dest="upload.rsync.path", help="Rsync upload base destination path (default: /)", default='/', type=str)
    parser.add_argument("--upload.rsync.user", dest="upload.rsync.user", help="Rsync upload SSH username (default: current)", default=None, type=str)
    parser.add_argument("--upload.rsync.host", dest="upload.rsync.host", help="Rsync upload SSH hostname/IP", default=None, type=str)
    parser.add_argument("--upload.rsync.port", dest="upload.rsync.port", help="Rsync upload SSH port number (default: 22)", default=22, type=int)
    parser.add_argument("--upload.rsync.ssh_key", dest="upload.rsync.ssh_key", help="Rsync upload SSH key path", default=None, type=str)
    return parser
