import os
import re
import logging


def get_upload_files(backup_dir, regex=None):
    upload_files = []
    r = None
    if regex is not None:
        logging.info("Only uploading files from %s that match the specified regex" % backup_dir)
        r = re.compile(regex)
    else:
        logging.debug("No regex specified, uploading all files in %s" % backup_dir)

    for root, dirs, files in os.walk(backup_dir):
        for f in files:
            if r is not None:
                if r.search(f):
                    logging.info("Adding %s to files to upload, as it matches the regex" % f)
                    upload_files.append(os.path.join(root, f))
                else:
                    logging.debug("Skipping %s because it does not match the regex" % f)
            else:
                upload_files.append(os.path.join(root, f))
    return upload_files
