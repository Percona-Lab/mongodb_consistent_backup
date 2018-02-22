import os


def get_upload_files(backup_dir):
    upload_files = []
    for root, dirs, files in os.walk(backup_dir):
        for file in files:
            upload_files.append(os.path.join(root, file))
    return upload_files
