import socket

from dateutil import parser
from hashlib import md5
from select import select

from mongodb_consistent_backup.Errors import OperationError


def config_to_string(config):
    config_pairs = []
    for key in config:
        config_pairs.append("%s=%s" % (key, config[key]))
    return ", ".join(config_pairs)


def is_datetime(string):
    try:
        parser.parse(string)
        return True
    except Exception:
        return False


def parse_method(method):
    return method.rstrip().lower()


def validate_hostname(hostname):
    try:
        if ":" in hostname:
            hostname, port = hostname.split(":")
        socket.getaddrinfo(hostname, None)
    except socket.error, e:
        raise OperationError("Could not resolve host '%s', error: %s" % (hostname, e))


def wait_popen(process, stderr_callback, stdout_callback):
    try:
        while not process.returncode:
            poll = select([process.stderr.fileno(), process.stdout.fileno()], [], [])
            if len(poll) >= 1:
                for fd in poll[0]:
                    if process.stderr and fd == process.stderr.fileno():
                        stderr_callback(process.stderr.readline().rstrip())
                    if process.stdout and fd == process.stdout.fileno():
                        stdout_callback(process.stdout.readline().rstrip())
            if process.poll() is not None:
                break
        stderr, stdout = process.communicate()
        stderr_callback(stderr.rstrip())
        stdout_callback(stdout.rstrip())
    except Exception, e:
        raise e
    return True


def file_md5hash(file_path, blocksize=65536):
    md5hash = md5()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            md5hash.update(block)
    return md5hash.hexdigest()
