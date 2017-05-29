import socket

from dateutil import parser

from mongodb_consistent_backup.Errors import OperationError


def config_to_string(config):
    config_vars = ""
    for key in config:
        config_vars += "%s=%s, " % (key, config[key])
    return config_vars[:-1]

def is_datetime(string):
    try:
        parser.parse(string)
        return True
    except:
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
