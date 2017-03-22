import socket


def config_to_string(config):
    config_vars = ""
    for key in config:
        config_vars += "%s=%s, " % (key, config[key])
    return config_vars[:-1]

def parse_method(method):
    return method.rstrip().lower()

def validate_hostname(hostname):
    try:
        if ":" in hostname:
            hostname, port = hostname.split(":")
        socket.gethostbyname(hostname)
    except socket.error, e:
        raise Exception, "Could not resolve host '%s', error: %s" % (hostname, e), None
