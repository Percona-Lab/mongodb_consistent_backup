import logging
import socket


def validate_hostname(hostname):
    try:
        if ":" in hostname:
            hostname, port = hostname.split(":")
        socket.gethostbyname(hostname)
    except socket.error, e:
        logging.fatal("Could not resolve host '%s', error: %s" % (hostname, e))
        raise Exception, "Could not resolve host '%s', error: %s" % (hostname, e), None
