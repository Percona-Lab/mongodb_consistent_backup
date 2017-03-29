from Util import validate_hostname


class MongoAddr:
    def __init__(self, host=None, port=None, replset=None):
        self.host    = host
        self.port    = port
        self.replset = replset

    def str(self):
        if self.host and self.port:
            string = "%s:%i" % (self.host, self.port)
        return string

    def __str__(self):
        return self.str()

class MongoUri:
    def __init__(self, url, default_port=27017, replset=None):
        self.url          = url
        self.default_port = default_port
        self.replset      = replset

        self.addrs    = []
        self.addr_idx = 0

        self.parse()

    def hosts(self):
        if len(self.addrs) > 0:
            hosts = []
            for addr in self.addrs:
                hosts.append(str(addr))
            return ",".join(hosts)

    def str(self):
        string = self.hosts()
        if self.replset:
            string = "%s/%s" % (self.replset, string)
        return string

    def __str__(self):
        return self.str()

    def parse(self):
        if "/" in self.url:
            self.replset, self.url = self.url.split("/")
        for url in self.url.split(","):
            addr = MongoAddr()
            addr.replset = self.replset
            if ":" in url:
                 addr.host, addr.port = url.split(":")
                 addr.port = int(addr.port)
            else:
                addr.host = url
                if not addr.port:
                    addr.port = self.default_port
            validate_hostname(addr.host)
            self.addrs.append(addr)
        return True

    def next(self):
        return self.get(True)

    def get(self, incr_idx=False):
        addr = None
        try:
            addr = self.addrs[self.addr_idx]
            if addr:
                if incr_idx:
                    self.addr_idx += 1
                return addr
        except:
            pass
        return None

    def len(self):
        return len(self.addrs)
