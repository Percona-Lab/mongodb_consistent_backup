# To install to a different prefix use "make PREFIX=/your/path install, default = /usr/local"
# 
PREFIX?=/usr/local
BASEDIR=$(DESTDIR)$(PREFIX)
BINDIR=$(BASEDIR)/bin

all: bin/mongodb-consistent-backup

bin/mongodb-consistent-backup: setup.py requirements.txt VERSION scripts/build.sh MongoBackup/*.py MongoBackup/Common/*.py MongoBackup/Notify/*.py MongoBackup/Oplog/*.py MongoBackup/Upload/*.py
	PYTHON_BIN=$(PYTHON_BIN) VIRTUALENV_BIN=$(VIRTUALENV_BIN) bash scripts/build.sh

install: bin/mongodb-consistent-backup
	mkdir -p $(BINDIR) || true
	install -m 0755 bin/mongodb-consistent-backup $(BINDIR)/mongodb-consistent-backup

uninstall:
	rm -f $(BINDIR)/mongodb-consistent-backup

clean:
	rm -rf ./bin
	rm -rf ./build
