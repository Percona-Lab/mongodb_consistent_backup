# To install to a different prefix use "make PREFIX=/your/path install, default = /usr/local"
# 
PREFIX?=/usr/local
BASEDIR=$(DESTDIR)$(PREFIX)
BINDIR=$(BASEDIR)/bin
VERSION=$(shell cat VERSION)

all: bin/mongodb-consistent-backup

bin/mongodb-consistent-backup: setup.py requirements.txt VERSION scripts/build.sh MongoBackup/*.py MongoBackup/Common/*.py MongoBackup/Methods/*.py MongoBackup/Notify/*.py MongoBackup/Oplog/*.py MongoBackup/Upload/*.py
	PYTHON_BIN=$(PYTHON_BIN) VIRTUALENV_BIN=$(VIRTUALENV_BIN) bash scripts/build.sh

install: bin/mongodb-consistent-backup
	mkdir -p $(BINDIR) || true
	install -m 0755 bin/mongodb-consistent-backup $(BINDIR)/mongodb-consistent-backup

uninstall:
	rm -f $(BINDIR)/mongodb-consistent-backup

rpm:
	rm -rf rpmbuild
	mkdir -p rpmbuild/{SPECS,SOURCES/mongodb_consistent_backup}
	cp -dpR MongoBackup conf Makefile setup.py scripts requirements.txt LICENSE README.md VERSION rpmbuild/SOURCES/mongodb_consistent_backup
	cp -dp scripts/mongodb_consistent_backup.spec rpmbuild/SPECS/mongodb_consistent_backup.spec
	tar --remove-files -C rpmbuild/SOURCES -czf rpmbuild/SOURCES/mongodb_consistent_backup.tar.gz mongodb_consistent_backup
	rpmbuild -D "_topdir $(PWD)/rpmbuild" -D "version $(VERSION)" -bb rpmbuild/SPECS/mongodb_consistent_backup.spec

docker:
	docker build -t --no-cache mongodb_consistent_backup .

clean:
	rm -rf bin build rpmbuild
