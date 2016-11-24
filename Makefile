# To install to a different prefix use "make PREFIX=/your/path install, default = /usr/local"
# 
PREFIX?=/usr/local
VERSION=$(shell cat VERSION)
BASEDIR=$(DESTDIR)$(PREFIX)
BINDIR=$(BASEDIR)/bin
SHAREDIR=$(BASEDIR)/share/mongodb_consistent_backup

all: bin/mongodb-consistent-backup

bin/mongodb-consistent-backup: setup.py requirements.txt VERSION scripts/build.sh mongodb_consistent_backup/*.py mongodb_consistent_backup/Archive/*.py mongodb_consistent_backup/Common/*.py mongodb_consistent_backup/Backup/*.py mongodb_consistent_backup/Notify/*.py mongodb_consistent_backup/Oplog/*.py mongodb_consistent_backup/Replication/*.py mongodb_consistent_backup/Upload/*.py
	PYTHON_BIN=$(PYTHON_BIN) VIRTUALENV_BIN=$(VIRTUALENV_BIN) bash scripts/build.sh

install: bin/mongodb-consistent-backup
	mkdir -p $(BINDIR) $(SHAREDIR) || true
	install -m 0755 bin/mongodb-consistent-backup $(BINDIR)/mongodb-consistent-backup
	install -m 0644 conf/example.yml $(SHAREDIR)/example.yml
	install -m 0644 LICENSE $(SHAREDIR)/LICENSE
	install -m 0644 README.rst $(SHAREDIR)/README.rst

uninstall:
	rm -f $(BINDIR)/mongodb-consistent-backup
	rm -rf $(SHAREDIR)

rpm:
	rm -rf rpmbuild
	mkdir -p rpmbuild/{SPECS,SOURCES/mongodb_consistent_backup}
	cp -dpR mongodb_consistent_backup conf Makefile setup.py scripts requirements.txt LICENSE README.rst VERSION rpmbuild/SOURCES/mongodb_consistent_backup
	install scripts/mongodb_consistent_backup.spec rpmbuild/SPECS/mongodb_consistent_backup.spec
	tar --remove-files -C rpmbuild/SOURCES -czf rpmbuild/SOURCES/mongodb_consistent_backup.tar.gz mongodb_consistent_backup
	rpmbuild -D "_topdir $(PWD)/rpmbuild" -D "version $(VERSION)" -bb rpmbuild/SPECS/mongodb_consistent_backup.spec

clean:
	rm -rf bin build rpmbuild
