# To install to a different prefix use "make PREFIX=/your/path install, default = /usr/local"
# 

NAME=mongodb_consistent_backup
VERSION=$(shell cat VERSION | cut -d- -f1)
PREFIX?=/usr/local
BASEDIR?=$(DESTDIR)$(PREFIX)
BINDIR?=$(BASEDIR)/bin
SHAREDIR?=$(BASEDIR)/share
DOCKER_TAG?="$(NAME):$(VERSION)"


all: bin/mongodb-consistent-backup

bin/mongodb-consistent-backup: setup.py requirements.txt README.rst VERSION scripts/build.sh $(NAME)/*.py $(NAME)/*/*.py $(NAME)/*/*/*.py
	PYTHON_BIN=$(PYTHON_BIN) VIRTUALENV_BIN=$(VIRTUALENV_BIN) bash scripts/build.sh

install: bin/mongodb-consistent-backup
	mkdir -p $(BINDIR) $(SHAREDIR)/$(NAME) || true
	install -m 0755 bin/mongodb-consistent-backup $(BINDIR)/mongodb-consistent-backup
	install -m 0644 conf/mongodb-consistent-backup.example.conf $(SHAREDIR)/$(NAME)/example.conf
	install -m 0644 LICENSE $(SHAREDIR)/$(NAME)/LICENSE
	install -m 0644 README.rst $(SHAREDIR)/$(NAME)/README.rst

uninstall:
	rm -f $(BINDIR)/mongodb-consistent-backup
	rm -rf $(SHAREDIR)/$(NAME)

rpm: bin/mongodb-consistent-backup
	rm -rf build/rpm 2>/dev/null || true
	mkdir -p build/rpm/SOURCES
	cp -f $(PWD)/{LICENSE,README.rst} build/rpm/SOURCES
	cp -f $(PWD)/bin/mongodb-consistent-backup build/rpm/SOURCES/mongodb-consistent-backup
	cp -f $(PWD)/conf/mongodb-consistent-backup.example.conf build/rpm/SOURCES/mongodb-consistent-backup.conf
	rpmbuild -D "_topdir $(PWD)/build/rpm" -D "version $(VERSION)" -bb scripts/$(NAME).spec

docker:
	docker build --no-cache --tag $(DOCKER_TAG) --build-arg "RELEASE=$(VERSION)" .

clean:
	rm -rf bin build $(NAME).egg-info tmp 2>/dev/null
