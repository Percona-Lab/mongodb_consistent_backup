# To install to a different prefix use "make PREFIX=/your/path install, default = /usr/local"
# 

NAME=mongodb_consistent_backup
VERSION=$(shell cat VERSION | cut -d- -f1)
PREFIX?=/usr/local
BASEDIR?=$(DESTDIR)$(PREFIX)
BINDIR?=$(BASEDIR)/bin
SHAREDIR?=$(BASEDIR)/share


all: bin/mongodb-consistent-backup

bin/mongodb-consistent-backup: setup.py requirements.txt README.rst VERSION scripts/build.sh $(NAME)/*.py $(NAME)/*/*.py $(NAME)/*/*/*.py
	PYTHON_BIN=$(PYTHON_BIN) VIRTUALENV_BIN=$(VIRTUALENV_BIN) bash scripts/build.sh

install: bin/mongodb-consistent-backup
	rm -rf bin build 2>/dev/null
	mkdir -p $(BINDIR) $(SHAREDIR)/$(NAME) || true
	install -m 0755 bin/mongodb-consistent-backup $(BINDIR)/mongodb-consistent-backup
	install -m 0644 conf/example.yml $(SHAREDIR)/$(NAME)/example.yml
	install -m 0644 LICENSE $(SHAREDIR)/$(NAME)/LICENSE
	install -m 0644 README.rst $(SHAREDIR)/$(NAME)/README.rst

uninstall:
	rm -f $(BINDIR)/mongodb-consistent-backup
	rm -rf $(SHAREDIR)/$(NAME)

rpm: clean
	mkdir -p rpmbuild/{SPECS,SOURCES/$(NAME)}
	cp -dpR $(NAME) conf Makefile setup.py scripts requirements.txt LICENSE README.rst VERSION rpmbuild/SOURCES/$(NAME)
	install scripts/$(NAME).spec rpmbuild/SPECS/$(NAME).spec
	tar --remove-files -C rpmbuild/SOURCES -czf rpmbuild/SOURCES/$(NAME).tar.gz $(NAME)
	rpmbuild -D "_topdir $(PWD)/rpmbuild" -D "version $(VERSION)" -bb rpmbuild/SPECS/$(NAME).spec

docker:
	docker build --no-cache -t mongodb_consistent_backup .

clean:
	rm -rf bin build rpmbuild $(NAME).egg-info tmp 2>/dev/null
