# To install to a different prefix use "make PREFIX=/your/path install, default = /usr/local"
# 

NAME=mongodb_consistent_backup
BIN_NAME?=mongodb-consistent-backup
VERSION=$(shell cat VERSION | cut -d- -f1)
RELEASE?=1
GIT_COMMIT?=$(shell git show 2>/dev/null | awk 'NR==1{print $$2}')
PREFIX?=/usr/local
ARCH?=x86_64
BASEDIR?=$(DESTDIR)$(PREFIX)
BINDIR?=$(BASEDIR)/bin
SHAREDIR?=$(BASEDIR)/share
DOCKER_TAG?="$(NAME):$(VERSION)"
DOCKER_BASE_IMAGE?=$(shell awk '/FROM/{print $$2}' Dockerfile)
MAKE_DIR=$(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))

all: bin/$(BIN_NAME)

bin/$(BIN_NAME): setup.py requirements.txt README.rst VERSION scripts/build.sh $(NAME)/*.py $(NAME)/*/*.py $(NAME)/*/*/*.py
	BIN_NAME=$(BIN_NAME) RELEASE=$(RELEASE) GIT_COMMIT=$(GIT_COMMIT) PYTHON_BIN=$(PYTHON_BIN) VIRTUALENV_BIN=$(VIRTUALENV_BIN) bash scripts/build.sh

install: bin/$(BIN_NAME)
	mkdir -p $(BINDIR) $(SHAREDIR)/$(NAME) || true
	install -m 0755 bin/$(BIN_NAME) $(BINDIR)/mongodb-consistent-backup
	install -m 0644 conf/mongodb-consistent-backup.example.conf $(SHAREDIR)/$(NAME)/example.conf
	install -m 0644 LICENSE $(SHAREDIR)/$(NAME)/LICENSE
	install -m 0644 README.rst $(SHAREDIR)/$(NAME)/README.rst

flake8:
	# Ignore space-aligned = and : for now, use 160 for max-line-length
	flake8 --count --max-line-length=160 --show-source --ignore E221,E241 $(PWD)/$(NAME)

rpm: bin/$(BIN_NAME)
	mkdir -p $(MAKE_DIR)/build/rpm/SOURCES
	cp -f $(MAKE_DIR)/{LICENSE,README.rst} build/rpm/SOURCES
	cp -f $(MAKE_DIR)/bin/$(BIN_NAME) build/rpm/SOURCES/mongodb-consistent-backup
	cp -f $(MAKE_DIR)/conf/mongodb-consistent-backup.example.conf build/rpm/SOURCES/mongodb-consistent-backup.conf
	rpmbuild -D "_topdir $(MAKE_DIR)/build/rpm" -D "version $(VERSION)" -D "release $(RELEASE)" -bb $(MAKE_DIR)/scripts/$(NAME).spec

uninstall:
	rm -f $(BINDIR)/mongodb-consistent-backup
	rm -rf $(SHAREDIR)/$(NAME)

# Build CentOS7 RPM (in Docker)
build/rpm/RPMS/$(ARCH)/$(NAME)-$(VERSION)-$(RELEASE).el7.$(ARCH).rpm:
	mkdir -p $(MAKE_DIR)/build/rpm/RPMS/$(ARCH)
	docker run --rm \
		-v "$(MAKE_DIR)/bin:/src/bin:Z" \
		-v "$(MAKE_DIR)/conf:/src/conf:Z" \
		-v "$(MAKE_DIR)/mongodb_consistent_backup:/src/mongodb_consistent_backup:Z" \
		-v "$(MAKE_DIR)/scripts:/src/scripts:Z" \
		-v "$(MAKE_DIR)/tmp/pip:/src/tmp/pip:Z" \
		-v "$(MAKE_DIR)/setup.py:/src/setup.py:Z" \
		-v "$(MAKE_DIR)/requirements.txt:/src/requirements.txt:Z" \
		-v "$(MAKE_DIR)/Makefile:/src/Makefile:Z" \
		-v "$(MAKE_DIR)/README.rst:/src/README.rst:Z" \
		-v "$(MAKE_DIR)/LICENSE:/src/LICENSE:Z" \
		-v "$(MAKE_DIR)/VERSION:/src/VERSION:Z" \
		-v "$(MAKE_DIR)/build/rpm/RPMS/$(ARCH):/src/build/rpm/RPMS/$(ARCH):Z" \
		-i centos:centos7 \
		/bin/bash -c "yum install -y python-devel python-virtualenv gcc make libffi-devel openssl-devel rpm-build && \
			make -C /src RELEASE=$(RELEASE) GIT_COMMIT=$(GIT_COMMIT) BIN_NAME=mongodb-consistent-backup.el7.$(ARCH) rpm && \
			/src/bin/mongodb-consistent-backup.el7.$(ARCH) --version"

centos7: build/rpm/RPMS/$(ARCH)/$(NAME)-$(VERSION)-1.el7.$(ARCH).rpm

# Build Debian8 Binary (in Docker - .deb package soon!)
bin/mongodb-consistent-backup.debian8.$(ARCH):
	docker run --rm \
		-v "$(MAKE_DIR)/bin:/src/bin:Z" \
		-v "$(MAKE_DIR)/conf:/src/conf:Z" \
		-v "$(MAKE_DIR)/mongodb_consistent_backup:/src/mongodb_consistent_backup:Z" \
		-v "$(MAKE_DIR)/scripts:/src/scripts:Z" \
		-v "$(MAKE_DIR)/tmp/pip:/src/tmp/pip:Z" \
		-v "$(MAKE_DIR)/setup.py:/src/setup.py:Z" \
		-v "$(MAKE_DIR)/requirements.txt:/src/requirements.txt:Z" \
		-v "$(MAKE_DIR)/Makefile:/src/Makefile:Z" \
		-v "$(MAKE_DIR)/README.rst:/src/README.rst:Z" \
		-v "$(MAKE_DIR)/LICENSE:/src/LICENSE:Z" \
		-v "$(MAKE_DIR)/VERSION:/src/VERSION:Z" \
		-i debian:jessie \
		/bin/bash -c "apt-get update && apt-get install -y python2.7-minimal python2.7-dev python-virtualenv gcc make libffi-dev libssl-dev && \
			make -C /src RELEASE=$(RELEASE) GIT_COMMIT=$(GIT_COMMIT) BIN_NAME=mongodb-consistent-backup.debian8.$(ARCH).tmp && \
			mv -vf /src/bin/mongodb-consistent-backup.debian8.$(ARCH).tmp /src/bin/mongodb-consistent-backup.debian8.$(ARCH) && \
			/src/bin/mongodb-consistent-backup.debian8.$(ARCH) --version"

debian8: bin/mongodb-consistent-backup.debian8.$(ARCH)

# Build Debian9 Binary (in Docker - .deb package soon!)
bin/mongodb-consistent-backup.debian9.$(ARCH):
	docker run --rm \
		-v "$(MAKE_DIR)/bin:/src/bin:Z" \
		-v "$(MAKE_DIR)/conf:/src/conf:Z" \
		-v "$(MAKE_DIR)/mongodb_consistent_backup:/src/mongodb_consistent_backup:Z" \
		-v "$(MAKE_DIR)/scripts:/src/scripts:Z" \
		-v "$(MAKE_DIR)/tmp/pip:/src/tmp/pip:Z" \
		-v "$(MAKE_DIR)/setup.py:/src/setup.py:Z" \
		-v "$(MAKE_DIR)/requirements.txt:/src/requirements.txt:Z" \
		-v "$(MAKE_DIR)/Makefile:/src/Makefile:Z" \
		-v "$(MAKE_DIR)/README.rst:/src/README.rst:Z" \
		-v "$(MAKE_DIR)/LICENSE:/src/LICENSE:Z" \
		-v "$(MAKE_DIR)/VERSION:/src/VERSION:Z" \
		-i debian:stretch \
		/bin/bash -c "apt-get update && apt-get install -y python2.7-minimal python2.7-dev python-virtualenv gcc make libffi-dev libssl-dev && \
			make -C /src RELEASE=$(RELEASE) GIT_COMMIT=$(GIT_COMMIT) BIN_NAME=mongodb-consistent-backup.debian9.$(ARCH).tmp && \
			mv -vf /src/bin/mongodb-consistent-backup.debian9.$(ARCH).tmp /src/bin/mongodb-consistent-backup.debian9.$(ARCH) && \
			/src/bin/mongodb-consistent-backup.debian9.$(ARCH) --version"

debian9: bin/mongodb-consistent-backup.debian9.$(ARCH)

docker: build/rpm/RPMS/$(ARCH)/$(NAME)-$(VERSION)-$(RELEASE).el7.$(ARCH).rpm
	docker build --no-cache --tag $(DOCKER_TAG) .
	docker tag $(DOCKER_TAG) $(NAME):latest
	docker run --rm -i $(DOCKER_TAG) --version

release: centos7 debian8 debian9 docker

clean:
	rm -rf bin build $(NAME).egg-info tmp 2>/dev/null
