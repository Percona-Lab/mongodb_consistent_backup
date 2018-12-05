#!/bin/bash

set -x

readlink_bin=readlink
cp_bin=cp
if [[ "`uname`" =~ "Darwin" ]]; then
	if [[ -x /usr/local/bin/gcp && -x /usr/local/bin/greadlink ]]; then
		readlink_bin=greadlink
		cp_bin=gcp
	else
		echo "To run this on macOS, please install coreutils via homebrew first."
		exit 1
	fi
fi

name=${BIN_NAME:-mongodb-consistent-backup}
mod_name=mongodb_consistent_backup
rootdir=$(${readlink_bin} -f $(dirname $0)/..)
srcdir=${rootdir}/${mod_name}
bindir=${rootdir}/bin
builddir=${rootdir}/build
tmpdir=${rootdir}/tmp
pexdir=${tmpdir}/pex
pipdir=${tmpdir}/pip
venvdir=${builddir}/venv
output_file=${bindir}/${name}
require_file=${builddir}/requirements.txt
version_file=${builddir}/VERSION
git_commit=${GIT_COMMIT:-unknown}

python_bin=${PYTHON_BIN}
if [ -z "$python_bin" ]; then
	if [[ "`uname`" =~ "Darwin" ]]; then
		python_bin=$(which python)
	else
		python_bin=$(whereis -b python | tr ' ' '\n' | egrep "/python(2\.7)?$" | head -1)
	fi
fi
if [ -z "$python_bin" ]; then
	echo "Python not found! Install Python 2.7 or set PYTHON_BIN environment variable to a path to Python 2.7"
	exit 1
elif [ ! -x "$python_bin" ]; then
	echo "Python path: $python_bin does not exist!"
	exit 1
elif [ ! "`${python_bin} --version 2>&1 | awk '{print $2}' | cut -d "." -f1-2`" = "2.7" ]; then
	echo "Unable to use system python due to not being 2.7, please use environment variable PYTHON_BIN to set a custom path to the python interpreter"
	exit 1
fi

virtualenv_bin=${VIRTUALENV_BIN}
if [ -z "$virtualenv_bin" ]; then
	if [[ "`uname`" =~ "Darwin" ]]; then
		virtualenv_bin=$(which virtualenv)
	else
		virtualenv_bin=$(whereis -b virtualenv | tr ' ' '\n' | egrep "/virtualenv$" | head -1)
	fi
fi
if [ -z "$virtualenv_bin" ]; then
	echo "VIRTUALENV_BIN environment variable must be set to a path to 'virtualenv'"
	exit 1
elif [ ! -x "$virtualenv_bin" ]; then
	echo "Virtualenv path: $virtualenv_bin does not exist!"
	exit 1
fi 

if [ -d ${srcdir} ]; then
	[ -e ${builddir} ] && rm -rf ${builddir}
	mkdir -p ${builddir}
	${cp_bin} -dpR ${rootdir}/${mod_name} ${builddir}/${mod_name}
	${cp_bin} -dp ${rootdir}/{setup.py,requirements.txt,README.rst,VERSION} ${builddir}
	find ${builddir} -type f -name "*.pyc" -delete

	# Replace version number in setup.py and mongodb_consistent_backup/__init__.py with number in VERSION:
	if [ -f "$version_file" ]; then
		version=$(cat ${version_file})
		if [ -z "$version" ]; then
			echo "Cannot get version from file $version_file!"
			exit 1
		else
			sed -i -e s@\#.\#.\#@${version}@g ${builddir}/setup.py
			sed -i -e s@\#.\#.\#@${version}@g ${builddir}/${mod_name}/__init__.py
		fi
	else
		echo "Cannot find version file $version_file!"
		exit 1
	fi

	if [ -z "$git_commit" ]; then
		echo "Warning: cannot find git commit hash!"
	else
		sed -i -e s@GIT_COMMIT_HASH@${git_commit}@g ${builddir}/${mod_name}/__init__.py
	fi

	${python_bin} ${virtualenv_bin} -p ${python_bin} ${venvdir}
	if [ $? -gt 0 ]; then
		echo "Failed to setup virtualenv for building!"
		exit 1
	fi
	source ${venvdir}/bin/activate

	[ ! -d ${pipdir} ] && mkdir -p ${pipdir}
	pip_flags="--download-cache=${pipdir}"
	${venvdir}/bin/python2.7 ${venvdir}/bin/pip --help | grep -q '\-\-cache\-dir'
	[ $? = 0 ] && pip_flags="--cache-dir=${pipdir}"
	${venvdir}/bin/python2.7 ${venvdir}/bin/pip install ${pip_flags} "requests"
	if [ $? -gt 0 ]; then
		echo "Failed to install 'requests'!"
		exit 1
	fi

	# build fails on Pex 1.5+
	${venvdir}/bin/python2.7 ${venvdir}/bin/pip install ${pip_flags} "pex<=1.4"
	if [ $? -gt 0 ]; then
		echo "Failed to install pex utility for building!"
		exit 1
	fi

	if [ ! -d ${pexdir} ]; then
		mkdir -p ${pexdir}
	else
		find ${pexdir} -type f -name "${mod_name}-*.whl" -delete
	fi
	[ ! -d ${bindir} ] && mkdir -p ${bindir}
	${venvdir}/bin/python2.7 ${venvdir}/bin/pex -o ${output_file} -m ${mod_name} -r ${require_file} --pex-root=${pexdir} ${builddir}
	if [ $? -lt 1 ] && [ -x ${output_file} ]; then
		echo "pex executable written to '$output_file'"
	else
		echo "Failed to build project using pex!"
		exit 1
	fi
else
	echo "Failed to find source code at '$srcdir'!"
	exit 1
fi
