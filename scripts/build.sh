#!/bin/bash

set -x

name=mongodb-consistent-backup
mod_name=MongoBackup
py_entry_point=${mod_name}:run
rootdir=$(readlink -f $(dirname $0)/..)
srcdir=${rootdir}/${mod_name}
bindir=${rootdir}/bin
builddir=${rootdir}/build
venvdir=${builddir}/venv
output_file=${bindir}/${name}
require_file=${builddir}/requirements.txt
version_file=${builddir}/VERSION

python_bin=${PYTHON_BIN}
if [ -z "$python_bin" ]; then
	if [[ "`uname`" =~ "Darwin" ]]; then
		python_bin=$(which python)
	else
		python_bin=$(whereis -b python | tr ' ' '\n' | egrep "/python([0-9].[0-9])?$" | head -1)
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
	cp -dpR ${rootdir}/${mod_name} ${builddir}/${mod_name}
	cp -dp ${rootdir}/{setup.py,requirements.txt,VERSION} ${builddir}
	find ${builddir} -type f -name "*.pyc" -delete

	# Replace version number in setup.py and MongoBackup/__init__.py with number in VERSION:
	if [ -f "$version_file" ]; then
		version=$(cat ${version_file})
		if [ -z "$version" ]; then
			echo "Cannot get version from file $version_file!"
			exit 1
		else
			sed -i -e s@\#.\#.\#@${version}@g ${builddir}/setup.py
			sed -i -e s@\#.\#.\#@${version}@g ${builddir}/${mod_name}/__init__.py
			sed -i -e s@\#.\#.\#@${version}@g ${builddir}/${mod_name}/Config.py
		fi
	else
		echo "Cannot find version file $version_file!"
		exit 1
	fi

	git_commit=$(git show 2>/dev/null | awk 'NR==1{print $2}')
	if [ -z "$git_commit" ]; then
		echo "Warning: cannot find git commit hash!"
	else
		sed -i -e s@GIT_COMMIT_HASH@${git_commit}@g ${builddir}/${mod_name}/__init__.py
		sed -i -e s@GIT_COMMIT_HASH@${git_commit}@g ${builddir}/${mod_name}/Config.py
	fi

	${python_bin} ${virtualenv_bin} -p ${python_bin} ${venvdir}
	if [ $? -gt 0 ]; then
		echo "Failed to setup virtualenv for building!"
		exit 1
	fi
	source ${venvdir}/bin/activate
		
	${venvdir}/bin/pip install pex requests
	if [ $? -gt 0 ]; then
		echo "Failed to install pex utility for building!"
		exit 1
	fi

	[ ! -d ${bindir} ] && mkdir -p ${bindir}
	${venvdir}/bin/pex --disable-cache -o ${output_file} -m ${py_entry_point} -r ${require_file} ${builddir}
	if [ $? -lt 1 ] && [ -x ${output_file} ]; then
		echo "pex executable written to '$output_file'"
		rm -rf ${builddir}
	else
		echo "Failed to build project using pex!"
		exit 1
	fi
else
	echo "Failed to find source code at '$srcdir'!"
	exit 1
fi
