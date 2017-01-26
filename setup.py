#!/usr/bin/env python

from setuptools import setup, find_packages


__version__ = "#.#.#"

def readme():
    with open("README.rst") as f:
        return f.read()

setup(
    name='mongodb-consistent-backup',
    version=__version__,
    license="ASL-2",
    description='Tool for getting consistent backups from MongoDB Clusters and ReplicaSet',
    long_description=readme(),
    author='Percona-Lab',
    author_email='tim.vaillancourt@percona.com',
    url='https://github.com/Percona-Lab/mongodb_consistent_backup',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: System Administrators' ,
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: System :: Archiving :: Backup',
        'Topic :: System :: Recovery Tools',
        'Topic :: System :: Systems Administration',
        'Topic :: Utilities'
    ]
)
