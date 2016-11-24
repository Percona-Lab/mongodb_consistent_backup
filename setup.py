#!/usr/bin/env python

from setuptools import setup, find_packages


setup(
    name='mongodb_consistent_backup',
    version='#.#.#',
    license="ASL-2",
    description='Tool for getting consistent backups from MongoDB Clusters and ReplicaSet',
    author='Percona-Lab',
    author_email='tim.vaillancourt@percona.com',
    url='https://github.com/Percona-Lab/mongodb_consistent_backup',
    packages=find_packages(),
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: Database',
        'Topic :: System :: Archiving :: Backup'
    ]
)
