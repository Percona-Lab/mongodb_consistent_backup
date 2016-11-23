#!/usr/bin/env python

from distutils.core import setup

setup(
    name='mongodb_consistent_backup',
    version='#.#.#',
    description='Tool for getting consistent backups from MongoDB Clusters and ReplicaSet',
    author='Percona-Lab',
    author_email='tim.vaillancourt@percona.com',
    url='https://github.com/Percona-Lab/mongodb_consistent_backup',
    packages=[
        'MongoBackup',
        'MongoBackup.Archive',
        'MongoBackup.Backup',
        'MongoBackup.Common',
        'MongoBackup.Oplog',
        'MongoBackup.Notify',
        'MongoBackup.Replication',
        'MongoBackup.Upload'
    ])
