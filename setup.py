#!/usr/bin/env python

from distutils.core import setup

setup(
    name='MongoBackup',
    version='#.#.#',
    description='Percona MongoDB Consistent Backup Tool',
    author='Percona',
    author_email='tim.vaillancourt@percona.com',
    url='https://github.com/percona/MongoToolsAndSnippets/rdba/mongo_backup',
    packages=[
        'MongoBackup',
        'MongoBackup.Common',
        'MongoBackup.Oplog',
        'MongoBackup.Notify',
        'MongoBackup.Upload'
    ])
