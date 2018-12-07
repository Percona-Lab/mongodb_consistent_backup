MongoDB Consistent Backup Tool - mongodb-consistent-backup
----------------------------------------------------------

.. image:: https://github-release-version.herokuapp.com/github/Percona-Lab/mongodb_consistent_backup/release.svg?style=flat
    :target: https://github.com/Percona-Lab/mongodb_consistent_backup/releases/latest

.. image:: https://travis-ci.org/Percona-Lab/mongodb_consistent_backup.svg?branch=master
    :target: https://travis-ci.org/Percona-Lab/mongodb_consistent_backup

About
~~~~~

Creates cluster-consistent point-in-time backups of MongoDB with optional
archiving, compression/de-duplication, encryption and upload functionality

The motivation for this tool in explained in this Percona blog post *(more posts coming soon)*:
`"MongoDB Consistent Backups" <https://www.percona.com/blog/2016/07/25/mongodb-consistent-backups/>`__

Features
~~~~~~~~

-  Works on a single replset (2+ members) or a sharded cluster
-  Auto-discovers healthy members for backup by considering replication
   lag, replication 'priority' and by preferring 'hidden' members
-  Creates cluster-consistent backups across many separate shards
-  `'mongodump' <https://docs.mongodb.com/manual/reference/program/mongodump/>`__ is the default *(and currently only)* backup method. Other methods coming soon!
-  Transparent restore process (*just add --oplogReplay flag to your
   mongorestore command*)
-  Archiving and compression of backups (*optional*)
-  Block de-duplication and optional AES encryption at rest via `ZBackup <http://zbackup.org/>`__
   archiving method (*optional*)
-  `AWS S3 <https://aws.amazon.com/s3/>`__ Secure Multipart backup uploads (*optional*)
-  `Google Cloud Storage <https://cloud.google.com/storage/>`__ Secure backup uploads (*optional*)
-  Rsync (over SSH) secure backup uploads (*optional*)
-  `Nagios NSCA <https://sourceforge.net/p/nagios/nsca>`__ push
   notification support (*optional*)
- `Zabbix <https://www.zabbix.com/>`__ sender notification support (*optional*)
-  Modular backup, archiving, upload and notification components
-  Support for `MongoDB Authentication <https://docs.mongodb.com/manual/core/authentication>`__ and `SSL database connections <https://docs.mongodb.com/manual/core/security-transport-encryption/>`__
-  Support for `Read Preference Tags <https://docs.mongodb.com/manual/core/read-preference/#tag-sets>`__ for selecting specific nodes for backup
-  `mongodb+srv:// DNS Seedlist <https://docs.mongodb.com/manual/reference/connection-string/#dns-seedlist-connection-format>`__ support
-  Rotation of backups by time or count
-  Multi-threaded, single executable
-  Auto-scales to number of available CPUs by default

Limitations
~~~~~~~~~~~~~~~~~~~

-  `MongoDB Replication <https://docs.mongodb.com/manual/replication>`__ is required on all nodes *(sharding config servers included)*
-  The host running 'mongodb-consistent-backup' must have enough disk,
   network and cpu resources to backup all shards in parallel
-  When MongoDB authentication is used, the same user/password/authdb
   and role(s) must exist on all hosts

Requirements:
~~~~~~~~~~~~~

-  MongoDB / Percona Server for MongoDB 3.2 and above with `Replication <https://docs.mongodb.com/manual/replication>`__ enabled
-  Backup consistency depends on consistent server time across all
   hosts! Server time **must be synchronized on all nodes** using ntpd
   and a consistent time source or virtualization guest agent that 
   syncs time
-  Must have `'mongodump' <https://docs.mongodb.com/manual/reference/program/mongodump/>`__ installed and specified if not at default:
   */usr/bin/mongodump*. Even if you do not run MongoDB 3.2+, it is
   strongly recommended to use MongoDB 3.2+ mongodump binaries due
   to inline compression and parallelism features
-  Must have Python 2.7 installed

Releases
~~~~~~~~

Pre-built release binaries and packages are available on our `GitHub Releases Page <https://github.com/Percona-Lab/mongodb_consistent_backup/releases>`__. We recommend most users deploy mongodb_consistent_backup using these packages.

Build/Install
~~~~~~~~~~~~~

To build on CentOS/RedHat, you will need the following packages installed:

::

    $ yum install python python-devel python-virtualenv gcc git make libffi-devel openssl-devel

To build an CentOS/RedHat RPM of the tool *(recommended)*:

::

    $ cd /path/to/mongodb_consistent_backup
    $ yum install -y rpm-build
    $ make rpm

To build and install from source *(to default '/usr/local/bin/mongodb-consistent-backup')*:

::

    $ cd /path/to/mongodb_consistent_backup
    $ make
    $ make install

Use the PREFIX= variable to change the installation path (*default: /usr/local*), ie: ``make PREFIX=/usr install`` to install to: '*/usr/bin/mongodb-consistent-backup*'.

MongoDB Authorization
~~~~~~~~~~~~~~~~~~~~~

If your replset/cluster uses `Authentication <https://docs.mongodb.com/manual/core/authentication>`__, you must add a user with the `"backup" <https://docs.mongodb.com/manual/reference/built-in-roles/#backup>`__ and `"clusterMonitor" <https://docs.mongodb.com/manual/reference/built-in-roles/#clusterMonitor>`__ built-in auth roles.

To create a user, execute the following **replace the 'pwd' field with a secure password!**:

::

    db.getSiblingDB("admin").createUser({
            user: "mongodb_consistent_backup",
            pwd: "PASSWORD-HERE",
            roles: [
                    { role: "backup", db: "admin" },
                    { role: "clusterMonitor", db: "admin" }
            ]
    })

User and password are set using the 'user' and 'password' config-file fields or via the '-u' and '-p' command-line flags **not recommended due to security concerns**

Run a Backup
~~~~~~~~~~~~

**Using Command-Line Flags**

*Note: username+password is visible in process lists when set using the command-line flags. Use a config file (below) to hide credentials!*

::

    $ mongodb-consistent-backup -H mongos1.example.com -P 27018 -u mongodb-consistent-backup -p s3cr3t -n prodwebsite -l /var/lib/mongodb-consistent-backup
    ...
    ...
    $ ls /opt/mongobackups
    prodwebsite

**Using a Config File**

The tool supports a YAML-based config file for settings. The config file is loaded first and any additional command-line arguments override the file based config settings.

::

    $ mongodb-consistent-backup --config /etc/mongodb-consistent-backup.yml
    ...

An example *(with comments)* of the YAML-based config file is here: `conf/mongodb-consistent-backup.example.conf <conf/mongodb-consistent-backup.example.conf>`__.

A description of all available config settings can also be listed by passing the '--help' flag to the tool.

Restore a Backup
~~~~~~~~~~~~~~~~

The backups are `mongorestore <https://docs.mongodb.com/manual/reference/program/mongorestore/>`__ compatible and stored in a directory per backup. The *--oplogReplay* flag **MUST** be present to replay the oplogs to ensure consistency.

::

    $ tar xfvz <shardname>.tar.gz
    ...
    $ mongorestore --host mongod12.example.com --port 27017 -u admin -p 123456 --oplogReplay --dir /var/lib/mongodb-consistent-backup/default/20170424_0000/rs0/dump

Run as Docker Container
~~~~~~~~~~~~~~~~~~~~~~~

To persist logs, configs and backup data 3 directories should be mapped to be inside the Docker container.

The 'docker run' command -v/--volume flags in the examples below map container paths to paths on your Docker host. The example below assumes there is a path on the Docker host named *'/data/mongobackup'* with *'data'*, *'conf'* and *'logs'* subdirectories mapped to inside the container. Replace any instance of *'/data/mongobackup'* below to a different path if necessary.

*Note: store a copy of your mongodb-consistent-backup.conf in the 'conf' directory and pass it's container path as the --config= flag if you wish to use config files.*

**Via Docker Hub**

::

    $ mkdir -p /data/mongobackup/{conf,data,logs}
    $ cp -f /path/to/mongodb-consistent-backup.conf /data/mongobackup/conf
    $ docker run -it \
        -v "/data/mongobackup/conf:/conf:Z" \
        -v "/data/mongobackup/data:/var/lib/mongodb-consistent-backup:Z" \
        -v "/data/mongobackup/logs:/var/log/mongodb-consistent-backup:Z" \
      perconalab/mongodb_consistent_backup:latest --config=/conf/mongodb-consistent-backup.conf

**Build and Run Docker Image**

::

    $ cd /path/to/mongodb_consistent_backup
    $ make docker
    $ mkdir -p /data/mongobackup/{conf,data,logs}
    $ cp -f /path/to/mongodb-consistent-backup.conf /data/mongobackup/conf
    $ docker run -it \
        -v "/data/mongobackup/conf:/conf:Z" \
        -v "/data/mongobackup/data:/var/lib/mongodb-consistent-backup:Z" \
        -v "/data/mongobackup/logs:/var/log/mongodb-consistent-backup:Z" \
      mongodb_consistent_backup --config=/conf/mongodb-consistent-backup.conf

ZBackup Archiving (Optional)
~~~~~~~

*Note: the ZBackup archive method is not yet compatible with the 'Upload' phase. Disable uploading by setting 'upload.method' to 'none' in the meantime.*

`ZBackup <http://zbackup.org/>`__ *(with LZMA compression)* is an optional archive method for mongodb_consistent_backup. This archive method significantly reduces disk usage for backups via de-duplication and compression. 

ZBackup offers block de-duplication and compression of backups and optionally supports AES-128 *(CBC mode with PKCS#7 padding)* encryption at rest. The ZBackup archive method causes backups to be stored via ZBackup at archive time.

To enable, ZBackup must be installed on your system and the 'archive.method' config file variable *(or --archive.method flag=)* must be set to 'zbackup'.

ZBackup's compression is most efficient when compression is disabled in the backup phase, to do this set 'backup.<method>.compression' to 'none'.

**Install on CentOS/RHEL**

::

    $ yum install zbackup

**Install on Debian/Ubuntu**

::

    $ apt-get install zbackup


**Get Backup from ZBackup**

ZBackup data is stored in a storage directory named *'mongodb_consistent_backup-zbackup'* and must be restored using a 'zbackup restore ...' command.

::

    $ zbackup restore --password-file /etc/zbackup.passwd /mnt/backup/default/mongodb_consistent_backup-zbackup/backups/20170424_0000.tar | tar -xf

**Delete Backup from ZBackup**

To remove a backup, first delete the .tar file in 'backups' subdir of the ZBackup storage directory. After, run a 'zbackup gc full' garbage collection to remove unused data.

::

    $ rm -f /mnt/backup/default/mongodb_consistent_backup-zbackup/backups/20170424_0000.tar
    $ zbackup gc full --password-file /etc/zbackup.passwd /mnt/backup/default/mongodb_consistent_backup-zbackup 
    
Submitting Code
~~~~~~~~~~~~~~~

-  Submitted code must pass Python `'flake8' <https://pypi.python.org/pypi/flake8>`__ checks. Run *'make flake8'* to test.
-  To make review easier, pull requests must address and solve one problem at a time.

Links
~~~~~

- https://www.percona.com/blog/2016/07/25/mongodb-consistent-backups/
- https://www.percona.com/blog/2017/01/09/mongodb-pit-backups-part-2/
- https://www.percona.com/blog/2017/05/10/percona-lab-mongodb_consistent_backup-1-0-release-explained/
- https://hub.docker.com/r/perconalab/mongodb_consistent_backup/
- https://docs.mongodb.com/manual/reference/program/mongodump/
- https://docs.mongodb.com/manual/reference/program/mongorestore/
- http://zbackup.org

Contact
~~~~~~~

`Contact Percona <mailto:mongodb-backup@percona.com>`__
