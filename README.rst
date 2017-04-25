MongoDB Consistent Backup Tool - mongodb-consistent-backup
----------------------------------------------------------

About
~~~~~

Creates cluster-consistent point-in-time backups of MongoDB via wrapping
'mongodump'. Backups are remotely-pulled and outputted onto the host
running the tool.

Features
~~~~~~~~

-  Works on a single replset (2+ members) or a sharded cluster
-  Auto-discovers healthy members for backup by considering replication
   lag, replication 'priority' and by preferring 'hidden' members.
-  Creates cluster-consistent backups across many separate shards
-  Transparent restore process (*just add --oplogReplay flag to your
   mongorestore command*)
-  Archiving and compression of backups (*optional*)
-  Block de-duplication and optional AES encryption at rest via `ZBackup <http://zbackup.org/>`__
   archiving method (*optional*)
-  AWS S3 Secure/HTTPS Multipart backup uploads (*optional*)
-  `Nagios NSCA <https://sourceforge.net/p/nagios/nsca>`__ push
   notification support (*optional*)
-  Modular backup, archiving, upload and notification components
-  Multi-threaded, single executable

Current Limitations
~~~~~~~~~~~~~~~~~~~

-  The host running 'mongodb-consistent-backup' must have enough disk,
   network and cpu resources to backup all shards in parallel
-  When MongoDB authentication is used, the same user/password/authdb
   and role(s) must exist on all hosts

Requirements:
~~~~~~~~~~~~~

-  Backup consistency depends on consistent server time across all
   hosts. Server time **must be synchronized on all nodes** using ntpd
   and a consistent time source or virtualization guest agent that 
   syncs time
-  Must have 'mongodump' installed and specified if not at default:
   */usr/bin/mongodump*. Even if you do not run MongoDB 3.2+, it is
   strongly recommended to use MongoDB 3.2+ binaries due to inline
   compression, parallelism, etc
-  Must have Python 2.7 installed

Releases
~~~~~~~~

Prebuilt release binaries and packages are available on our `GitHub Releases Page <https://github.com/Percona-Lab/mongodb_consistent_backup/releases>`__. We recommend most users deploy mongodb_consistent_backup using these packages.

Build/Install
~~~~~~~~~~~~~

To build on CentOS/RedHat, you will need the following packages installed:

::

    $ yum install python python-devel python-virtualenv gcc git make libffi-devel openssl-devel

To build an CentOS/RedHat RPM of the tool *(recommended)*:

::

    $ cd /path/to/mongodb_consistent_backup
    $ make rpm

To build and install from source *(to default '/usr/local/bin/mongodb-consistent-backup')*:

::

    $ cd /path/to/mongodb_consistent_backup
    $ make
    $ make install

Use the PREFIX= variable to change the installation path (*default: /usr/local*), ie: ``make PREFIX=/usr install`` to install to: '*/usr/bin/mongodb-consistent-backup*'.

MongoDB Authorization
~~~~~~~~~~~~~~~~~~~~~

If your replset/cluster uses `Authentication <https://docs.mongodb.com/manual/core/authentication>`__, you must add a user with the "backup" and "clusterMonitor" built-in auth roles.

To create a user, execute the following **replace the 'pwd' field with a secure password!**:

::

    db.createUser({
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

The backups are mongorestore compatible and stored in a directory per backup. The *--oplogReplay* flag **MUST** be present to replay the oplogs to ensure consistency.

::

    $ tar xfvz <shardname>.tar.gz
    ...
    $ mongorestore --host mongod12.example.com --port 27017 -u admin -p 123456 --oplogReplay --dir /var/lib/mongodb-consistent-backup/default/20170424_0000/rs0/dump

Run as Docker Container (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~

*Note: you need to use persistent volumes to store backups and/or config files long-term when using Docker. Data in Docker containers is destroyed when the container is deleted.*

**Via Docker Hub**

::

    $ docker run -i timvaillancourt/mongodb_consistent_backup <mongodb_consistent_backup-flags>

**Build and Run Docker Image**

::

    $ cd /path/to/mongodb_consistent_backup
    $ make docker
    $ docker run -t mongodb_consistent_backup <mongodb_consistent_backup-flags>


ZBackup Archiving (Optional)
~~~~~~~

*Note: the ZBackup archive method is not yet compatible with the 'Upload' phase. Disable uploading by setting 'upload.method' to 'none' in the meantime.*

`ZBackup <http://zbackup.org/>`__ *(with LZMA compression)* is an optional archive method for mongodb_consistent_backup. This archive method significantly reduces disk usage for backups via deduplication and compression. 

ZBackup offers block de-duplication and compression of backups and optionally supports AES-128 encryption at rest. The ZBackup archive method causes backups to be stored via ZBackup at archive time.

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
    
Roadmap
~~~~~~~

-  "Distributed Mode" for running backup on remote hosts *(vs. only on one host)*
-  Upload compatibility for ZBackup archive phase *(upload unsupported today)*
-  Backup retention/rotation *(eg: delete old backups)*
-  Support more notification methods *(Prometheus, PagerDuty, etc)*
-  Support more upload methods *(Google Cloud Storage, Rsync, etc)*
-  Support SSL MongoDB connections
-  Python unit tests

Contact
~~~~~~~

-  Tim Vaillancourt - `Github <https://github.com/timvaillancourt>`__ /
   `Email <mailto:tim.vaillancourt@percona.com>`__
-  David Murphy - `Twitter <https://twitter.com/dmurphy_data>`__ /
   `Github <https://github.com/dbmurphy>`__ /
   `Email <mailto:david.murphy@percona.com>`__
-  Percona - `Twitter <https://twitter.com/Percona>`__ / `Contact
   Page <https://www.percona.com/about-percona/contact>`__

