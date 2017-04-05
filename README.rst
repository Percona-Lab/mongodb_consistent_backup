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
-  Archives and compresses backups (*inline compression with mongodump
   3.2+*)
-  Transparent restore process (*just add --oplogReplay flag to your
   mongorestore command*)
-  AWS S3 Secure/HTTPS Multipart backup uploads (*optional*)
-  `Nagios NSCA <https://sourceforge.net/p/nagios/nsca>`__ push
   notification support (*optional*)
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
   and a consistent time source or virtualization guest agents that 
   can sync time
-  Must have 'mongodump' installed and specified if not at default:
   */usr/bin/mongodump*. Even if you do not run MongoDB 3.2+, it is
   strongly recommended to use MongoDB 3.2+ binaries due to inline
   compression, parallelism, etc
-  Must have Python 2.7 installed

Build/Install
~~~~~~~~~~~~~

To build on CentOS/RedHat, you wil need the following packages (see
command):

::

    yum install python python-devel python-virtualenv gcc make libffi-devel openssl-devel

To install to default '*/usr/local/bin/mongodb-consistent-backup*\ ':

::

    cd path/to/mongo_backup 
    make
    make install

Use the PREFIX= variable to change the installation path (*default:
/usr/local*), ie: ``make PREFIX=/usr install`` to install to:
'*/usr/bin/mongodb-consistent-backup*\ '.

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

::

    $ mongodb-consistent-backup -H mongos1.example.com -P 27018 -u mongodb-consistent-backup -p s3cr3t -n prodwebsite -l /opt/mongobackups
    ...
    ...
    $ ls /opt/mongobackups
    prodwebsite

**Using a Config File**

::
    $ mongodb-consistent-backup --config /etc/mongodb-consistent-backup.yml
    ...

Restore a Backup
~~~~~~~~~~~~~~~~

The backups are mongorestore compatible. The *--oplogReplay* flag **MUST** be present to replay the oplogs to ensure consistency.

::

    $ tar xfvz <shardname>.tar.gz
    ...
    $ mongorestore --host mongod12.example.com --port 27017 -u admin -p 123456 --oplogReplay --dir /path/to/backup/dump

Run as Docker Container (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~

*Note: you need to use persistent volumes to store backups long-term on disk when using Docker. Data in Docker containers is destroyed when the container is deleted.*

**Via Docker Hub**
::

    $ docker run -i timvaillancourt/mongodb_consistent_backup <mongodb_consistent_backup-flags>

**Build and Run Docker Image**
::

    $ cd /path/to/mongodb_consistent_backup
    $ make docker
    $ docker run -t mongodb_consistent_backup <mongodb_consistent_backup-flags>

Roadmap
~~~~~~~

-  "Distributed Mode" for running backup on remote hosts *(vs. only on one host)*
-  Support more notification methods *(Prometheus, PagerDuty, etc)* and upload methods *(Google Cloud Storage, Rsync, etc)*
-  Support SSL MongoDB connections
-  Unit tests

Contact
~~~~~~~

-  Tim Vaillancourt - `Github <https://github.com/timvaillancourt>`__ /
   `Email <mailto:tim.vaillancourt@percona.com>`__
-  David Murphy - `Twitter <https://twitter.com/dmurphy_data>`__ /
   `Github <https://github.com/dbmurphy>`__ /
   `Email <mailto:david.murphy@percona.com>`__
-  Percona - `Twitter <https://twitter.com/Percona>`__ / `Contact
   Page <https://www.percona.com/about-percona/contact>`__

