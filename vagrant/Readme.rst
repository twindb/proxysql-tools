Percona XtraDB Cluster : Vagrant & pupppet recipes
**************************************************

This Vagrant setup can be used to deploy and test Percona XtraDB Cluster
(Percona Server with Galera -
http://www.percona.com/software/percona-xtradb-cluster) in VirtualBox.

The puppet code illustrates the latest improvement of PXC since version 5.5.31-23.7.5
(http://www.percona.com/doc/percona-xtradb-cluster/release-notes/Percona-XtraDB-Cluster-5.5.31-23.7.5.html):

* bootstrap the cluster
* use extended Xtrabackup features during SST

Puppet recipes can also be used without Vagrant


Requirements
============

You need :

* VirtualBox - https://www.virtualbox.org/
* a working Vagrant environment - http://vagrantup.com/
* an Internet connection for the VM's


How to use it ?
===============

Once you have your requirements ready, it's very easy to deploy your boxes::

   $ git clone https://github.com/twindb/proxysql-tools.git
   $ cd proxysql-tools/vagrant
   $ vagrant up

This will deploy a ProxySQL on node ``proxysql`` and Percona XtraDB Cluster on nodes ``node1``, ``node2``, and ``node3``.

And this is all !


.. note:: Starting MySQL on node2 and node3 takes a lot of time as the sleep delay of the init script has been increased during SST.


Details about the new functionalities
=====================================

bootstrap the cluster
---------------------

Since PXC 5.5.31, it's now possible to bootstrap the cluster (http://www.percona.com/doc/percona-xtradb-cluster/manual/bootstrap.html) using
*bootstrap-pxc* instead of *start* as command for the init script. Read this for more information related on how it had to be done before:
 http://www.mysqlperformanceblog.com/2013/01/29/how-to-start-a-percona-xtradb-cluster/

With this puppet code, we define in *site.pp* the node we want to use to bootstrap the cluster (percona1 in this case)::

   class {
         'percona::cluster':
            bootstrap => True
   }

If you use these puppet recipes in production, I recommend you to set *bootstrap* to **True** only when needed.

SST integration with Xtrabackup 2.1
-----------------------------------

Since PXC 5.5.31 it's alo possible to use the new xbstream options for Xtrabackup.

In the current environment, SST is performed by Xtrabackup in 2 parallel threads and compressed. (It's also possible to crypt it).

To achieve that, some easy additions to my.cnf are needed. So this is what I added in the template (*modules/percona/templates/cluster/my.cnf.erb*)::

   [sst]
   streamfmt=xbstream

   [xtrabackup]
   compress
   compact
   parallel=2
   compress-threads=2
   rebuild-threads=2

But it is also **mandatory** to have *qpress* (http://www.quicklz.com/) installed and the MySQL datadir must be empty (which is currently very boring, see
https://bugs.launchpad.net/percona-xtrabackup/+bug/1193240).

There are 2 puppet classes performing these actions:

* class qpress
* class percona::cluster::xbstream

This is what you should see in mysql's error log if it worked as expected::
OO
   WSREP_SST: [INFO] Streaming with xbstream (20130627 13:05:20.991)
   WSREP_SST: [INFO] xbstream requires manual cleanup of data directory before SST - lp:1193240 (20130627 13:05:20.998)
   ...
   WSREP_SST: [INFO] Proceeding with SST (20130627 13:05:54.335)
   WSREP_SST: [INFO] Removing existing ib_logfile files (20130627 13:05:54.340)
   WSREP_SST: [INFO] Index compaction detected (20130627 13:05:54.348)
   WSREP_SST: [INFO] Rebuilding with 2 threads (20130627 13:05:54.356)
   WSREP_SST: [INFO] Compressed qpress files found (20130627 13:05:54.364)
   WSREP_SST: [INFO] Removing existing ibdata1 file (20130627 13:05:54.370)
   WSREP_SST: [INFO] Decompression with 1 threads (20130627 13:05:54.375)


How to setup the environment
============================

These are the step to run to be able to setup everything you need.

VirtualBox
----------

On RedHat/CentOS/Fedora...

::

   # yum install virtualbox

On Ubuntu/Debian

::

   # apt-get install virtualbox


Vagrant
-------

Install Vagrant from https://www.vagrantup.com/

Acknowledgements
----------------

This vagrant config is based on https://github.com/lefred/percona-cluster .
Thank you LeFred_ :)

.. _LeFred: https://github.com/lefred
