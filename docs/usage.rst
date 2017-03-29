=====
Usage
=====

ProxySQL Tool is a command line tool. Check help message for syntax and options:

::

    $ proxysql-tool
    Usage: proxysql-tool [OPTIONS] COMMAND [ARGS]...

    Options:
      --debug        Print debug messages
      --config TEXT  ProxySQL Tools configuration file.  [default: /etc/twindb
                     /proxysql-tools.cfg]
      --version      Show tool version and exit.
      --help         Show this message and exit.

    Commands:
      aws     Commands to interact with ProxySQL on AWS.
      galera  Commands for ProxySQL and Galera integration.
      ping    Checks the health of ProxySQL.

Configuration file
~~~~~~~~~~~~~~~~~~
By default ``proxysql-tool`` looks for a config in ``/etc/twindb/proxysql-tools.cfg``.

Example:

::

    [proxysql]
    host=172.25.3.100
    admin_port=6032
    admin_username=admin
    admin_password=admin

    monitor_username=monitor
    monitor_password=monitor

    [galera]
    cluster_host=172.25.3.10
    cluster_port=3306
    cluster_username=root
    cluster_password=r00t

    load_balancing_mode=singlewriter

    writer_hostgroup_id=10
    reader_hostgroup_id=11
