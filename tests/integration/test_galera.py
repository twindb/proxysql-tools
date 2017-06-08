#import pymysql
#from pymysql.cursors import DictCursor
#from proxysql_tools.proxysql.proxysql import (
#    ProxySQLMySQLBackend,
#)
#
#
#def test__register_cluster_with_proxysql_is_idempotent(
#        percona_xtradb_cluster_node, proxysql_instance):
#    hostgroup_writer = 10
#    hostgroup_reader = 11
#
#
#    mysql_backend_writer = ProxySQLMySQLBackend(hostname=percona_xtradb_cluster_node.host,
#                                         port=percona_xtradb_cluster_node.port,
#                                         hostgroup_id=hostgroup_writer
#    )
#    proxysql_instance.register_backend(mysql_backend_writer)
#
#    mysql_backend_reader = ProxySQLMySQLBackend(hostname=percona_xtradb_cluster_node.host,
#                                         port=percona_xtradb_cluster_node.port,
#                                         hostgroup_id=hostgroup_reader
#    )
#    proxysql_instance.register_backend(mysql_backend_reader)
#
#
#    connection = pymysql.connect(host=proxysql_instance.host, port=proxysql_instance.port,
#                           user=proxysql_instance.user, passwd=proxysql_instance.password,
#                           connect_timeout=20,
#                           cursorclass=DictCursor)
#
#    try:
#        with connection.cursor() as cursor:
#            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
#                           '`port`, `status`, `weight`, `compression`, '
#                           '`max_connections`, `max_replication_lag`, '
#                           '`use_ssl`, `max_latency_ms`, `comment`'
#                           ' FROM `mysql_servers`'
#                           ' WHERE hostgroup_id = %s', hostgroup_writer)
#            row = cursor.fetchall()[0]
#            backend_writer = ProxySQLMySQLBackend(row['hostname'],
#                                           hostgroup_id=row['hostgroup_id'],
#                                           port=row['port'],
#                                           status=row['status'],
#                                           weight=row['weight'],
#                                           compression=row['compression'],
#                                           max_connections=
#                                           row['max_connections'],
#                                           max_replication_lag=
#                                           row['max_replication_lag'],
#                                           use_ssl=row['use_ssl'],
#                                           max_latency_ms=
#                                           row['max_latency_ms'],
#                                           comment=row['comment'])
#            cursor.execute('SELECT `hostgroup_id`, `hostname`, '
#                           '`port`, `status`, `weight`, `compression`, '
#                           '`max_connections`, `max_replication_lag`, '
#                           '`use_ssl`, `max_latency_ms`, `comment`'
#                           ' FROM `mysql_servers`'
#                           ' WHERE hostgroup_id = %s', hostgroup_reader)
#            row = cursor.fetchall()[0]
#            backend_reader = ProxySQLMySQLBackend(row['hostname'],
#                                           hostgroup_id=row['hostgroup_id'],
#                                           port=row['port'],
#                                           status=row['status'],
#                                           weight=row['weight'],
#                                           compression=row['compression'],
#                                           max_connections=
#                                           row['max_connections'],
#                                           max_replication_lag=
#                                           row['max_replication_lag'],
#                                           use_ssl=row['use_ssl'],
#                                           max_latency_ms=
#                                           row['max_latency_ms'],
#                                           comment=row['comment'])
#    finally:
#        connection.close()
#
#
#    # We try to register again and check that registration is idempotent
#
#    proxysql_instance.register_backend(mysql_backend_writer)
#    proxysql_instance.register_backend(mysql_backend_reader)
#
#
#    assert proxysql_instance.find_backends(hostgroup_writer)[0].hostname == backend_writer.hostname  # NOQA
#    assert proxysql_instance.find_backends(hostgroup_reader)[0].hostname == backend_reader.hostname  # NOQA
