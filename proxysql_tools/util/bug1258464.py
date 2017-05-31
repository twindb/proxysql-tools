import os
import signal
from ConfigParser import ConfigParser
from proxysql_tools.entities.galera import GaleraNode


def bug1258464(cfg):
    parser = ConfigParser(allow_no_value=True)
    parser.read(cfg)

    parser_my_cnf = ConfigParser(allow_no_value=True)
    parser_my_cnf.read('/root/.my.cnf')

    node = GaleraNode()
    node.username = parser_my_cnf.get('client', 'user')
    node.password = parser_my_cnf.get('client', 'password')
    node.password = parser.get('galera', 'cluster_host')
    node.port = parser.get('galera', 'cluster_port')
    with node.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*)"
                               "FROM information_schema.processlist"
                               "WHERE State = 'wsrep in pre-commit stage' AND"
                               "Info = 'COMMIT'")
                count = cursor.fetchone()[0]
                if count > 100:
                    cursor.execute("SELECT COUNT (*)"
                                   "FROM information_schema.processlist"
                                   "WHERE State = "
                                   "'Waiting for table metadata lock'"
                                   "AND Info LIKE 'ALTER TABLE%;")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        with open('/var/run/mysqld/mysqld.pid', 'r') as f:
                            pid = f.readline()
                            os.kill(int(pid), signal.SIGKILL)
                        return True
    return False
