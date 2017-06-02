import os
import platform
import signal
from ConfigParser import ConfigParser
import pymysql
from pymysql import OperationalError

from proxysql_tools import log


def kill_process(pid):
    os.kill(pid, signal.SIGKILL)


def get_pid(path):
    parser_my_cnf = ConfigParser(allow_no_value=True)
    parser_my_cnf.read(path)
    pid_file = parser_my_cnf.get('mysqld', 'pid-file')
    with open(pid_file, 'r') as f:
        pid = f.readline()
        return int(pid)


def get_my_cnf():
    dist_name = platform.linux_distribution()[0]
    if dist_name.upper() in ["DEBIAN", "UBUNTU"]:
        return '/etc/mysql/my.cnf'
    elif dist_name.upper() in ["RHEL", "CENTOS", "FEDORA"]:
        return '/etc/my.cnf'
    else:
        raise NotImplementedError


def bug1258464(default_file):
    try:
        db = pymysql.connect(read_default_file=default_file)
        with db.cursor() as cursor:
            cursor.execute("SELECT COUNT(*)"
                           "FROM information_schema.processlist "
                           "WHERE State = 'wsrep in pre-commit stage' "
                           "AND Info = 'COMMIT'")
            count_pre_commit = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*)"
                           "FROM information_schema.processlist "
                           "WHERE State = "
                           "'Waiting for table metadata lock' "
                           "AND Info LIKE 'ALTER TABLE%'")
            count_waiting = cursor.fetchone()[0]
            if count_pre_commit > 100 and count_waiting > 0:
                    my_cnf_path = get_my_cnf()
                    pid = get_pid(my_cnf_path)
                    kill_process(pid)
                    return True
    except OperationalError as err:
        log.error(str(err))
    except OSError as err:
        log.error(str(err))
    except NotImplementedError as err:
        log.error(str(err))
    return False
