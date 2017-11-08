from contextlib import contextmanager

import boto3
from subprocess import check_call, CalledProcessError

import pymysql
from pymysql import MySQLError
from pymysql.cursors import DictCursor

from proxysql_tools import LOG


def domainname(name):
    """Extracts domain name from a fqdn
    :param name: FQDN like www.google.com or www.yahoo.com.
    :type name: str
    :return: domain name like google.com  or yahoo.com.
    :rtype: str
    """
    result = name.split('.')
    result.pop(0)
    return '.'.join(result)


def start_proxy(proxy):
    """Start ProxySQL on a remote server proxy"""
    cmd = [
        'sudo',
        'ssh',
        '-t',
        proxy,
        'sudo /etc/init.d/proxysql start'
    ]
    LOG.info('Executing: %s', ' '.join(cmd))
    check_call(cmd)


def stop_proxy(proxy):
    """Stop ProxySQL on a remote server proxy"""
    cmd = [
        'sudo',
        'ssh',
        '-t',
        proxy,
        'sudo killall -9 proxysql'
    ]
    LOG.info('Executing: %s', ' '.join(cmd))
    check_call(cmd)


def restart_proxy(proxy):
    """Restart ProxySQL on server proxy"""
    LOG.info('Restarting %s', proxy)
    stop_proxy(proxy)
    start_proxy(proxy)


def change_names_to(names, ip_addr):
    client = boto3.client('route53')
    if names:
        for name in names:
            LOG.info('Updating A record of %s to %s', name, ip_addr)

            print(name)
            response = client.list_hosted_zones_by_name(
                DNSName=domainname(name),
            )
            zone_id = response['HostedZones'][0]['Id']
            request = {
                'HostedZoneId': zone_id,
                'ChangeBatch': {
                    'Comment': 'Automated switchover DNS update',
                    'Changes': [
                        {
                            'Action': 'UPSERT',
                            'ResourceRecordSet': {
                                'Name': name,
                                'Type': 'A',
                                'TTL': 300,
                                'ResourceRecords': [
                                    {
                                        'Value': ip_addr
                                    },
                                ]
                            }
                        }
                    ]
                }
            }
            client.change_resource_record_sets(**request)


def log_remaining_sessions(host, user='root', password='', port=3306):
    """Connect to host and print existing sessions.
    :return: Number of connected sessions
    :rtype: int
    """
    with _connect(host, user=user, password=password, port=port) as conn:
        cursor = conn.cursor()
        query = "SHOW PROCESSLIST"
        cursor.execute(query)
        nrows = cursor.rowcount
        while True:
            row = cursor.fetchone()
            if row:
                print(row)
            else:
                break

    return nrows


def server_ready(host, user='root', password='', port=3306):
    """Connect to host and execute SELECT 1 to make sure
    it's up and running.
    :return: True if server is ready for connections
    :rtype: bool
    """
    try:
        with _connect(host, user=user, password=password, port=port) as conn:
            cursor = conn.cursor()
            query = "SELECT 1"
            cursor.execute(query)
            return True
    except MySQLError as err:
        LOG.error(err)
        return False


def eth1_present(proxy):
    """Check if eth1 is up on remote host proxy"""
    cmd = [
        'sudo',
        'ssh',
        '-t',
        proxy,
        '/sbin/ifconfig eth1'
    ]
    LOG.info('Executing: %s', ' '.join(cmd))
    try:
        check_call(cmd)
        return True
    except CalledProcessError:
        return False


@contextmanager
def _connect(host, user='root', password='', port=3306):
    """Connect to ProxySQL admin interface."""
    pass
    connect_args = {
        'host': host,
        'port': port,
        'user': user,
        'passwd': password,
        'connect_timeout': 60,
        'cursorclass': DictCursor
    }

    conn = pymysql.connect(**connect_args)
    yield conn
    conn.close()
