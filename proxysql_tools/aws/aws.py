# pylint: skip-file
import os
from ConfigParser import NoOptionError
from subprocess import CalledProcessError

import time

from proxysql_tools import LOG
from proxysql_tools.aws.notify_master import server_ready, eth1_present, restart_proxy, change_names_to, \
    log_remaining_sessions, stop_proxy, start_proxy


def aws_notify_master(cfg, proxy_a, proxy_b, vip, dns,
                      mysql_user, mysql_password, mysql_port):

    try:
        os.environ['AWS_ACCESS_KEY_ID'] = cfg.get('aws', 'aws_access_key_id')
        os.environ['AWS_SECRET_ACCESS_KEY'] = cfg.get('aws',
                                                      'aws_secret_access_key')
        os.environ['AWS_DEFAULT_REGION'] = cfg.get('aws', 'aws_default_region')
    except NoOptionError:
        LOG.error('aws_access_key_id, aws_secret_access_key and '
                  'aws_default_region must be defined in '
                  'aws section of the config file.')
        exit(-1)

    for host in [proxy_a, proxy_b, vip]:
        if not server_ready(host,
                            user=mysql_user,
                            password=mysql_password,
                            port=mysql_port):
            LOG.error('Server %s must be up and running to do switchover',
                      host)
            exit(1)
    if proxy_a == proxy_b:
        LOG.error('Proxy A and Proxy B cannot be same')
        exit(1)

    if vip in [proxy_a, proxy_b]:
        LOG.error('VIP address cannot be equal to proxy A or proxy B')
        exit(1)

    LOG.info("Switching active ProxySQL from %s to %s", proxy_a, proxy_b)
    LOG.debug('DNS names: %s', ', '.join(dns))

    if not eth1_present(proxy_a):
        LOG.error('It looks like %s is not active proxy', proxy_a)
        exit(1)

    if eth1_present(proxy_b):
        LOG.error('Interface eth1 is expected on %s, but found on %s. '
                  'Exiting', proxy_a, proxy_b)
        exit(1)

    # Step 1. Restart ProxyB
    try:
        restart_proxy(proxy_b)
        pass
    except CalledProcessError as err:
        LOG.error(err)
        exit(1)

    # Step 2, 3. Change DNS so dns points to Proxy B Private IP
    change_names_to(dns, proxy_b)

    # Step 4. Wait TTL * 2 time
    wait_time = 30 * 60
    timeout = time.time() + wait_time
    n_conn = 0
    while time.time() < timeout:
        # Step 5. Check if any MySQL users are connected to Proxy A
        #   (and log if any)
        n_conn = log_remaining_sessions(proxy_a,
                                        mysql_user,
                                        mysql_password,
                                        mysql_port)
        if not n_conn:
            LOG.info('All sessions disconnected from %s', proxy_a)
            break
        LOG.info('There are still %d open sessions on %s', n_conn, proxy_a)
        time.sleep(3)
    if n_conn > 0:
        LOG.warning('Will kill existing sessions anyway')
    # 6. Stop Proxy A
    stop_proxy(proxy_a)

    # 7. Wait until eth1 shows up on Proxy B. If not - log error and stop.
    wait_time = 60
    timeout = time.time() + wait_time
    while time.time() < timeout:
        if eth1_present(proxy_b) and server_ready(vip,
                                                  user=mysql_user,
                                                  password=mysql_password,
                                                  port=mysql_port):
            break
        time.sleep(3)

    if not eth1_present(proxy_b):
        LOG.error('Keepalived failed to move VIP to %s', proxy_b)
        exit(1)

    # 8. Start Proxy A
    start_proxy(proxy_a)

    # 9. Change DNS so dns points to points to VIP
    # 10. Change DNS so dns points to points to VIP
    change_names_to(dns, vip)
    LOG.info('Successfully done.')
