"""Entry points for proxysql-tools"""
from __future__ import print_function
import os
from ConfigParser import ConfigParser, NoOptionError, NoSectionError

import click
from pymysql import MySQLError

from proxysql_tools import setup_logging, LOG, __version__, OPTIONS_MAPPING
from proxysql_tools.aws.aws import aws_notify_master
from proxysql_tools.cli_entrypoint.galera import galera_register
from proxysql_tools.galera.server import server_status, \
    server_set_wsrep_desync
from proxysql_tools.galera.user import get_users
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound
from proxysql_tools.proxysql.proxysql import ProxySQL
from proxysql_tools.util.bug1258464 import bug1258464

PASS_CFG = click.make_pass_decorator(ConfigParser, ensure=True)


@click.group(invoke_without_command=True)
@click.option('--debug', help='Print debug messages', is_flag=True,
              default=False)
@click.option('--config', help='ProxySQL Tools configuration file.',
              default='/etc/twindb/proxysql-tools.cfg',
              show_default=True)
@click.option('--version', help='Show tool version and exit.', is_flag=True,
              default=False)
@PASS_CFG
@click.pass_context
def main(ctx, cfg, debug, config, version):
    """proxysql-tool entrypoint"""
    if ctx.invoked_subcommand is None:
        if version:
            print(__version__)
            exit(0)
        else:
            print(ctx.get_help())
            exit(-1)

    setup_logging(LOG, debug=debug)

    if os.path.exists(config):
        cfg.read(config)
    else:
        LOG.error("Config file %s doesn't exist", config)
        exit(1)


@main.command()
@PASS_CFG
def ping(cfg):
    """Checks the health of ProxySQL."""
    kwargs = {}

    for key in OPTIONS_MAPPING:
        try:
            kwargs[key] = cfg.get('proxysql', OPTIONS_MAPPING[key])
        except NoOptionError:
            pass

    if ProxySQL(**kwargs).ping():
        LOG.debug('ProxySQL is alive')
        exit(0)
    else:
        LOG.debug('ProxySQL is dead')
        exit(1)


@main.group()
def aws():
    """Commands to interact with ProxySQL on AWS."""


@aws.command()
@PASS_CFG
def notify_master(cfg):
    """The notify_master script for keepalived."""
    LOG.debug('Switching to master role and executing keepalived '
              'notify_master script.')
    aws_notify_master(cfg)


@main.group()
def galera():
    """Commands for ProxySQL and Galera integration."""


@galera.command()
@PASS_CFG
def register(cfg):
    """Registers Galera cluster nodes with ProxySQL."""

    try:
        galera_register(cfg)
    except NotImplementedError as err:
        LOG.error(err)
        exit(1)
    except (NoOptionError, NoSectionError) as err:
        LOG.error('Failed to parse config: %s', err)
        exit(1)

    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)


@galera.command()
@click.option('--default-file', help='Path to my.cnf with custom settings')
def bug1258464killer(default_file):
    """
    bug1258464killer checks status of a local Galera node
    and if a) There are stuck COMMIT queries and b) There is an ALTER TABLE
    it will kill the node. This command workarounds a known bug
    https://bugs.launchpad.net/percona-xtradb-cluster/+bug/1258464
    """
    if default_file:
        if os.path.isfile(default_file):
            bug1258464(default_file)
        else:
            LOG.error('File not found : %s', default_file)
    else:
        bug1258464('/root/.my.cnf')


@galera.group()
def server():
    """Commands to manipulate MySQL servers."""


@server.command()
@PASS_CFG
def status(cfg):
    """Show status of MySQL backends."""

    try:
        server_status(cfg)
    except NotImplementedError as err:
        LOG.error(err)
        exit(1)
    except (NoOptionError, NoSectionError) as err:
        LOG.error('Failed to parse config: %s', err)
        exit(1)

    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)


@server.command()
@click.argument('ip_address', required=True)
@click.argument('port', required=False, type=int, default=3306)
@PASS_CFG
def set_desync(cfg, ip_address, port):
    """Set server to desync state."""

    try:
        server_set_wsrep_desync(cfg, ip_address, port, wsrep_desync='ON')
    except NotImplementedError as err:
        LOG.error(err)
        exit(1)
    except (NoOptionError, NoSectionError) as err:
        LOG.error('Failed to parse config: %s', err)
        exit(1)

    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)
    except ProxySQLBackendNotFound as err:
        LOG.error(err)


@server.command()
@click.argument('ip_address', required=True)
@click.argument('port', required=False, type=int, default=3306)
@PASS_CFG
def set_sync(cfg, ip_address, port):
    """Set server to sync state."""

    try:
        server_set_wsrep_desync(cfg, ip_address, port, wsrep_desync='OFF')
    except NotImplementedError as err:
        LOG.error(err)
        exit(1)
    except (NoOptionError, NoSectionError) as err:
        LOG.error('Failed to parse config: %s', err)
        exit(1)

    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)
    except ProxySQLBackendNotFound as err:
        LOG.error(err)


@galera.group()
def user():
    """Commands for ProxySQL users"""


@user.command(name='list')
@PASS_CFG
def user_list(cfg):
    """Get user list for MySQL backends."""
    get_users(cfg)
