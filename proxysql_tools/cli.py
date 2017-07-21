"""Entry points for proxysql-tools"""
from __future__ import print_function
import os
from ConfigParser import ConfigParser, NoOptionError, NoSectionError

import click
from pymysql import MySQLError

from proxysql_tools import setup_logging, LOG, __version__
from proxysql_tools.aws.aws import aws_notify_master
from proxysql_tools.cli_entrypoint.galera import galera_register
from proxysql_tools.galera.server import server_status, \
    server_set_wsrep_desync
from proxysql_tools.galera.user import get_users, create_user, delete_user, \
    change_password, modify_user
from proxysql_tools.proxysql.exceptions import ProxySQLBackendNotFound, ProxySQLUserNotFound
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
    kwargs_maps = {
        'host': 'host',
        'port': 'admin_port',
        'user': 'admin_username',
        'password': 'admin_password'
    }
    kwargs = {}
    for key in kwargs_maps:
        try:
            kwargs[key] = cfg.get('proxysql', kwargs_maps[key])
        except NoOptionError:
            pass

    if ProxySQL(**kwargs).ping():
        LOG.info('ProxySQL is alive')
        exit(0)
    else:
        LOG.info('ProxySQL is dead')
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


@user.command()
@click.argument('username', required=True)
@click.option('--password', help='User password',
              type=str, default="")
@click.option('--active/--no-active', default=True,
              help='Is user active', show_default=True)
@click.option('--use_ssl/--no-use_ssl', default=False,
              help='Use SSL for user', show_default=True)
@click.option('--default_hostgroup', default=0,
              help='Default hostgroup for user', show_default=True)
@click.option('--default_schema', default='information_schema',
              help='Default shema for user', show_default=True)
@click.option('--schema_locked/--no-schema_locked', default=False,
              help='Is schema locked', show_default=True)
@click.option('--transaction_persistent/--no-transaction_persistent',
              default=False, help='Is transaction persistent',
              show_default=True)
@click.option('--fast_forward/--no-fast_forward', default=False,
              show_default=True, help='Is fast forward')
@click.option('--backend/--no-backend', default=True,
              show_default=True, help='Is user backend')
@click.option('--frontend/--no-frontend', default=True,
              show_default=True, help='Is user frontend')
@click.option('--max_connections', default=10000,
              help='Max connection for this user',
              show_default=True)
@PASS_CFG
def create(cfg, username, password, active, use_ssl,  # pylint: disable=too-many-arguments
           default_hostgroup, default_schema, schema_locked,
           transaction_persistent, fast_forward,
           backend, frontend, max_connections):
    """Add user of MySQL backend to ProxySQL"""
    kwargs = {
        'username': username,
        'password': password,
        'use_ssl': use_ssl,
        'active': active,
        'default_hostgroup': default_hostgroup,
        'default_schema': default_schema,
        'schema_locked': schema_locked,
        'transaction_persistent': transaction_persistent,
        'backend': backend,
        'frontend': frontend,
        'fast_forward': fast_forward,
        'max_connections': max_connections
    }
    try:
        create_user(cfg, kwargs)
        LOG.info('User %s successfully created', username)
    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)
    except (NoOptionError, NoSectionError) as err:
        LOG.error('Failed to parse config: %s', err)
        exit(1)


def validate_password(ctx, param, value):  # pylint: disable=unused-argument
    """CHeck password value and confirm again if it's empty."""
    if not value:
        password = raw_input("Repeat for confirmation: ")
        if password == '':
            return password
        else:
            raise click.BadParameter('Passwords do not match')
    return value


@user.command()
@click.argument('username')
@click.option('--password', prompt=True, hide_input=True,
              confirmation_prompt=True, default='',
              callback=validate_password)
@PASS_CFG
def set_password(cfg, username, password):
    """Change password of exists MySQL user"""
    try:
        change_password(cfg, username, password)
        LOG.info('Password for user %s changed', username)
    except ProxySQLUserNotFound:
        LOG.error("User not found")
        exit(1)
    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)
    except (NoOptionError, NoSectionError) as err:
        LOG.error('Failed to parse config: %s', err)
        exit(1)
    except ProxySQLBackendNotFound as err:
        LOG.error('ProxySQL backends not found: %s', err)
        exit(1)


@user.command()
@click.argument('username', required=True)
@PASS_CFG
def delete(cfg, username):
    """Delete MySQL backend user by username"""
    try:
        delete_user(cfg, username)
        LOG.info('User %s has deleted', username)
    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)
    except (NoOptionError, NoSectionError) as err:
        LOG.error('Failed to parse config: %s', err)
        exit(1)


@user.command(name='modify', context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('username')
@PASS_CFG
@click.pass_context
def modify(ctx, cfg, username):
    """Modify MySQL backend user by username"""
    try:
        modify_user(cfg, username, ctx.args)
        LOG.info("User %s has modified", username)
    except ProxySQLUserNotFound:
        LOG.error("User not found")
        exit(1)
    except MySQLError as err:
        LOG.error('Failed to talk to database: %s', err)
    except ValueError:
        LOG.error("Invalid input")
        exit(1)
