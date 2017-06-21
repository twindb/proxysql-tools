"""Entry points for proxysql-tools"""
from __future__ import print_function
import os
from ConfigParser import ConfigParser, NoOptionError, NoSectionError

import click
from pymysql import OperationalError

from proxysql_tools import setup_logging, LOG, __version__
from proxysql_tools.aws.aws import aws_notify_master
from proxysql_tools.cli_entrypoint.galera import galera_register
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
    option_mapping = {
        'host': 'host',
        'port': 'admin_port',
        'user': 'admin_username',
        'password': 'admin_password'
    }

    for key in option_mapping:
        try:
            kwargs[key] = cfg.get('proxysql', option_mapping[key])
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

    except OperationalError as err:
        LOG.error('Failed to talk to database: %s', err)


@galera.command()
@PASS_CFG
@click.option('--default-file', help='Path to my.cnf with custom settings')
def bug1258464killer(default_file):
    """
    bug1258464killer checks status of a local Galera node
    and if a) There are stuck COMMIT queries and b) There is an ALTER TABLE
    it will kill the node. This command workarounds a known bug
    https://bugs.launchpad.net/percona-xtradb-cluster/+bug/1258464
    """
    if os.path.isfile(default_file):
        bug1258464(default_file)
    else:
        LOG.error("Config file %s doesn't exist", default_file)
