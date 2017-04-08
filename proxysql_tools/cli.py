import os
from ConfigParser import ConfigParser

import click

from schematics.exceptions import ModelValidationError, ModelConversionError

import proxysql_tools.aws
import proxysql_tools.galera
from proxysql_tools import setup_logging, log, __version__
from proxysql_tools.entities.galera import GaleraConfig
from proxysql_tools.entities.proxysql import ProxySQLConfig
from proxysql_tools.managers.proxysql_manager import (
    ProxySQLManager,
    ProxySQLAdminConnectionError
)

pass_cfg = click.make_pass_decorator(ConfigParser, ensure=True)


@click.group(invoke_without_command=True)
@click.option('--debug', help='Print debug messages', is_flag=True,
              default=False)
@click.option('--config', help='ProxySQL Tools configuration file.',
              default='/etc/twindb/proxysql-tools.cfg',
              show_default=True)
@click.option('--version', help='Show tool version and exit.', is_flag=True,
              default=False)
@pass_cfg
@click.pass_context
def main(ctx, cfg, debug, config, version):
    if ctx.invoked_subcommand is None:
        if version:
            print(__version__)
            exit(0)
        else:
            print(ctx.get_help())
            exit(-1)

    setup_logging(log, debug=debug)

    if os.path.exists(config):
        cfg.read(config)
    else:
        log.error("Config file %s doesn't exist", config)
        exit(1)


@main.command()
@pass_cfg
def ping(cfg):
    """Checks the health of ProxySQL."""
    p_cfg = None

    cfg_opts = {item[0]: item[1] for item in cfg.items('proxysql')}
    try:
        p_cfg = ProxySQLConfig(cfg_opts)
        p_cfg.validate()

        log.debug('Performing health check on ProxySQL instance at %s:%s' %
                  (p_cfg.host, p_cfg.admin_port))

        proxysql_man = ProxySQLManager(host=p_cfg.host,
                                       port=p_cfg.admin_port,
                                       user=p_cfg.admin_username,
                                       password=p_cfg.admin_password)
        proxysql_man.ping()
    except ProxySQLAdminConnectionError:
        log.error('ProxySQL ping failed. Unable to connect at %s:%s '
                  'using username %s and password %s' %
                  (p_cfg.host, p_cfg.admin_port,
                   p_cfg.admin_username, "*" * len(p_cfg.admin_password)))
        exit(1)
    except (ModelValidationError, ModelConversionError) as e:
        log.error('ProxySQL configuration options error: %s' % e)
        exit(1)

    log.info('ProxySQL ping on %s:%s successful.' %
             (cfg_opts['host'], cfg_opts['admin_port']))


@main.group()
@pass_cfg
def aws(cfg):
    """Commands to interact with ProxySQL on AWS."""
    pass


@aws.command()
@pass_cfg
def notify_master(cfg):
    """The notify_master script for keepalived."""
    log.debug('Switching to master role and executing keepalived '
              'notify_master script.')
    proxysql_tools.aws.notify_master(cfg)


@main.group()
@pass_cfg
def galera(cfg):
    """Commands for ProxySQL and Galera integration."""
    pass


@galera.command()
@pass_cfg
def register(cfg):
    """Registers Galera cluster nodes with ProxySQL."""
    proxy_cfg = galera_cfg = None

    proxy_options = {item[0]: item[1] for item in cfg.items('proxysql')}
    try:
        proxy_cfg = ProxySQLConfig(proxy_options)
        proxy_cfg.validate()
    except ModelValidationError as e:
        log.error('ProxySQL configuration options error: %s' % e)
        exit(1)

    galera_options = {item[0]: item[1] for item in cfg.items('galera')}
    try:
        galera_cfg = GaleraConfig(galera_options)
        galera_cfg.validate()
    except (ModelValidationError, ModelConversionError) as e:
        log.error('Galera configuration options error: %s' % e)
        exit(1)

    if not proxysql_tools.galera.register_cluster_with_proxysql(proxy_cfg,
                                                                galera_cfg):
        log.error('Registration of Galera cluster nodes failed.')
        exit(1)

    log.info('Registration of Galera cluster nodes successful.')
