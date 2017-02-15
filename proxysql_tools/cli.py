# -*- coding: utf-8 -*-
import os
from ConfigParser import ConfigParser

import click

from proxysql_tools import setup_logging, log, __version__
from proxysql_tools.aws import notify_master

pass_cfg = click.make_pass_decorator(ConfigParser, ensure=True)


@click.group(invoke_without_command=True)
@click.option('--debug', help='Print debug messages', is_flag=True,
              default=False)
@click.option('--config', help='ProxySQL Tools config file.',
              default='/etc/twindb/proxysql-tools.cfg',
              show_default=True)
@click.option('--version', help='Show tool version and exit.', is_flag=True,
              default=False)
@pass_cfg
@click.pass_context
def main(ctx, cfg, debug, config, version):
    if not ctx.invoked_subcommand:
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

    pass


@main.command()
@pass_cfg
def check(cfg):
    """Check health of local ProxySQL"""
    log.debug('ProxySQL is alright')


@main.command()
@pass_cfg
def aws_notify_master(cfg):
    """notify_master script for keepalived"""
    log.debug('I am a master now')
    notify_master(cfg)
