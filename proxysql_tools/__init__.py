import logging

__author__ = """TwinDB Development Team"""
__email__ = 'dev@twindb.com'
__version__ = '0.2.13'


log = logging.getLogger(__name__)


def setup_logging(logger, debug=False):     # pragma: no cover

    fmt_str = "%(asctime)s: %(levelname)s:" \
              " %(module)s.%(funcName)s():%(lineno)d: %(message)s"

    console_handler = logging.StreamHandler()

    console_handler.setFormatter(logging.Formatter(fmt_str))
    logger.handlers = []
    logger.addHandler(console_handler)
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
