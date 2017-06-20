"""proxysql_tools module"""
import logging

__author__ = """TwinDB Development Team"""
__email__ = 'dev@twindb.com'
__version__ = '0.3.1'


LOG = logging.getLogger(__name__)


def setup_logging(logger, debug=False):     # pragma: no cover
    """Configure logging"""

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


def execute(conn, query, *args):
    """Execute query in connection"""
    cursor = conn.cursor()
    cursor.execute(query, *args)
    return cursor.fetchall()
