"""Errors related to ProxySQL"""


class ProxySQLError(Exception):
    """Base ProxySQL Error"""


class ProxySQLBackendNotFound(ProxySQLError):
    """Requested backend not found"""
