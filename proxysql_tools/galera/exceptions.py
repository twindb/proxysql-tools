"""Errors related to Galera"""


class GaleraClusterError(Exception):
    """Base Galera Cluster Error"""


class GaleraClusterNodeNotFound(GaleraClusterError):
    """Requested node not found"""


class GaleraClusterSyncedNodeNotFound(GaleraClusterNodeNotFound):
    """Cluster doesn't have a SYNCED node"""
