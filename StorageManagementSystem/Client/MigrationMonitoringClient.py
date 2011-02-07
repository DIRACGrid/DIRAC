""" Client for the Migration Monitoring DB that inherits the Migration Monitoring DB catalog plug-in.
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC                                                                        import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient                                                   import RPCClient
from DIRAC.ConfigurationSystem.Client                                             import PathFinder
from DIRAC.Resources.Catalog.MigrationMonitoringCatalogClient                     import   MigrationMonitoringCatalogClient
import types

class MigrationMonitoringClient(MigrationMonitoringCatalogClient):

  def __init__(self):
    MigrationMonitoringCatalogClient.__init__(self)

  def getMigratingReplicas(self,se,status):
    """ Get a list of files in the Migration Monitoring DB
    """
    try:
      gLogger.verbose("MigrationMonitoringClient.getMigratingReplicas: Attempting to get '%s' replicas." % (status))
      client = RPCClient(self.url,timeout=120)
      return client.getMigratingReplicas(se,status)
    except Exception,x:
      errStr = "MigrationMonitoringClient.getMigratingReplicas: Exception while getting replicas from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  def setMigratingReplicaStatus(self,fileIDs,status):
    """ Update the replica statuses in the migration monitoring DB
    """
    try:
      gLogger.verbose("MigrationMonitoringClient.setMigratingReplicaStatus: Attempting to update status of %d replicas to '%s'." % (len(fileIDs),status))
      client = RPCClient(self.url,timeout=120)
      return client.setMigratingReplicaStatus(fileIDs,status)
    except Exception,x:
      errStr = "MigrationMonitoringClient.setMigratingReplicaStatus: Exception while updating replicas statuses."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)
