""" Client for the Migration Monitoring DB that inherits the Migration Monitoring DB catalog plug-in.
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC                                                               import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient                                          import RPCClient
from DIRAC.ConfigurationSystem.Client                                    import PathFinder
from DIRAC.DataManagementSystem.Client.Catalog.MigrationMonitoringCatalogClient import MigrationMonitoringCatalogClient
import types

class MigrationMonitoringClient(MigrationMonitoringCatalogClient):

  def __init__(self):
    MigrationMonitoringCatalogClient.__init__(self)

  def getFiles(self,se,status):
    """ Get a list of files in the Migration Monitoring DB
    """
    try:
      gLogger.verbose("MigrationMonitoringClient.getFiles: Attempting to get '%s' files at %s." % (status,se))
      client = RPCClient(self.url,timeout=120)
      return client.getFiles(se,status)
    except Exception,x:
      errStr = "MigrationMonitoringClient.getFiles: Exception while getting files from database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

  def setFilesStatus(self,fileIDs,status):
    """ Update the file statuses in the migration monitoring DB
    """
    try:
      gLogger.verbose("MigrationMonitoringClient.setFilesStatus: Attempting to update status of %d files to '%s'." % (len(fileIDs),status))
      client = RPCClient(self.url,timeout=120)
      return client.setFilesStatus(fileIDs,status)
    except Exception,x:
      errStr = "MigrationMonitoringClient.setFilesStatus: Exception while updating file statuses."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)
