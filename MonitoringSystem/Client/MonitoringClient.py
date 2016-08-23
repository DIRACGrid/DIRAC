########################################################################
# $Id: $
########################################################################
"""

"""
__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient      import RPCClient
from DIRAC.MonitoringSystem.private.FileCoding import codeRequestInFileId
import types 

class MonitoringClient( object ):
  """ This class expose the methods of the ReportGenerator Service"""

  def __init__( self, rpcClient = None ):
    self.rpcClient = rpcClient

  def __getServer( self, timeout = 3600 ):
    """It returns the access protocol to the ReportGenerator service"""
    if self.rpcClient:
      return self.rpcClient
    else:
      return RPCClient( 'Monitoring/ReportGenerator', timeout = timeout )

  #############################################################################
  def listUniqueKeyValues( self, typeName ):
    """It print the string"""
    server = self.__getServer()
    return server.listUniqueKeyValues( typeName )
  
  #############################################################################
  def listReports( self, typeName ):
    """
    :param str typeName monitoring type for example WMSHistory
      return the list of available reports
    """
    server = self.__getServer()
    return server.listReports( typeName )
  
  def generateDelayedPlot( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = None, compress = True ):
    if type( extraArgs ) != types.DictType:
      extraArgs = {}
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'condDict' : condDict,
                    'grouping' : grouping,
                    'extraArgs' : extraArgs }
    return codeRequestInFileId( plotRequest, compress )