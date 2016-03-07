"""
This is the client of the Monitoring service based on Elasticsearch.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient                 import RPCClient
from DIRAC.Core.Utilities.Plotting.FileCoding   import codeRequestInFileId
import types 

class MonitoringClient( object ):
  """ 
  .. class:: MonitoringClient
  
  This class expose the methods of the Monitoring Service
  
  param: RPCClient __rpcClient stores the rpc client used to connect to the service
  
  """
  
  def __init__( self, rpcClient = None ):
    self.__rpcClient = rpcClient

  def __getServer( self, timeout = 3600 ):
    """It returns the access protocol to the Monitoring service"""
    if self.__rpcClient:
      return self.__rpcClient
    else:
      return RPCClient( 'Monitoring/Monitoring', timeout = timeout )

  #############################################################################
  def listUniqueKeyValues( self, typeName ):
    """
    :param str typeName is the monitoring type registered in the Types.
    
    :return: S_OK({key:[]}) or S_ERROR()   The key is element of the __keyFields of the BaseType
    """
    server = self.__getServer()
    return server.listUniqueKeyValues( typeName )
  
  #############################################################################
  def listReports( self, typeName ):
    """
    :param str typeName monitoring type for example WMSHistory
    
    :return S_OK([]) or S_ERROR() the list of available plots
    """
    server = self.__getServer()
    return server.listReports( typeName )
  
  def generateDelayedPlot( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = None, compress = True ):
    """
    It is used to encode the plot parameters used to create a certain plot.
    
    :param str typeName the type of the monitoring
    :param int startTime epoch time, start time of the plot
    :param int endTime epoch time, end time of the plot
    :param dict condDict is the conditions used to gnerate the plot: {'Status':['Running'],'grouping': ['Site'] }
    :param str grouping is the grouping of the data for example: 'Site'
    :paran dict extraArgs epoch time which can be last day, last week, last month
    :param bool compress apply compression of the encoded values.
    
    :return S_OK(str) or S_ERROR() it returns the encoded plot parameters
    """
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
  
  def getReport( self, typeName, reportName, startTime, endTime, condDict, grouping, extraArgs = None ):
    """
    It is used to get the raw data used to create a plot.
    :param str typeName the type of the monitoring
    :param str reportName the name of the plotter used to create the plot for example:  NumberOfJobs
    :param int startTime epoch time, start time of the plot
    :param int endTime epoch time, end time of the plot
    :param dict condDict is the conditions used to gnerate the plot: {'Status':['Running'],'grouping': ['Site'] }
    :param str grouping is the grouping of the data for example: 'Site'
    :paran dict extraArgs epoch time which can be last day, last week, last month
    :rerturn S_OK or S_ERROR
    """
    rpcClient = self.__getServer()
    if type( extraArgs ) != types.DictType:
      extraArgs = {}
    plotRequest = { 'typeName' : typeName,
                    'reportName' : reportName,
                    'startTime' : startTime,
                    'endTime' : endTime,
                    'condDict' : condDict,
                    'grouping' : grouping,
                    'extraArgs' : extraArgs }
    result = rpcClient.getReport( plotRequest )
    if 'rpcStub' in result:
      del( result[ 'rpcStub' ] )
    return result
