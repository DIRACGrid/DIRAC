########################################################################
# $HeadURL $
# File: RequestAgentBase.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/05/31 07:40:33
########################################################################
""" :mod: RequestAgentBase
    =======================
 
    .. module: RequestAgentBase
    :synopsis: Implementation of base class for DMS agents working with Requests. 
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    Implementation of base class for DMS agents working with Requests. 
"""

__RCSID__ = "$Id $"

##
# @file RequestAgentBase.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/05/31 07:41:05
# @brief Definition of RequestAgentBase class.

## imports 

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB
from DIRAC.RequestManagementSystem.DB.RequestDBMySQL import RequestDBMySQL
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer

AGENT_NAME = None

########################################################################
class RequestAgentBase( object ):
  """
  .. class:: RequestAgentBase

  Helper class for DMS agents dealing with RequestContainers and Requests.
  """
  ## reference to ReplicaManager
  __replicaManager = None
  ## reference to DataLoggingClient
  __dataLoggingClient = None
  ## reference to RequestClient
  __requestClient = None
  ## reference to RequestDbMySQL
  __requestDBMySQL = None
  ## reference to TransferDB itself
  __transferDB = None
  ## reference to StotageFactory
  __storageFactory = None

  ##############################################
  # componets getters
  @classmethod 
  def replicaManager( cls ):
    """ ReplicaManager getter 
    :param cls: class reference
    """
    if not cls.__replicaManager:
      cls.__replicaManager = ReplicaManager()
    return cls.__replicaManager

  @classmethod
  def dataLoggingClient( cls ):
    """ DataLoggingClient getter
    :param cls: class reference
    """
    if not cls.__dataLoggingClient:
      cls.__dataLoggingClient = DataLoggingClient()
    return cls.__dataLoggingClient
  
  @classmethod
  def requestClient( cls ):
    """ RequestClient getter
    :param cls: class reference
    """
    if not cls.__requestClient:
      cls.__requestClient = RequestClient()
    return cls.__requestClient

  @classmethod
  def requestDBMySQL( cls ):
    """ RequestDBMySQL getter
    :param cls: class reference
    """
    if not cls.__requestDBMySQL:
      cls.__requestDBMySQL = RequestDBMySQL()
    return cls.__requestDBMySQL

  @classmethod
  def transferDB( cls ):
    """ TransferDB getter
    :param cls: class reference
    """
    if not cls.__transferDB:
      cls.__transferDB = TransferDB()
    return cls.__transferDB

  @classmethod
  def storageFactory( cls ):
    """ StorageFactory getter
    :param cls: class reference
    """
    if not cls.__storageFactory:
      cls.__storageFactory = StorageFactory()
    return cls.__storageFactory

  @classmethod
  def getRequestDict( cls, requestType ):
    """ retrive Request of type requestType from RequestDB
        
    :param cls: class reference
    :param str requestType: type of request
    :return: S_ERROR on error
    :return: S_OK with request dictionary::
    
       requestDict = { 
         "requestString" : str,
         "requestName" : str,
         "sourceServer" : str,
         "executionOrder" : list,
         "requestObj" : RequestContainer,
         "jobId" : int }
    """
    ## prepare requestDict
    requestDict = { "requestString" : None,
                    "requestName" : None,
                    "sourceServer" : None,
                    "executionOrder" : None,
                    "requestObj" : None,
                    "jobId" : None }
    ## get request out of DB 
    res = cls.requestClient().getRequest( requestType )
    if not res["OK"]:
      gLogger.error( res["Message"] )
      return res
    elif not res["Value"]:
      msg = "Request of type '%s' not found in RequestDB." % requestType 
      gLogger.info( msg ) 
      return S_OK()
    ## store values
    requestDict["requestName"] = res["Value"]["RequestName"]
    requestDict["requestString"] = res["Value"]["RequestString"]
    requestDict["sourceServer"] = res["Value"]["Server"]
    requestDict["requestObj"] = RequestContainer( request = requestDict["requestString"] )
    ## get JobID
    try:
      requestDict["jobId"] = int( res["JobID"] )
    except ValueError, exc:
      gLogger.warn( "Cannot read JobID for request %s, setting it to 0: %s" % ( requestDict["requestName"], 
                                                                                str(exc) ) )
      requestDict["jobId"] = 0
    ## get the execution order
    res = cls.requestClient().getCurrentExecutionOrder( requestDict["requestName"], 
                                                        requestDict["sourceServer"] )
    if not res["OK"]:
      msg = "Can not get the execution order for request %s." % requestDict["requestName"]
      gLogger.error( msg, res["Message"] )
      return res
    requestDict["executionOrder"] = res["Value"]
    ## return requestDict
    return S_OK( requestDict )
    
 
