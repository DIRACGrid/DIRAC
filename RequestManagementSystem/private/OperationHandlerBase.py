########################################################################
# $HeadURL $
# File: OperationHandlerBase.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/03/13 13:48:52
########################################################################
""" :mod: OperationHandlerBase
    ==========================

    .. module: OperationHandlerBase
    :synopsis: request operation handler base class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    RMS Operation handler base class.

    This should be a functor getting Operation as ctor argument and calling it (executing __call__)
    should return S_OK/S_ERROR.

    Helper functions and tools:

    * self.replicaManager() -- returns ReplicaManager
    * self.dataLoggingClient() -- returns DataLoggingClient
    * self.rssClient() -- returns RSSClient
    * self.getProxyForLFN( LFN ) -- sets X509_USER_PROXY environment variable to LFN owner proxy
    * self.rssSEStatus( SE, status ) returns S_OK(True/False) depending of RSS :status:

    Properties:

    * self.shifter -- list of shifters matching request owner (could be empty!!!)
    * each CS option stored under CS path "RequestExecutingAgent/OperationHandlers/Foo" is exported as read-only property too
    * self.initialize() -- overwrite it to perform additional initialization
    * self.log -- own sub logger
    * self.request, self.operation -- reference to Operation and Request itself

    In all inherited class one should overwrite __call__ and initialize, when appropriate.

    For monitoring purpose each of operation handler has got defined at this level three
    :gMonitor: activities to be used together with given operation.Type, namely
    operation.Type + "Att", operation.Type + "Succ" and operation.Type + "Fail", i.e. for
    operation.Type = "Foo", they are "FooAtt", "FooSucc", "FooFail". Treating of those is done
    automatically, but if you need to monitor more, DIY.

"""
__RCSID__ = "$Id $"
# #
# @file OperationHandlerBase.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/03/13 13:49:02
# @brief Definition of OperationHandlerBase class.

# # imports
import os
# # from DIRAC
from DIRAC import gLogger, gConfig, S_ERROR, S_OK
from DIRAC.Core.Utilities.Graph import DynamicProps
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupsWithVOMSAttribute

########################################################################
class OperationHandlerBase( object ):
  """
  .. class:: OperationHandlerBase

  request operation handler base class
  """
  __metaclass__ = DynamicProps
  # # private replica manager
  __replicaManager = None
  # # private data logging client
  __dataLoggingClient = None
  # # private ResourceStatusClient
  __rssClient = None
  # # shifter list
  __shifterList = []

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param Operation operation: Operation instance
    :param str csPath: config path in CS for this operation
    """
    # # placeholders for operation and request
    self.operation = None
    self.request = None

    self.csPath = csPath if csPath else ""
    # # get name
    name = self.__class__.__name__
    # # all options are r/o properties now
    csOptionsDict = gConfig.getOptionsDict( self.csPath )
    csOptionsDict = csOptionsDict["Value"] if "Value" in csOptionsDict else {}

    for option, value in csOptionsDict.iteritems():
      # # hack to set proper types
      try:
        value = eval( value )
      except NameError:
        pass
      self.makeProperty( option, value, True )

    # # pre setup logger
    self.log = gLogger.getSubLogger( name, True )
    # # set log level
    logLevel = getattr( self, "LogLevel" ) if hasattr( self, "LogLevel" ) else "INFO"
    self.log.setLevel( logLevel )

    # # list properties
    for option in csOptionsDict:
      self.log.debug( "%s = %s" % ( option, getattr( self, option ) ) )

    # # setup operation
    if operation:
      self.setOperation( operation )
    # # initialize at least
    if hasattr( self, "initialize" ) and callable( getattr( self, "initialize" ) ):
      getattr( self, "initialize" )()

  def setOperation( self, operation ):
      """ operation and request setter

      :param Operation operation: operation instance
      :raises: TypeError is :operation: in not an instance of Operation
      """
      if not isinstance( operation, Operation ):
        raise TypeError( "expecting Operation instance" )
      self.operation = operation
      self.request = operation._parent
      self.log = gLogger.getSubLogger( "%s/%s/%s" % ( self.request.RequestName,
                                                      self.request.Order,
                                                      self.operation.Type ) )
  @classmethod
  def replicaManager( cls ):
    """ ReplicaManger getter """
    if not cls.__replicaManager:
      from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
      cls.__replicaManager = ReplicaManager()
    return cls.__replicaManager

  @classmethod
  def dataLoggingClient( cls ):
    """ DataLoggingClient getter """
    if not cls.__dataLoggingClient:
      from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
      cls.__dataLoggingClient = DataLoggingClient()
    return cls.__dataLoggingClient

  @classmethod
  def rssClient( cls ):
    """ ResourceStatusClient getter """
    if not cls.__rssClient:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
      cls.__rssClient = ResourceStatus()
    return cls.__rssClient

  def getProxyForLFN( self, lfn ):
    """ get proxy for lfn

    :param str lfn: LFN
    :return: S_ERROR or S_OK( "/path/to/proxy/file" )
    """
    dirMeta = self.replicaManager().getCatalogDirectoryMetadata( lfn, singleFile = True )
    if not dirMeta["OK"]:
      return dirMeta
    dirMeta = dirMeta["Value"]

    ownerRole = "/%s" % dirMeta["OwnerRole"] if not dirMeta["OwnerRole"].startswith( "/" ) else dirMeta["OwnerRole"]
    ownerDN = dirMeta["OwnerDN"]

    ownerProxy = None
    for ownerGroup in getGroupsWithVOMSAttribute( ownerRole ):
      vomsProxy = gProxyManager.downloadVOMSProxy( ownerDN, ownerGroup, limited = True,
                                                   requiredVOMSAttribute = ownerRole )
      if not vomsProxy["OK"]:
        self.log.debug( "getProxyForLFN: failed to get VOMS proxy for %s role=%s: %s" % ( ownerDN,
                                                                                          ownerRole,
                                                                                          vomsProxy["Message"] ) )
        continue
      ownerProxy = vomsProxy["Value"]
      self.log.debug( "getProxyForLFN: got proxy for %s@%s [%s]" % ( ownerDN, ownerGroup, ownerRole ) )
      break

    if not ownerProxy:
      return S_ERROR( "Unable to get owner proxy" )

    dumpToFile = ownerProxy.dumpAllToFile()
    if not dumpToFile["OK"]:
      self.log.error( "getProxyForLFN: error dumping proxy to file: %s" % dumpToFile["Message"] )
      return dumpToFile
    dumpToFile = dumpToFile["Value"]
    os.environ["X509_USER_PROXY"] = dumpToFile
    return dumpToFile

  def getWaitingFilesList( self ):
    """ prepare waiting files list, update Attempt, filter out MaxAttempt """
    if not self.operation:
      self.log.warning( "getWaitingFilesList: operation not set, returning empty list" )
      return []
    waitingFiles = [ opFile for opFile in self.operation if opFile.Status == "Waiting" ]
    for opFile in waitingFiles:
      opFile.Attempt += 1
      maxAttempts = getattr( self, "MaxAttempts" ) if hasattr( self, "MaxAttempts" ) else 256
      if opFile.Attempt > maxAttempts:
        opFile.Status = "Failed"
        opFile.Error = "Max attempts limit reached"
    return [ opFile for opFile in self.operation if opFile.Status == "Waiting" ]

  def rssSEStatus( self, se, status ):
    """ check SE :se: for status :status:

    :param str se: SE name
    :param str status: RSS status
    """
    rssStatus = self.rssClient().getStorageElementStatus( se, status )
    # gLogger.always( rssStatus )
    if not rssStatus["OK"]:
      return S_ERROR( "unknown SE: %s" % se )
    if rssStatus["Value"] == "Banned":
      return S_OK( False )
    return S_OK( True )

  @property
  def shifter( self ):
    return self.__shifterList

  @shifter.setter
  def shifter( self, shifterList ):
    self.__shifterList = shifterList

  def __call__( self ):
    """ this one should be implemented in the inherited class

    should return S_OK/S_ERROR
    """
    raise NotImplementedError( "Implement me please!" )
