# $HeadURL$
__RCSID__ = "$Id$"

import threading
import thread
import time
import random
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.PathFinder import getGatewayURLs
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List, LockRing
from DIRAC.Core.Utilities.EventDispatcher import gEventDispatcher
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

def _updateFromRemoteLocation( serviceClient ):
  gLogger.debug( "", "Trying to refresh from %s" % serviceClient.serviceURL )
  localVersion = gConfigurationData.getVersion()
  retVal = serviceClient.getCompressedDataIfNewer( localVersion )
  if retVal[ 'OK' ]:
    dataDict = retVal[ 'Value' ]
    if localVersion < dataDict[ 'newestVersion' ] :
      gLogger.debug( "New version available", "Updating to version %s..." % dataDict[ 'newestVersion' ] )
      gConfigurationData.loadRemoteCFGFromCompressedMem( dataDict[ 'data' ] )
      gLogger.debug( "Updated to version %s" % gConfigurationData.getVersion() )
      gEventDispatcher.triggerEvent( "CSNewVersion", dataDict[ 'newestVersion' ], threaded = True )
    return S_OK()
  return retVal


class Refresher( threading.Thread ):

  def __init__( self ):
    threading.Thread.__init__( self )
    self.__automaticUpdate = False
    self.__lastUpdateTime = 0
    self.__url = False
    self.__refreshEnabled = True
    self.__timeout = 60
    self.__callbacks = { 'newVersion' : [] }
    gEventDispatcher.registerEvent( "CSNewVersion" )
    random.seed()
    self.__triggeredRefreshLock = LockRing.LockRing().getLock()

  def disable( self ):
    self.__refreshEnabled = False

  def enable( self ):
    self.__refreshEnabled = True
    if self.__lastRefreshExpired():
      self.forceRefresh()

  def isEnabled( self ):
    return self.__refreshEnabled

  def addListenerToNewVersionEvent( self, functor ):
    gEventDispatcher.addListener( "CSNewVersion", functor )

  def __refreshInThread( self ):
    retVal = self.__refresh()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while updating the configuration", retVal[ 'Message' ] )

  def __lastRefreshExpired( self ):
    return time.time() - self.__lastUpdateTime >= gConfigurationData.getRefreshTime()

  def refreshConfigurationIfNeeded( self ):
    if not self.__refreshEnabled or self.__automaticUpdate or not gConfigurationData.getServers():
      return
    self.__triggeredRefreshLock.acquire()
    try:
      if not self.__lastRefreshExpired():
        return
      self.__lastUpdateTime = time.time()
    finally:
      try:
        self.__triggeredRefreshLock.release()
      except thread.error:
        pass
    #Launch the refresh
    thd = threading.Thread( target = self.__refreshInThread )
    thd.setDaemon( 1 )
    thd.start()


  def forceRefresh( self ):
    if self.__refreshEnabled:
      return self.__refresh()
    return S_OK()

  def autoRefreshAndPublish( self, sURL ):
    gLogger.debug( "Setting configuration refresh as automatic" )
    if not gConfigurationData.getAutoPublish():
      gLogger.debug( "Slave server won't auto publish itself" )
    if not gConfigurationData.getName():
      import DIRAC
      DIRAC.abort( 10, "Missing configuration name!" )
    self.__url = sURL
    self.__automaticUpdate = True
    self.setDaemon( 1 )
    self.start()

  def run( self ):
    while self.__automaticUpdate:
      iWaitTime = gConfigurationData.getPropagationTime()
      time.sleep( iWaitTime )
      if self.__refreshEnabled:
        if not self.__refreshAndPublish():
          gLogger.error( "Can't refresh configuration from any source" )


  def __refreshAndPublish( self ):
    self.__lastUpdateTime = time.time()
    gLogger.info( "Refreshing from master server" )
    from DIRAC.Core.DISET.RPCClient import RPCClient
    sMasterServer = gConfigurationData.getMasterServer()
    if sMasterServer:
      oClient = RPCClient( sMasterServer, timeout = self.__timeout,
                           useCertificates = gConfigurationData.useServerCertificate(),
                           skipCACheck = gConfigurationData.skipCACheck() )
      dRetVal = _updateFromRemoteLocation( oClient )
      if not dRetVal[ 'OK' ]:
        gLogger.error( "Can't update from master server", dRetVal[ 'Message' ] )
        return False
      if gConfigurationData.getAutoPublish():
        gLogger.info( "Publishing to master server..." )
        dRetVal = oClient.publishSlaveServer( self.__url )
        if not dRetVal[ 'OK' ]:
          gLogger.error( "Can't publish to master server", dRetVal[ 'Message' ] )
      return True
    else:
      gLogger.warn( "No master server is specified in the configuration, trying to get data from other slaves" )
      return self.__refresh()[ 'OK' ]

  def __refresh( self ):
    self.__lastUpdateTime = time.time()
    gLogger.debug( "Refreshing configuration..." )
    gatewayList = getGatewayURLs( "Configuration/Server" )
    updatingErrorsList = []
    if gatewayList:
      lInitialListOfServers = gatewayList
      gLogger.debug( "Using configuration gateway", str( lInitialListOfServers[0] ) )
    else:
      lInitialListOfServers = gConfigurationData.getServers()
      gLogger.debug( "Refreshing from list %s" % str( lInitialListOfServers ) )
    lRandomListOfServers = List.randomize( lInitialListOfServers )
    gLogger.debug( "Randomized server list is %s" % ", ".join( lRandomListOfServers ) )

    for sServer in lRandomListOfServers:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      oClient = RPCClient( sServer,
                         useCertificates = gConfigurationData.useServerCertificate(),
                         skipCACheck = gConfigurationData.skipCACheck() )
      dRetVal = _updateFromRemoteLocation( oClient )
      if dRetVal[ 'OK' ]:
        return dRetVal
      else:
        updatingErrorsList.append( dRetVal[ 'Message' ] )
        gLogger.warn( "Can't update from server", "Error while updating from %s: %s" % ( sServer, dRetVal[ 'Message' ] ) )
        if dRetVal[ 'Message' ].find( "Insane environment" ) > -1:
          break
    return S_ERROR( "Reason(s):\n\t%s" % "\n\t".join( List.uniqueElements( updatingErrorsList ) ) )

  def daemonize( self ):
    self.setDaemon( 1 )
    self.start()

gRefresher = Refresher()

if __name__ == "__main__":
  time.sleep( 0.1 )
  gRefresher.daemonize()
