# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/Refresher.py,v 1.27 2008/09/09 14:02:23 acasajus Exp $
__RCSID__ = "$Id: Refresher.py,v 1.27 2008/09/09 14:02:23 acasajus Exp $"

import threading
import time
import os
import random
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.PathFinder import getGatewayURL
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class Refresher( threading.Thread ):

  def __init__( self ):
    threading.Thread.__init__( self )
    self.bAutomaticUpdate = False
    self.iLastUpdateTime = 0
    self.sURL = False
    self.bEnabled = True
    self.timeout = False
    random.seed()
    self.oTriggeredRefreshLock = threading.Lock()

  def disable( self ):
    self.bEnabled = False

  def __refreshInThread(self):
    retVal = self.__refresh()
    if not retVal[ 'OK' ]:
      gLogger.error( "Error while updating the configuration", retVal[ 'Message' ] )

  def refreshConfigurationIfNeeded( self ):
    if not self.bEnabled or self.bAutomaticUpdate or not gConfigurationData.getServers():
      return
    self.oTriggeredRefreshLock.acquire()
    try:
      if time.time() - self.iLastUpdateTime < gConfigurationData.getRefreshTime():
        return
      self.iLastUpdateTime = time.time()
      thd = threading.Thread( target = self.__refreshInThread )
      thd.setDaemon(1)
      thd.start()
    finally:
      self.oTriggeredRefreshLock.release()

  def forceRefresh( self ):
    if self.bEnabled:
      return self.__refresh()
    return S_OK()

  def autoRefreshAndPublish( self, sURL ):
    gLogger.debug( "Setting configuration refresh as automatic" )
    if not gConfigurationData.getAutoPublish():
      gLogger.debug( "Slave server won't auto publish itself" )
    if not gConfigurationData.getName():
      DIRAC.abort( 10, "Missing configuration name!" )
    self.sURL = sURL
    self.bAutomaticUpdate = True
    self.setDaemon(1)
    self.start()

  def run( self ):
    while self.bAutomaticUpdate:
      iWaitTime = gConfigurationData.getPropagationTime()
      time.sleep( iWaitTime )
      if not self.__refreshAndPublish():
        gLogger.error( "Can't refresh configuration from any source" )


  def __refreshAndPublish( self ):
    self.iLastUpdateTime = time.time()
    gLogger.info( "Refreshing from master server" )
    from DIRAC.Core.DISET.RPCClient import RPCClient
    sMasterServer = gConfigurationData.getMasterServer()
    if sMasterServer:
      oClient = RPCClient( sMasterServer, timeout = self.timeout, useCertificates = gConfigurationData.useServerCertificate() )
      dRetVal = self.__updateFromRemoteLocation( oClient )
      if not dRetVal[ 'OK' ]:
        gLogger.error( "Can't update from master server", dRetVal[ 'Message' ] )
        return False
      if gConfigurationData.getAutoPublish():
        gLogger.info( "Publishing to master server..." )
        dRetVal = oClient.publishSlaveServer( self.sURL )
        if not dRetVal[ 'OK' ]:
          gLogger.error( "Can't publish to master server", dRetVal[ 'Message' ] )
      return True
    else:
      gLogger.warn( "No master server is specified in the configuration, trying to get data from other slaves")
      return self.__refresh()[ 'OK' ]

  def __refresh( self ):
    self.iLastUpdateTime = time.time()
    gLogger.debug( "Refreshing configuration..." )
    sGateway = getGatewayURL()
    updatingErrorsList = []
    if sGateway:
      lInitialListOfServers = [ sGateway ]
      gLogger.debug( "Using configuration gateway", str( lInitialListOfServers[0] ) )
    else:
      lInitialListOfServers = gConfigurationData.getServers()
      gLogger.debug( "Refreshing from list %s" % str( lInitialListOfServers ) )
    lRandomListOfServers = List.randomize( lInitialListOfServers )
    gLogger.debug( "Randomized server list is %s" % ", ".join( lRandomListOfServers ) )

    for sServer in lRandomListOfServers:
        from DIRAC.Core.DISET.RPCClient import RPCClient
        #oClient = RPCClient( sServer, timeout = self.timeout, useCertificates = gConfigurationData.useServerCertificate() )
        oClient = RPCClient( sServer, useCertificates = gConfigurationData.useServerCertificate() )
        dRetVal = self.__updateFromRemoteLocation( oClient )
        if dRetVal[ 'OK' ]:
          return dRetVal
        else:
          updatingErrorsList.append( dRetVal[ 'Message' ] )
          gLogger.warn( "Can't update from server", "Error while updating from %s: %s" %( sServer, dRetVal[ 'Message' ] ) )
    return S_ERROR( "Reason(s):\n\t%s" % "\n\t".join( List.uniqueElements( updatingErrorsList) ) )

  def __updateFromRemoteLocation( self, serviceClient ):
    gLogger.debug( "", "Trying to refresh from %s" % serviceClient.serviceURL )
    localVersion = gConfigurationData.getVersion()
    retVal = serviceClient.getCompressedDataIfNewer( localVersion )
    if retVal[ 'OK' ]:
      dataDict = retVal[ 'Value' ]
      if localVersion < dataDict[ 'newestVersion' ] :
        gLogger.debug( "New version available", "Updating to version %s..." % dataDict[ 'newestVersion' ] )
        gConfigurationData.loadRemoteCFGFromCompressedMem( dataDict[ 'data' ] )
        gLogger.debug( "Updated to version %s" % gConfigurationData.getVersion() )
      return S_OK()
    return retVal

gRefresher = Refresher()

if __name__=="__main__":
  time.sleep(0.1)
  gRefresher.daemonize()
