# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/Refresher.py,v 1.4 2007/03/29 17:11:24 acasajus Exp $
__RCSID__ = "$Id: Refresher.py,v 1.4 2007/03/29 17:11:24 acasajus Exp $"

import threading
import time
import os
import random
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class Refresher( threading.Thread ):

  def __init__( self ):
    threading.Thread.__init__( self )
    self.bAutomaticUpdate = False
    self.iLastUpdateTime = 0
    self.bUpdating = False
    self.sURL = False
    self.bEnabled = True
    random.seed()
    self.oTriggeredRefreshLock = threading.Lock()

  def disable( self ):
    self.bEnabled = False

  def refreshConfigurationIfNeeded( self ):
    # FIXME: this should if necesary lauch a thread and wait few seconds at most
    # to return.
    if not self.bEnabled:
      return
    if self.bAutomaticUpdate:
      return
    if self.bUpdating:
      return
    if time.time() - self.iLastUpdateTime < gConfigurationData.getRefreshTime():
      return
    self.__lockRefresh()
    bResult = self.__refresh()
    self.__unlockRefresh()
    return bResult

  def forceRefreshConfiguration( self ):
    if self.bEnabled:
      return self.__refresh()
    return True

  def autoRefreshAndPublish( self, sURL ):
    gLogger.info( "Setting configuration refresh as automatic" )
    if not gConfigurationData.getAutoPublish():
      gLogger.info( "Slave server won't auto publish itself" )
    if not gConfigurationData.getName():
      gLogger.fatal( "Missing configuration name!" )
      os._exit(1)
    self.sURL = sURL
    self.bAutomaticUpdate = True
    self.setDaemon(1)
    self.start()

  def run( self ):
    while self.bAutomaticUpdate:
      if not self.__refreshAndPublish():
        gLogger.error( "Can't refresh configuration from any source" )
      iWaitTime = gConfigurationData.getPropagationTime()
      time.sleep( iWaitTime )

  def __lockRefresh( self ):
    self.oTriggeredRefreshLock.acquire()
    self.bUpdating = True

  def __unlockRefresh( self ):
    self.bUpdating = False
    self.oTriggeredRefreshLock.release()

  def __refreshAndPublish( self ):
    self.iLastUpdateTime = time.time()
    gLogger.info( "Refresing from master server" )
    from DIRAC.Core.DISET.Client import Client
    sMasterServer = gConfigurationData.getMasterServer()
    if sMasterServer:
      oClient = Client( sMasterServer )
      if gConfigurationData.getAutoPublish():
        gLogger.debug( "Publishing to master server..." )
        dRetVal = oClient.publishSlaveServer( self.sURL )
        if not dRetVal[ 'OK' ]:
          gLogger.error( "Can't publish to master server", dRetVal[ 'Message' ] )
      dRetVal = self.__updateFromRemoteLocation( oClient )
      if not dRetVal[ 'OK' ]:
        gLogger.error( "Can't update from master server", dRetVal[ 'Message' ] )
        return False
      return True
    else:
      gLogger.warn( "No master server is specified in the configuration, trying to get data from other slaves")
      return self.__refresh()[ 'OK' ]

  def __refresh( self ):
    self.iLastUpdateTime = time.time()
    gLogger.verbose( "Refresing configuration..." )
    sGateway = gConfigurationData.getConfigurationGateway()
    if sGateway:
      lInitialListOfServers = [ sGateway ]
      gLogger.debug( "Using configuration gateway", str( lInitialListOfServers[0] ) )
    else:
      lInitialListOfServers = gConfigurationData.getServers()
      gLogger.debug( "Refresing from list %s" % str( lInitialListOfServers ) )
    lRandomListOfServers = List.randomize( lInitialListOfServers )
    gLogger.debug( "Randomized server list is %s" % ", ".join( lRandomListOfServers ) )

    for sServer in lRandomListOfServers:
        from DIRAC.Core.DISET.Client import Client
        oClient = Client( sServer )
        dRetVal = self.__updateFromRemoteLocation( oClient )
        if dRetVal[ 'OK' ]:
          return dRetVal
        else:
          gLogger.warn( "Can't update from server", "Error while updating from %s: %s" %( sServer, dRetVal[ 'Message' ] ) )
    return S_ERROR( "Can't update from any server" )

  def __updateFromRemoteLocation( self, oClient ):
    gLogger.debug( "", "Trying to refresh from %s" % oClient.sURL )
    dRetVal = oClient.getVersion()
    if not dRetVal[ 'OK' ]:
      return dRetVal
    sVersion = gConfigurationData.getVersion()
    if sVersion < dRetVal[ 'Value' ]:
      gLogger.info( "New version available. Updating.." )
      dRetVal = oClient.getCompressedData()
      if not dRetVal[ 'OK' ]:
        return dRetVal
      gConfigurationData.loadRemoteCFGFromCompressedMem( dRetVal[ 'Value' ] )
      gLogger.info( "New configuration version %s" % gConfigurationData.getVersion() )
    return S_OK()

gRefresher = Refresher()

if __name__=="__main__":
  time.sleep(0.1)
  gRefresher.daemonize()
