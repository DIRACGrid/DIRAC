# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/ServiceInterface.py,v 1.4 2007/05/08 14:44:04 acasajus Exp $
__RCSID__ = "$Id: ServiceInterface.py,v 1.4 2007/05/08 14:44:04 acasajus Exp $"

import sys
import time
import threading
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData, ConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class ServiceInterface( threading.Thread ):

  def __init__( self, sURL ):
    threading.Thread.__init__( self )
    self.sURL = sURL
    gLogger.info( "Initializing Configuration Service", "URL is %s" % sURL )
    gConfigurationData.setAsService()
    if not gConfigurationData.isMaster():
      gLogger.info( "Starting configuration service as slave" )
      gRefresher.autoRefreshAndPublish( self.sURL )
    else:
      gLogger.info( "Starting configuration service as master" )
      gRefresher.disable()
      self.__loadConfigurationData()
      self.dAliveSlaveServers = {}
      self.__launchCheckSlaves()

  def __launchCheckSlaves(self):
    gLogger.info( "Starting purge slaves thread" )
    self.setDaemon(1)
    self.start()

  def __loadConfigurationData( self ):
    gConfigurationData.loadConfigurationData()
    if gConfigurationData.isMaster():
      bBuiltNewConfiguration = False
      sVersion = gConfigurationData.getVersion()
      if sVersion == "0":
        gLogger.info( "There's no version. Generating a new one" )
        gConfigurationData.generateNewVersion()
        bBuiltNewConfiguration = True

      if self.sURL not in gConfigurationData.getServers():
        gConfigurationData.setServers( self.sURL )
        bBuiltNewConfiguration = True

        gConfigurationData.setMasterServer( self.sURL )

      if bBuiltNewConfiguration:
        gConfigurationData.writeRemoteConfigurationToDisk()

  def __generateNewVersion( self ):
    if gConfigurationData.isMaster():
      gConfigurationData.generateNewVersion()
      gConfigurationData.writeRemoteConfigurationToDisk()

  def publishSlaveServer( self, sSlaveURL ):
    bNewSlave = False
    if not sSlaveURL in self.dAliveSlaveServers.keys():
      bNewSlave = True
      gLogger.info( "New slave registered", sSlaveURL )
    self.dAliveSlaveServers[ sSlaveURL ] = time.time()
    if bNewSlave:
      gConfigurationData.setServers( "%s, %s" % ( self.sURL,
                                                    ", ".join( self.dAliveSlaveServers.keys() ) ) )
      self.__generateNewVersion()

  def __checkSlavesStatus( self, forceWriteConfiguration = False ):
    gLogger.info( "Checking status of slave servers" )
    iGraceTime = gConfigurationData.getSlavesGraceTime()
    lSlaveURLs = self.dAliveSlaveServers.keys()
    bModifiedSlaveServers = False
    for sSlaveURL in lSlaveURLs:
      if time.time() - self.dAliveSlaveServers[ sSlaveURL ] > iGraceTime:
        gLogger.info( "Found dead slave", sSlaveURL )
        del( self.dAliveSlaveServers[ sSlaveURL ] )
        bModifiedSlaveServers = True
    if bModifiedSlaveServers or forceWriteConfiguration:
      gConfigurationData.setServers( "%s, %s" % ( self.sURL,
                                                    ", ".join( self.dAliveSlaveServers.keys() ) ) )
      self.__generateNewVersion()

  def now( self ):
    from DIRAC.Core.Utils.Time import datetime
    return datetime()

  def getCompressedConfiguration( self ):
    sData = gConfigurationData.getCompressedData()

  def updateConfiguration( self, sBuffer ):
    if not gConfigurationData.isMaster():
      return S_ERROR( "Configuration modification is not allowed in this server" )
    #Load the data in a ConfigurationData object
    oRemoteConfData = ConfigurationData( False )
    oRemoteConfData.loadRemoteCFGFromCompressedMem( sBuffer )
    #Test that remote and new versions are the same
    sRemoteVersion = oRemoteConfData.getVersion()
    sLocalVersion = gConfigurationData.getVersion()
    if sRemoteVersion != sLocalVersion:
      return S_ERROR( "Configuration names differ: Server %s is and remote is %s" % ( sLocalName, sRemoteName ) )
    #Test that configuration names are the same
    sRemoteName = oRemoteConfData.getName()
    sLocalName = gConfigurationData.getName()
    if sRemoteName != sLocalName:
      return S_ERROR( "Versions differ: Server is %s and remote is %s" % ( sLocalVersion, sRemoteVersion ) )
    #Update and generate a new version
    gConfigurationData.lock()
    gConfigurationData.loadRemoteCFGFromCompressedMem( sBuffer )
    gConfigurationData.generateNewVersion()
    #self.__checkSlavesStatus( forceWriteConfiguration = True )
    gConfigurationData.writeRemoteConfigurationToDisk( sLocalVersion )
    gConfigurationData.unlock()
    return S_OK()

  def getCompressedConfigurationData( self ):
    return gConfigurationData.getCompressedData()

  def getVersion( self ):
    return gConfigurationData.getVersion()

  def run( self ):
    while True:
      iWaitTime = gConfigurationData.getSlavesGraceTime()
      time.sleep( iWaitTime )
      self.__checkSlavesStatus()

