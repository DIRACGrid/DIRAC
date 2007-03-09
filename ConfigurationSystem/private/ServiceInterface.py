# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/ServiceInterface.py,v 1.1 2007/03/09 15:20:22 rgracian Exp $
__RCSID__ = "$Id: ServiceInterface.py,v 1.1 2007/03/09 15:20:22 rgracian Exp $"

import sys
import time
import threading
from DIRAC.ConfigurationSystem.Client.ConfigurationData import g_oConfigurationData, ConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import g_oRefresher
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class ServiceInterface( threading.Thread ):
  
  def __init__( self, sURL ):
    threading.Thread.__init__( self )
    self.sURL = sURL
    gLogger.info( "Initializing Configuration Service", "URL is %s" % sURL )
    g_oConfigurationData.setAsService()
    if not g_oConfigurationData.isMaster():
      gLogger.info( "Starting configuration service as slave" )
      g_oRefresher.autoRefreshAndPublish( self.sURL )
    else:
      gLogger.info( "Starting configuration service as master" )
      g_oRefresher.disable()
      self.__loadConfigurationData()
      self.dAliveSlaveServers = {}
      self.__launchCheckSlaves()
      
  def __launchCheckSlaves(self):
    gLogger.info( "Starting purge slaves thread" )
    self.setDaemon(1)
    self.start()
      
  def __loadConfigurationData( self ):
    g_oConfigurationData.loadConfigurationData()
    if g_oConfigurationData.isMaster():
      bBuiltNewConfiguration = False       
      sVersion = g_oConfigurationData.getVersion()
      if sVersion == "0":
        gLogger.info( "There's no version. Generating a new one" )
        g_oConfigurationData.generateNewVersion()
        bBuiltNewConfiguration = True
        
      if self.sURL not in g_oConfigurationData.getServers():
        g_oConfigurationData.setServers( self.sURL )
        bBuiltNewConfiguration = True
        
        g_oConfigurationData.setMasterServer( self.sURL )
        
      if bBuiltNewConfiguration:
        g_oConfigurationData.writeRemoteConfigurationToDisk()
        
  def __generateNewVersion( self ):
    if g_oConfigurationData.isMaster():
      g_oConfigurationData.generateNewVersion()
      g_oConfigurationData.writeRemoteConfigurationToDisk()
        
  def publishSlaveServer( self, sSlaveURL ):
    bNewSlave = False
    if not sSlaveURL in self.dAliveSlaveServers.keys():
      bNewSlave = True
      gLogger.info( "New slave registered", sSlaveURL )
    self.dAliveSlaveServers[ sSlaveURL ] = time.time()
    if bNewSlave:
      g_oConfigurationData.setServers( "%s, %s" % ( self.sURL,
                                                    ", ".join( self.dAliveSlaveServers.keys() ) ) )
      self.__generateNewVersion()
      
  def __checkSlavesStatus( self ):
    gLogger.info( "Checking status of slave servers" )
    iGraceTime = g_oConfigurationData.getSlavesGraceTime()
    lSlaveURLs = self.dAliveSlaveServers.keys()
    bModifiedSlaveServers = False
    for sSlaveURL in lSlaveURLs:
      if time.time() - self.dAliveSlaveServers[ sSlaveURL ] > iGraceTime:
        gLogger.info( "Found dead slave", sSlaveURL )
        del( self.dAliveSlaveServers[ sSlaveURL ] )
        bModifiedSlaveServers = True
    if bModifiedSlaveServers:
      g_oConfigurationData.setServers( "%s, %s" % ( self.sURL,
                                                    ", ".join( self.dAliveSlaveServers.keys() ) ) )
      self.__generateNewVersion()
      
  def now( self ):
    from DIRAC.Core.Utils.Time import datetime
    return datetime()
      
  def getCompressedConfiguration( self ):
    sData = g_oConfigurationData.getCompressedData()
    
  def updateConfiguration( self, sBuffer ):
    if not g_oConfigurationData.isMaster():
      return S_ERROR( "Configuration modification is not allowed in this server" )
    #Load the data in a ConfigurationData object
    oRemoteConfData = ConfigurationData( False )
    oRemoteConfData.loadRemoteCFGFromCompressedMem( sBuffer )
    #Test that remote and new versions are the same
    sRemoteVersion = oRemoteConfData.getVersion()
    sLocalVersion = g_oConfigurationData.getVersion()
    if sRemoteVersion != sLocalVersion:
      return S_ERROR( "Configuration names differ: Server %s is and remote is %s" % ( sLocalName, sRemoteName ) )
    #Test that configuration names are the same
    sRemoteName = oRemoteConfData.getName()
    sLocalName = g_oConfigurationData.getName()
    if sRemoteName != sLocalName:
      return S_ERROR( "Versions differ: Server is %s and remote is %s" % ( sLocalVersion, sRemoteVersion ) )
    #Update and generate a new version
    g_oConfigurationData.lock()
    g_oConfigurationData.loadRemoteCFGFromCompressedMem( sBuffer )
    g_oConfigurationData.generateNewVersion()
    g_oConfigurationData.writeRemoteConfigurationToDisk( sLocalVersion )
    g_oConfigurationData.unlock()
    return S_OK()
    
  def getCompressedConfigurationData( self ):
    return g_oConfigurationData.getCompressedData()
  
  def getVersion( self ):
    return g_oConfigurationData.getVersion()
  
  def run( self ):
    while True:
      iWaitTime = g_oConfigurationData.getSlavesGraceTime()
      time.sleep( iWaitTime )
      self.__checkSlavesStatus()
  
