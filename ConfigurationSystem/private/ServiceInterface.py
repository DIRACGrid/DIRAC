# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/ServiceInterface.py,v 1.10 2008/03/05 16:32:00 acasajus Exp $
__RCSID__ = "$Id: ServiceInterface.py,v 1.10 2008/03/05 16:32:00 acasajus Exp $"

import sys
import os
import time
import re
import threading
import zipfile
import zlib
import DIRAC
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData, ConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

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

  def isMaster( self ):
    return gConfigurationData.isMaster()

  def __launchCheckSlaves(self):
    gLogger.info( "Starting purge slaves thread" )
    self.setDaemon(1)
    self.start()

  def __loadConfigurationData( self ):
    try:
      os.makedirs( "%s/etc/csbackup" % DIRAC.rootPath )
    except:
      pass
    gConfigurationData.loadConfigurationData()
    if gConfigurationData.isMaster():
      bBuiltNewConfiguration = False
      if not gConfigurationData.getName():
        DIRAC.abort( 10, "Missing name for the configuration to be exported!" )
      gConfigurationData.exportName()
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
    gLogger.info( "Pinging slave %s" % sSlaveURL )
    rpcClient = RPCClient( sSlaveURL, timeout = 10, useCertificates = True )
    retVal = rpcClient.ping()
    if not retVal[ 'OK' ]:
      gLogger.info( "Slave %s didn't reply" % sSlaveURL )
      return
    if retVal[ 'Value' ][ 'name' ] != 'Configuration/Server':
      gLogger.info( "Slave %s is not a CS serveR" % sSlaveURL )
      return
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

  def getCompressedConfiguration( self ):
    sData = gConfigurationData.getCompressedData()

  def updateConfiguration( self, sBuffer, commiterDN = "", updateVersionOption = False ):
    if not gConfigurationData.isMaster():
      return S_ERROR( "Configuration modification is not allowed in this server" )
    #Load the data in a ConfigurationData object
    oRemoteConfData = ConfigurationData( False )
    oRemoteConfData.loadRemoteCFGFromCompressedMem( sBuffer )
    if updateVersionOption:
      oRemoteConfData.setVersion( gConfigurationData.getVersion() )
    #Test that remote and new versions are the same
    sRemoteVersion = oRemoteConfData.getVersion()
    sLocalVersion = gConfigurationData.getVersion()
    print sRemoteVersion, sLocalVersion
    if sRemoteVersion != sLocalVersion:
      return S_ERROR( "Versions differ: Server %s is and remote is %s" % ( sLocalVersion, sRemoteVersion ) )
    #Test that configuration names are the same
    sRemoteName = oRemoteConfData.getName()
    sLocalName = gConfigurationData.getName()
    if sRemoteName != sLocalName:
      return S_ERROR( "Names differ: Server is %s and remote is %s" % ( sLocalName, sRemoteName ) )
    #Update and generate a new version
    gConfigurationData.lock()
    gConfigurationData.loadRemoteCFGFromCompressedMem( sBuffer )
    gConfigurationData.generateNewVersion()
    #self.__checkSlavesStatus( forceWriteConfiguration = True )
    retVal = gConfigurationData.writeRemoteConfigurationToDisk( "%s@%s" % ( commiterDN, gConfigurationData.getVersion() ) )
    gConfigurationData.unlock()
    return retVal

  def getCompressedConfigurationData( self ):
    return gConfigurationData.getCompressedData()

  def getVersion( self ):
    return gConfigurationData.getVersion()

  def getCommitHistory( self ):
    files = self.__getCfgBackups( gConfigurationData.getBackupDir() )
    backups = [ ".".join( file.split( "." )[1:3] ).split( "@" ) for file in files ]
    return backups

  def run( self ):
    while True:
      iWaitTime = gConfigurationData.getSlavesGraceTime()
      time.sleep( iWaitTime )
      self.__checkSlavesStatus()

  def getVersionContents( self, date ):
    backupDir = gConfigurationData.getBackupDir()
    files = self.__getCfgBackups( backupDir, date )
    for fileName in files:
      zFile = zipfile.ZipFile( "%s/%s" % ( backupDir, fileName ), "r" )
      cfgName = zFile.namelist()[0]
      #retVal = S_OK( zlib.compress( str( fd.read() ), 9 ) )
      retVal = S_OK(zlib.compress( zFile.read( cfgName ) , 9 ) )
      zFile.close()
      return retVal
    return S_ERROR( "Version %s does not exist" % date )

  def __getCfgBackups( self, basePath, date = "", subPath = "" ):
    rs = re.compile( "^%s\..+@%s.*\.zip$" % ( gConfigurationData.getName(), date ) )
    fsEntries = os.listdir( "%s/%s" % ( basePath, subPath ) )
    fsEntries.sort( reverse = True )
    backupsList = []
    for entry in fsEntries:
      entryPath = "%s/%s/%s" % ( basePath, subPath, entry )
      if os.path.isdir( entryPath ):
        backupsList.extend( self.__getCfgBackups( basePath, date, "%s/%s" % ( subPath, entry ) ) )
      elif os.path.isfile( entryPath ):
        if rs.search( entry ):
          backupsList.append( "%s/%s" % ( subPath, entry ) )
    return backupsList

