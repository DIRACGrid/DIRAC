# $HeadURL$
__RCSID__ = "$Id$"

import os
import time
import re
import threading
import zipfile
import zlib
import DIRAC
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData, ConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient

class ServiceInterface( threading.Thread ):

  def __init__( self, sURL ):
    threading.Thread.__init__( self )
    self.sURL = sURL
    gLogger.info( "Initializing Configuration Service", "URL is %s" % sURL )
    self.__modificationsIgnoreMask = [ '/DIRAC/Configuration/Servers', '/DIRAC/Configuration/Version' ]
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

  def __launchCheckSlaves( self ):
    gLogger.info( "Starting purge slaves thread" )
    self.setDaemon( 1 )
    self.start()

  def __loadConfigurationData( self ):
    try:
      os.makedirs( os.path.join( DIRAC.rootPath, "etc", "csbackup" ) )
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
    if not gConfigurationData.isMaster():
      return S_ERROR( "Configuration modification is not allowed in this server" )
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

  def updateConfiguration( self, sBuffer, commiter = "", updateVersionOption = False ):
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
    gLogger.info( "Checking versions\nremote: %s\nlocal:  %s" % ( sRemoteVersion, sLocalVersion ) )
    if sRemoteVersion != sLocalVersion:
      if not gConfigurationData.mergingEnabled():
        return S_ERROR( "Local and remote versions differ (%s vs %s). Cannot commit." % ( sLocalVersion, sRemoteVersion ) )
      else:
        gLogger.info( "AutoMerging new data!" )
        if updateVersionOption:
          return S_ERROR( "Cannot AutoMerge! version was overwritten" )
        result = self.__mergeIndependentUpdates( oRemoteConfData )
        if not result[ 'OK' ]:
          gLogger.warn( "Could not AutoMerge!", result[ 'Message' ] )
          return S_ERROR( "AutoMerge failed: %s" % result[ 'Message' ] )
        requestedRemoteCFG = result[ 'Value' ]
        gLogger.info( "AutoMerge successful!" )
        oRemoteConfData.setRemoteCFG( requestedRemoteCFG )
    #Test that configuration names are the same
    sRemoteName = oRemoteConfData.getName()
    sLocalName = gConfigurationData.getName()
    if sRemoteName != sLocalName:
      return S_ERROR( "Names differ: Server is %s and remote is %s" % ( sLocalName, sRemoteName ) )
    #Update and generate a new version
    gLogger.info( "Committing new data..." )
    gConfigurationData.lock()
    gLogger.info( "Setting the new CFG" )
    gConfigurationData.setRemoteCFG( oRemoteConfData.getRemoteCFG() )
    gConfigurationData.unlock()
    gLogger.info( "Generating new version" )
    gConfigurationData.generateNewVersion()
    #self.__checkSlavesStatus( forceWriteConfiguration = True )
    gLogger.info( "Writing new version to disk!" )
    retVal = gConfigurationData.writeRemoteConfigurationToDisk( "%s@%s" % ( commiter, gConfigurationData.getVersion() ) )
    gLogger.info( "New version it is!" )
    return retVal

  def getCompressedConfigurationData( self ):
    return gConfigurationData.getCompressedData()

  def getVersion( self ):
    return gConfigurationData.getVersion()

  def getCommitHistory( self ):
    files = self.__getCfgBackups( gConfigurationData.getBackupDir() )
    backups = [ ".".join( fileName.split( "." )[1:-1] ).split( "@" ) for fileName in files ]
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
      retVal = S_OK( zlib.compress( zFile.read( cfgName ) , 9 ) )
      zFile.close()
      return retVal
    return S_ERROR( "Version %s does not exist" % date )

  def __getCfgBackups( self, basePath, date = "", subPath = "" ):
    rs = re.compile( "^%s\..*%s.*\.zip$" % ( gConfigurationData.getName(), date ) )
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

  def __getPreviousCFG( self, oRemoteConfData ):
    remoteExpectedVersion = oRemoteConfData.getVersion()
    backupsList = self.__getCfgBackups( gConfigurationData.getBackupDir(), date = oRemoteConfData.getVersion() )
    if not backupsList:
      return S_ERROR( "Could not AutoMerge. Could not retrieve original commiter's version" )
    prevRemoteConfData = ConfigurationData()
    backFile = backupsList[0]
    if backFile[0] == "/":
      backFile = os.path.join( gConfigurationData.getBackupDir(), backFile[1:] )
    try:
      prevRemoteConfData.loadConfigurationData( backFile )
    except Exception, e:
      return S_ERROR( "Could not load original commiter's version: %s" % str( e ) )
    gLogger.info( "Loaded client original version %s" % prevRemoteConfData.getVersion() )
    return S_OK( prevRemoteConfData.getRemoteCFG() )

  def _checkConflictsInModifications( self, realModList, reqModList, parentSection = "" ):
    realModifiedSections = dict( [ ( modAc[1], modAc[3] ) for modAc in realModList if modAc[0].find( 'Sec' ) == len( modAc[0] ) - 3 ] )
    reqOptionsModificationList = dict( [ ( modAc[1], modAc[3] ) for modAc in reqModList if modAc[0].find( 'Opt' ) == len( modAc[0] ) - 3 ] )
    optionModRequests = 0
    for modAc in reqModList:
      action = modAc[0]
      objectName = modAc[1]
      if action == "addSec":
        if objectName in realModifiedSections:
          return S_ERROR( "Section %s/%s already exists" % ( parentSection, objectName ) )
      elif action == "delSec":
        if objectName in realModifiedSections:
          return S_ERROR( "Section %s/%s cannot be deleted. It has been modified." % ( parentSection, objectName ) )
      elif action == "modSec":
        if objectName in realModifiedSections:
          result = self._checkConflictsInModifications( realModifiedSections[ objectName ],
                                                         modAc[3], "%s/%s" % ( parentSection, objectName ) )
          if not result[ 'OK' ]:
            return result
    for modAc in realModList:
      action = modAc[0]
      objectName = modAc[1]
      if action.find( "Opt" ) == len( action ) - 3:
        return S_ERROR( "Section %s cannot be merged. Option %s/%s has been modified" % ( parentSection, parentSection, objectName ) )
    return S_OK()

  def __mergeIndependentUpdates( self, oRemoteConfData ):
    #return S_ERROR( "AutoMerge is still not finished. Meanwhile... why don't you get the newest conf and update from there?" )
    #Get all the CFGs
    curSrvCFG = gConfigurationData.getRemoteCFG().clone()
    curCliCFG = oRemoteConfData.getRemoteCFG().clone()
    result = self.__getPreviousCFG( oRemoteConfData )
    if not result[ 'OK' ]:
      return result
    prevCliCFG = result[ 'Value' ]
    #Try to merge curCli with curSrv. To do so we check the updates from
    # prevCli -> curSrv VS prevCli -> curCli
    prevCliToCurCliModList = prevCliCFG.getModifications( curCliCFG )
    prevCliToCurSrvModList = prevCliCFG.getModifications( curSrvCFG )
    result = self._checkConflictsInModifications( prevCliToCurSrvModList,
                                                   prevCliToCurCliModList )
    if not result[ 'OK' ]:
      return S_ERROR( "Cannot AutoMerge: %s" % result[ 'Message' ] )
    #Merge!
    result = curSrvCFG.applyModifications( prevCliToCurCliModList )
    if not result[ 'OK' ]:
      return result
    return S_OK( curSrvCFG )
