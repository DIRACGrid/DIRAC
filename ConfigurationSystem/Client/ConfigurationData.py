# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Client/ConfigurationData.py,v 1.3 2007/03/14 06:16:32 rgracian Exp $
__RCSID__ = "$Id: ConfigurationData.py,v 1.3 2007/03/14 06:16:32 rgracian Exp $"

import sys
import os.path
import zlib
import threading
import time
from DIRAC.Core.Utilities import List, Time
from DIRAC.ConfigurationSystem.private.CFG import CFG
from DIRAC.LoggingSystem.Client.Logger import gLogger

class ConfigurationData:
  
  def __init__( self, bLoadDIRACCfg = True ):
    self.oEvent = threading.Event()
    self.oEvent.set()
    self.oLock = threading.Lock()
    self.iRunningThreads = 0
    self.__getDIRACRoot()
    self.sCompressedConfigurationData= ""
    self.sConfigurationPath = "/Configuration"
    self.bIsService = False
    self.oLocalCFG = CFG()
    self.oRemoteCFG = CFG()
    if bLoadDIRACCfg:
      self.loadFile( "%s/etc/dirac.cfg" % self.sDIRACRoot )
      try:
        self.sync()
      except Exception, e:
        gLogger.fatal( "Error while loading configuration, aborting...", str( e ) )
        sys.exit( 1 )
    
  def __getDIRACRoot( self ):
    import DIRAC
    self.sDIRACRoot = DIRAC.rootPath
    gLogger.debug( "", "dirac.cfg should be at %s/etc/dirac.cfg" % self.sDIRACRoot)
    
  def sync( self ):
    gLogger.debug( "Updating configuration internals" )
    self.oMergedCFG = self.oRemoteCFG.mergeWith( self.oLocalCFG )
    self.lRemoteServers = []
    sLocalServers = self.extractOptionFromCFG( "%s/servers" % self.sConfigurationPath,
                                        self.oLocalCFG )
    if sLocalServers:
      self.lRemoteServers.extend( List.fromChar( sLocalServers, "," ) )
    sRemoteServers = self.extractOptionFromCFG( "%s/servers" % self.sConfigurationPath,
                                        self.oRemoteCFG )
    if sRemoteServers:
      self.lRemoteServers.extend( List.fromChar( sRemoteServers, "," ) )
    self.lRemoteServers = List.uniqueElements( self.lRemoteServers )
    self.sCompressedConfigurationData = zlib.compress( str( self.oRemoteCFG ), 9 )
    
  def loadFile( self, sFileName ):
    self.lock()
    try:
      oFileCFG = CFG()
      oFileCFG.loadFromFile( sFileName )
      self.oLocalCFG = self.oLocalCFG.mergeWith( oFileCFG )
    except IOError, e:
      gLogger.warn( "Can't load a cfg file", "file %s" % sFileName )
    self.unlock()
    self.sync()
          
  def loadRemoteCFGFromCompressedMem( self, sBuffer ):
    sUncompressedData = zlib.decompress( sBuffer )
    self.loadRemoteCFGFromMem( sUncompressedData )
          
  def loadRemoteCFGFromMem( self, sBuffer ):
    self.lock()
    self.oRemoteCFG.loadFromBuffer( sBuffer )
    self.unlock()
    self.sync()
    
  def loadConfigurationData( self ):
    sName = self.getName()
    self.lock()
    try:
      self.oRemoteCFG.loadFromFile( "%s/etc/%s.cfg" % ( self.sDIRACRoot, sName ) )
    except:
      pass
    self.unlock()
    self.sync()
          
  def getSectionsFromCFG( self, sPath, oCFG = False ):
    if not oCFG:
      oCFG = self.oMergedCFG
    self.dangerZoneStart()
    try:
      lLevels = [ sLevel.strip() for sLevel in sPath.split( "/" ) if sLevel.strip() != "" ]
      for sSection in lLevels[:-1]:
        oCFG = oCFG[ sSection ]
      return self.dangerZoneEnd( oCFG.listSections() )       
    except Exception, e:
      pass
    return self.dangerZoneEnd( None )

  def getOptionsFromCFG( self, sPath, oCFG = False ):
    if not oCFG:
      oCFG = self.oMergedCFG
    self.dangerZoneStart()
    try:
      lLevels = [ sLevel.strip() for sLevel in sPath.split( "/" ) if sLevel.strip() != "" ]
      for sSection in lLevels[:-1]:
        oCFG = oCFG[ sSection ]
      return self.dangerZoneEnd( oCFG.listOptions() )       
    except Exception, e:
      pass
    return self.dangerZoneEnd( None )

          
  def extractOptionFromCFG( self, sPath, oCFG = False ):
    if not oCFG:
      oCFG = self.oMergedCFG
    self.dangerZoneStart()
    try:
      lLevels = [ sLevel.strip() for sLevel in sPath.split( "/" ) if sLevel.strip() != "" ]
      for sSection in lLevels[:-1]:
        oCFG = oCFG[ sSection ]
      if lLevels[-1] in oCFG.listOptions():
        return self.dangerZoneEnd( oCFG[ lLevels[ -1 ] ] )       
    except Exception, e:
      pass
    return self.dangerZoneEnd( None )
      
  def setOptionInCFG( self, sPath, sValue, oCFG = False ):
    if not oCFG:
      oCFG = self.oLocalCFG
    self.dangerZoneStart()
    try:
      lLevels = [ sLevel.strip() for sLevel in sPath.split( "/" ) if sLevel.strip() != "" ]
      for sSection in lLevels[:-1]:
        if sSection not in oCFG.listSections():
          oCFG.createNewSection( sSection )
        oCFG = oCFG[ sSection ]
      oCFG.setOption( lLevels[ -1 ], sValue )  
    finally:
      self.dangerZoneEnd()
    self.sync()
      
  def generateNewVersion( self ):
    self.setOptionInCFG( "%s/Version" % self.sConfigurationPath,
                                  Time.toString(),
                                  self.oRemoteCFG )
    self.sync()
      
  def getVersion( self ):
    sValue = self.extractOptionFromCFG( "%s/Version" % self.sConfigurationPath,
                                        self.oRemoteCFG )
    if sValue:
      return sValue
    return "0"
    
  def getName( self ):
    return self.extractOptionFromCFG( "%s/Name" % self.sConfigurationPath,
                                        self.oMergedCFG )
    
  def getRefreshTime( self ):
    try:
      return int( self.extractOptionFromCFG( "%s/RefreshTime" % self.sConfigurationPath,
                                        self.oMergedCFG ) )
    except:
      return 300  
    
  def getPropagationTime( self ):
    try:
      return int( self.extractOptionFromCFG( "%s/PropagationTime" % self.sConfigurationPath,
                                        self.oMergedCFG ) )
    except:
      return 300  
    
  def getSlavesGraceTime( self ):
    try:
      return int( self.extractOptionFromCFG( "%s/SlavesGraceTime" % self.sConfigurationPath,
                                        self.oMergedCFG ) ) 
    except:
      return 600  
    
  def getAutoPublish( self ):
    sValue = self.extractOptionFromCFG( "%s/AutoPublish" % self.sConfigurationPath,
                                        self.oLocalCFG )
    if sValue and sValue.lower() in ( "no", "false", "n" ):
        return False
    else:
        return True
    
  def getServers( self ):
    return list( self.lRemoteServers )

  def getConfigurationGateway( self ):
    return self.extractOptionFromCFG( "%s/Gateway" % self.sConfigurationPath,
                                        self.oLocalCFG )

  def setServers( self, sServers ):
    self.setOptionInCFG( "%s/Servers" % self.sConfigurationPath,
                                  sServers,
                                  self.oRemoteCFG )
    self.sync()
    
  def getMasterServer( self ):
    return self.extractOptionFromCFG( "%s/Master" % self.sConfigurationPath, 
                                      self.oRemoteCFG )
    
  def setMasterServer( self, sURL ):
    self.setOptionInCFG( "%s/Master" % self.sConfigurationPath, 
                         sURL, 
                         self.oRemoteCFG )
    self.sync()
  
  def getCompressedData( self ):
    return self.sCompressedConfigurationData
  
  def isMaster( self ):
    sValue = self.extractOptionFromCFG( "%s/Master" % self.sConfigurationPath, 
                                            self.oLocalCFG )
    if sValue and sValue.lower() in ( "yes", "true", "y" ):
        return True
    else:
        return False
      
  def getServicesPath( self ):
    return "/Services"
    
  def setAsService( self ):
    self.bIsService = True
    
  def isService( self ):
    return self.bIsService
  
  def dumpLocalCFGToFile( self, sFileName ):
    oFD = open( sFileName, "w" )
    oFD.write( str( self.oLocalCFG ) )
    oFD.close()
    
  def dumpRemoteCFGToFile( self, sFileName ):
    oFD = open( sFileName, "w" )
    oFD.write( str( self.oRemoteCFG ) )
    oFD.close()
    
  def writeRemoteConfigurationToDisk( self, sBackupName = False ):
    import zipfile
    if not sBackupName:
      sBackupName = self.getVersion()
    sConfigurationFile = "%s/etc/%s.cfg" % ( self.sDIRACRoot, self.getName() )
    sBackupFile = sConfigurationFile.replace( ".cfg", ".%s.zip" % sBackupName )
    if os.path.isfile( sConfigurationFile ):
      try:
        zf = zipfile.ZipFile( sBackupFile, "w", zipfile.ZIP_DEFLATED );
        zf.write( sConfigurationFile, "%s.backup.%s" % ( os.path.split( sConfigurationFile )[1], sBackupName )  )
        zf.close()
      except Exception, v:
        gLogger.exception()
        gLogger.error( "Cannot backup configuration data file", 
                     "file %s" % sBackupFile )
    try:
      oFD = open( sConfigurationFile, "w" )
      oFD.write( str( self.oRemoteCFG ) )
      oFD.close()
    except:
      gLogger.fatal( "Cannot write new configuration to disk!", 
                     "file %s" % sConfigurationFile )
    
  def lock(self):
    """
    Locks Event to prevent further threads from reading.
    Stops current thread until no other thread is accessing.
    PRIVATE USE
    """
    self.oEvent.clear()
    while self.iRunningThreads > 0:
      time.sleep( 0.1 )

  def unlock(self):
    """
    Unlocks Event.
    PRIVATE USE
    """
    self.oEvent.set()

  def dangerZoneStart(self):
    """
    Start of danger zone. This danger zone may be or may not be a mutual exclusion zone. 
    Counter is maintained to know how many threads are inside and be able to enable and disable mutual exclusion.
    PRIVATE USE
    """
    self.oEvent.wait()
    self.oLock.acquire()
    self.iRunningThreads  += 1
    self.oLock.release()

      
  def dangerZoneEnd( self, returnValue = None ):
    """
    End of danger zone.
    PRIVATE USE
    """
    self.oLock.acquire()
    self.iRunningThreads -= 1
    self.oLock.release()
    return returnValue
    
g_oConfigurationData = ConfigurationData()
