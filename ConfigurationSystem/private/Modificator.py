# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/Modificator.py,v 1.4 2007/10/23 17:36:39 acasajus Exp $
__RCSID__ = "$Id: Modificator.py,v 1.4 2007/10/23 17:36:39 acasajus Exp $"

import zlib
import difflib
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.private.CFG import CFG
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

class Modificator:

  def __init__( self, rpcClient = False, commiterId = "unknown" ):
    self.commiterTag = "@@-"
    self.commiterId = commiterId
    if rpcClient:
      self.setRPCClient( rpcClient )

  def getCredentials(self):
    retVal = self.rpcClient.getCredentials()
    if retVal[ 'OK' ]:
      credDict = retVal[ 'Value' ]
      self.commiterId = "%s@%s - %s" % ( credDict[ 'username' ], credDict[ 'group' ], credDict[ 'DN' ] )
      return retVal
    return retVal

  def setRPCClient( self, rpcClient ):
    self.rpcClient = rpcClient

  def loadFromRemote( self ):
    retVal = self.rpcClient.getCompressedData()
    if retVal[ 'OK' ]:
      self.cfgData = CFG()
      self.cfgData.loadFromBuffer( zlib.decompress( retVal[ 'Value' ] ) )
    return retVal

  def getSections( self, sectionPath ):
    return gConfigurationData.getSectionsFromCFG( sectionPath, self.cfgData )

  def getComment( self, sectionPath ):
    return gConfigurationData.getCommentFromCFG( sectionPath, self.cfgData )

  def getOptions( self, sectionPath ):
    return gConfigurationData.getOptionsFromCFG( sectionPath, self.cfgData )

  def getValue( self, optionPath ):
    return gConfigurationData.extractOptionFromCFG( optionPath, self.cfgData )

  def __getSubCFG( self, path ):
    sectionList = List.fromChar( path, "/" )
    cfg = self.cfgData
    try:
      for section in sectionList[:-1]:
        cfg = cfg[ section ]
      return cfg
    except:
      return False

  def __setCommiter( self, entryPath ):
    cfg = self.__getSubCFG( entryPath )
    entry = List.fromChar( entryPath, "/" )[-1]
    comment = cfg.getComment( entry )
    filteredComment = [ line.strip() for line in comment.split( "\n" ) if line.find( self.commiterTag ) != 0 ]
    filteredComment.append( "%s%s" % ( self.commiterTag, self.commiterId ) )
    cfg.setComment( entry, "\n".join( filteredComment ) )

  def setOptionValue( self, optionPath, value ):
    gConfigurationData.setOptionInCFG( optionPath, value, self.cfgData )
    self.__setCommiter( optionPath )

  def setComment( self, entryPath, value ):
    cfg = self.__getSubCFG( entryPath )
    entry = List.fromChar( entryPath, "/" )[-1]
    if cfg.setComment( entry, value ):
      self.__setCommiter( entryPath )
      return True
    return False

  def existsSection( self, sectionPath ):
    sectionList = List.fromChar( sectionPath, "/" )
    cfg = self.cfgData
    try:
      for section in sectionList[:-1]:
        cfg = cfg[ section ]
      return len( sectionList ) == 0 or sectionList[-1] in cfg.listSections()
    except:
      return False

  def existsOption( self, optionPath ):
    sectionList =  List.fromChar( optionPath, "/" )
    cfg = self.cfgData
    try:
      for section in sectionList[:-1]:
        cfg = cfg[ section ]
      return sectionList[-1] in cfg.listOptions()
    except:
      return False

  def removeOption( self, optionPath ):
    if not self.existsOption( optionPath ):
      return False
    cfg = self.__getSubCFG( optionPath )
    optionName = List.fromChar( optionPath, "/" )[-1]
    return cfg.deleteKey( optionName )

  def removeSection( self, sectionPath ):
    if not self.existsSection( sectionPath ):
      return False
    cfg = self.__getSubCFG( sectionPath )
    sectionName = List.fromChar( sectionPath, "/" )[-1]
    return cfg.deleteKey( sectionName )

  def loadFromBuffer( self, data ):
    self.cfgData = CFG()
    self.cfgData.loadFromBuffer( data )

  def loadFromFile( self, filename ):
    self.cfgData = CFG()
    self.mergeFromFile( filename )

  def dumpToFile( self, filename ):
    fd = file( filename, "w" )
    fd.write( str( self.cfgData ) )
    fd.close()

  def mergeFromFile( self, filename ):
    cfg = CFG()
    cfg.loadFromFile( filename )
    self.cfgData = self.cfgData.mergeWith( cfg )

  def __str__( self ):
    return str( self.cfgData )

  def commit( self ):
    compressedData = zlib.compress( str( self.cfgData ), 9 )
    return self.rpcClient.commitNewData( compressedData )

  def getHistory( self, limit = 0 ):
    retVal = self.rpcClient.getCommitHistory( limit )
    if retVal[ 'OK' ]:
      return retVal[ 'Value' ]
    return []

  def showDiff( self ):
    retVal = self.rpcClient.getCompressedData()
    if retVal[ 'OK' ]:
      remoteData = zlib.decompress( retVal[ 'Value' ] ).splitlines()
      localData = str( self.cfgData ).splitlines()
      return difflib.ndiff( remoteData, localData )
    return []

  def mergeWithServer( self ):
    retVal = self.rpcClient.getCompressedData()
    if retVal[ 'OK' ]:
      remoteCFG = CFG()
      remoteCFG.loadFromBuffer( zlib.decompress( retVal[ 'Value' ] ) )
      serverVersion = gConfigurationData.getVersion( remoteCFG )
      self.cfgData = remoteCFG.mergeWith( self.cfgData )
      gConfigurationData.setVersion( serverVersion, self.cfgData )
    return retVal
