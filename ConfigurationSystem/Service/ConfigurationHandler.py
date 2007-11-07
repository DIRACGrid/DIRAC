# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Service/ConfigurationHandler.py,v 1.6 2007/11/07 16:08:37 acasajus Exp $
__RCSID__ = "$Id: ConfigurationHandler.py,v 1.6 2007/11/07 16:08:37 acasajus Exp $"
import types
from DIRAC.ConfigurationSystem.private.ServiceInterface import ServiceInterface
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

gServiceInterface = False

def initializeConfigurationHandler( serviceInfo ):
  global gServiceInterface
  gServiceInterface = ServiceInterface( serviceInfo[ 'URL' ] )
  return S_OK()

class ConfigurationHandler( RequestHandler ):

  types_getVersion = []
  def export_getVersion( self ):
    return S_OK( gServiceInterface.getVersion() )

  types_getCompressedData = []
  def export_getCompressedData( self ):
    sData = gServiceInterface.getCompressedConfigurationData()
    return S_OK( sData )

  types_getCompressedDataIfNewer = [ types.StringType ]
  def export_getCompressedDataIfNewer( self, sClientVersion ):
    sVersion = gServiceInterface.getVersion()
    retDict = { 'newestVersion' : sVersion }
    if sClientVersion < sVersion:
      retDict[ 'data' ] = gServiceInterface.getCompressedConfigurationData()
    return S_OK( retDict )

  types_publishSlaveServer = [ types.StringType ]
  def export_publishSlaveServer( self, sURL ):
    gServiceInterface.publishSlaveServer( sURL )
    return S_OK()

  types_commitNewData = [ types.StringType ]
  def export_commitNewData( self, sData ):
    credDict = self.getRemoteCredentials()
    if not 'DN' in credDict or not 'username' in credDict:
      return S_ERROR( "You must be authenticated!" )
    return gServiceInterface.updateConfiguration( sData, credDict[ 'username' ] )

  types_writeEnabled = []
  def export_writeEnabled( self ):
    return S_OK( gServiceInterface.isMaster() )

  types_getCommitHistory = []
  def export_getCommitHistory( self, limit = 100 ):
    if limit > 100:
      limit = 100
    history = gServiceInterface.getCommitHistory()
    if limit:
      history = history[ :limit ]
    return S_OK( history )

  types_getVersionContents = [ types.ListType ]
  def export_getVersionContents( self, versionList ):
    contentsList = []
    for version in versionList:
      retVal = gServiceInterface.getVersionContents( version )
      if retVal[ 'OK' ]:
        contentsList.append( retVal[ 'Value' ] )
      else:
        return S_ERROR( "Can't get contents for version %s: %s" % ( version, retVal[ 'Message' ] ) )
    return S_OK( contentsList )

  types_rollbackToVersion = [ types.StringType ]
  def export_rollbackToVersion( self, version ):
    retVal = gServiceInterface.getVersionContents( version )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't get contents for version %s: %s" % ( version, retVal[ 'Message' ] ) )
    credDict = self.getRemoteCredentials()
    if not 'DN' in credDict or not 'username' in credDict:
      return S_ERROR( "You must be authenticated!" )
    return gServiceInterface.updateConfiguration( retVal[ 'Value' ] , credDict[ 'username' ], updateVersionOption = True )
