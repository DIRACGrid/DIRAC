""" The CS! (Configuration Service)
"""

from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.private.ServiceInterface import ServiceInterface
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer

gServiceInterface = False

__RCSID__ = "$Id$"

def initializeConfigurationHandler( serviceInfo ):
  global gServiceInterface
  gServiceInterface = ServiceInterface( serviceInfo[ 'URL' ] )
  return S_OK()

class ConfigurationHandler( RequestHandler ):
  """ The CS handler
  """

  def initializeHandler( self, _serviceInfo ):
    """
    Handler class initialization
    """
    # Check the flag for updating the pilot 3 JSON file
    self.updatePilotJSONFile = self.srv_getCSOption( 'UpdatePilotCStoJSONFile', False )
    if self.updatePilotJSONFile:
      self.paramDict = {}
      self.paramDict['pilotFileServer'] = getServiceOption( _serviceInfo, "pilotFileServer", '' )
      self.paramDict['pilotRepo'] = getServiceOption( _serviceInfo, "pilotRepo", '' )
      self.paramDict['pilotVORepo'] = getServiceOption( _serviceInfo, "pilotVORepo", '' )
      self.paramDict['projectDir'] = getServiceOption( _serviceInfo, "projectDir", '' )
      self.paramDict['pilotVOScriptPath'] = getServiceOption( _serviceInfo, "pilotVOScriptPath", '' )
      self.paramDict['pilotScriptsPath'] = getServiceOption( _serviceInfo, "pilotScriptsPath", '' )

    return S_OK( 'Initialization went well' )

  types_getVersion = []
  def export_getVersion( self ):
    return S_OK( gServiceInterface.getVersion() )

  types_getCompressedData = []
  def export_getCompressedData( self ):
    sData = gServiceInterface.getCompressedConfigurationData()
    return S_OK( sData )

  types_getCompressedDataIfNewer = [ basestring ]
  def export_getCompressedDataIfNewer( self, sClientVersion ):
    sVersion = gServiceInterface.getVersion()
    retDict = { 'newestVersion' : sVersion }
    if sClientVersion < sVersion:
      retDict[ 'data' ] = gServiceInterface.getCompressedConfigurationData()
    return S_OK( retDict )

  types_publishSlaveServer = [ basestring ]
  def export_publishSlaveServer( self, sURL ):
    gServiceInterface.publishSlaveServer( sURL )
    return S_OK()

  types_commitNewData = [ basestring ]
  def export_commitNewData( self, sData ):
    credDict = self.getRemoteCredentials()
    if not 'DN' in credDict or not 'username' in credDict:
      return S_ERROR( "You must be authenticated!" )
    res = gServiceInterface.updateConfiguration( sData, credDict[ 'username' ] )

    if self.updatePilotJSONFile:
      if not res['OK']:
        return res
      return PilotCStoJSONSynchronizer( self.paramDict ).sync()


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

  types_getVersionContents = [ list ]
  def export_getVersionContents( self, versionList ):
    contentsList = []
    for version in versionList:
      retVal = gServiceInterface.getVersionContents( version )
      if retVal[ 'OK' ]:
        contentsList.append( retVal[ 'Value' ] )
      else:
        return S_ERROR( "Can't get contents for version %s: %s" % ( version, retVal[ 'Message' ] ) )
    return S_OK( contentsList )

  types_rollbackToVersion = [ basestring ]
  def export_rollbackToVersion( self, version ):
    retVal = gServiceInterface.getVersionContents( version )
    if not retVal[ 'OK' ]:
      return S_ERROR( "Can't get contents for version %s: %s" % ( version, retVal[ 'Message' ] ) )
    credDict = self.getRemoteCredentials()
    if not 'DN' in credDict or not 'username' in credDict:
      return S_ERROR( "You must be authenticated!" )
    return gServiceInterface.updateConfiguration( retVal[ 'Value' ],
                                                  credDict[ 'username' ],
                                                  updateVersionOption = True )
