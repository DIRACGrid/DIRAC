# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/Service/ConfigurationHandler.py,v 1.1 2007/03/09 15:22:43 rgracian Exp $
__RCSID__ = "$Id: ConfigurationHandler.py,v 1.1 2007/03/09 15:22:43 rgracian Exp $"
import types
from DIRAC.ConfigurationSystem.private.ServiceInterface import ServiceInterface
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

gServiceInterface = False

def initializeConfigurationHandler( serviceInfo ):
  global gServiceInterface
  gServiceInterface = ServiceInterface( serviceInfo.getURL() )
  return S_OK()

class ConfigurationHandler( RequestHandler ):
  
  types_getVersion = []
  def export_getVersion( self ):
    return S_OK( gServiceInterface.getVersion() )
  
  types_getCompressedData = []
  def export_getCompressedData( self ):
    sData = gServiceInterface.getCompressedConfigurationData()
    return S_OK( gServiceInterface.getCompressedConfigurationData() )
  
  types_publishSlaveServer = [ types.StringType ]
  def export_publishSlaveServer( self, sURL ):
    gServiceInterface.publishSlaveServer( sURL )
    return S_OK()
    
  types_setNewConfigurationData = [ types.StringType ]
  def export_setNewConfigurationData( self, sData ):
    gServiceInterface.updateConfiguration( sData )
    return S_OK( gServiceInterface.getVersion() )
    
    
