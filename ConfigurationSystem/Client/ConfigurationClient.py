# $Header$
__RCSID__ = "$Id$"

from DIRAC.ConfigurationSystem.Client.ConfigurationData import g_oConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import g_oRefresher
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR

class ConfigurationClient:
  
  def __init__( self, lFilesToLoad = [] ):
    for sFile in lFilesToLoad:
      g_oConfigurationData.loadFile( sFile )
      
  def loadFile( self, sFileName ):
    g_oConfigurationData.loadFile( sFileName )
    
  def getOption( self, sPath ):
    g_oRefresher.refreshConfigurationIfNeeded()
    sValue = g_oConfigurationData.extractOptionFromCFG( sPath )
    if sValue:
      return S_OK( sValue )
    else:
      return S_ERROR( "Path does not exist" )
    
  def getSections( self, sPath ):
    g_oRefresher.refreshConfigurationIfNeeded()
    lValue = g_oConfigurationData.getSectionsFromCFG( sPath )
    if lValue:
      return S_OK( lValue )
    else:
      return S_ERROR( "Path does not exist" )
    
  def getOptions( self, sPath ):
    g_oRefresher.refreshConfigurationIfNeeded()
    lValue = g_oConfigurationData.getOptionsFromCFG( sPath )
    if lValue:
      return S_OK( lValue )
    else:
      return S_ERROR( "Path does not exist" )
    
    
gConfig = ConfigurationClient()
    
if __name__=="__main__":
  gConfig.loadFile( "test.cfg" )
  gConfig.dumpLocalCFGToFile( "dump.cfg" )  
    
