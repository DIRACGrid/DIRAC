# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/ConfigurationClient.py,v 1.1 2007/03/16 11:57:33 rgracian Exp $
__RCSID__ = "$Id: ConfigurationClient.py,v 1.1 2007/03/16 11:57:33 rgracian Exp $"

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.LoggingSystem.Client.Logger import gLogger


class ConfigurationClient:
  
  def __init__( self, fileToLoadList = [] ):
    for fileName in fileToLoadList:
      gConfigurationData.loadFile( fileName )
      
  def loadFile( self, fileName ):
    return gConfigurationData.loadFile( fileName )
    
  def dumpLocalCFGToFile( self, fileName ):
    return gConfigurationData.dumpLocalCFGToFile( fileName )
    
  def getOption( self, optionPath ):
    gRefresher.refreshConfigurationIfNeeded()
    optionValue = gConfigurationData.extractOptionFromCFG( optionPath )
    if optionValue:
      return S_OK( optionValue )
    else:
      return S_ERROR( "Path does not exist" )
    
  def getSections( self, sectionPath ):
    gRefresher.refreshConfigurationIfNeeded()
    sectionList = gConfigurationData.getSectionsFromCFG( sectionPath )
    if sectionList:
      return S_OK( sectionList )
    else:
      return S_ERROR( "Path does not exist" )
    
  def getOptions( self, sectionPath ):
    gRefresher.refreshConfigurationIfNeeded()
    optionList = gConfigurationData.getOptionsFromCFG( sectionPath )
    if optionList:
      return S_OK( optionList )
    else:
      return S_ERROR( "Path does not exist" )
