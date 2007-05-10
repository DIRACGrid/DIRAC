# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ConfigurationSystem/private/ConfigurationClient.py,v 1.3 2007/05/10 14:46:27 acasajus Exp $
__RCSID__ = "$Id: ConfigurationClient.py,v 1.3 2007/05/10 14:46:27 acasajus Exp $"

import types
from DIRAC.Core.Utilities import List
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

  def getValue( self, optionPath, defaultValue = None ):
    retVal = self.get( optionPath, defaultValue )
    if retVal[ 'OK' ]:
      return retVal[ 'Value' ]
    else:
      gLogger.error( "gConfig.getValue for invalid value", retVal[ 'Message' ] )
      return None

  def getOption( self, optionPath, defaultValue = None ):
    gRefresher.refreshConfigurationIfNeeded()
    optionValue = gConfigurationData.extractOptionFromCFG( optionPath )
    if not optionValue:
      optionValue = defaultValue

    #Return value if existing, defaultValue if not
    if optionValue == defaultValue:
      if defaultValue == None or type( defaultValue ) == types.TypeType:
        return S_ERROR( "Path %s does not exist or it's not an option" % optionValue )
      return S_OK( optionValue )

    #Value has been returned from the configuration
    if defaultValue == None:
      return S_OK( optionValue )

    #Casting to defaultValue's type
    defaultType = defaultValue
    if not type( defaultValue ) == types.TypeType:
      defaultType = type( defaultValue )

    if defaultType == types.ListType:
      try:
        return S_OK( List.fromChar( optionValue, ',' ) )
      except Exception, v:
        return S_ERROR( "Can't convert value (%s) to comma separated list" % str( optionValue ) )
    else:
      try:
        return S_OK( defaultType( optionValue ) )
      except:
        return S_ERROR( "Type mismatch between default (%s) and configured value (%s) " % ( str( defaultValue ), optionValue ) )


  def getSections( self, sectionPath ):
    gRefresher.refreshConfigurationIfNeeded()
    sectionList = gConfigurationData.getSectionsFromCFG( sectionPath )
    if sectionList:
      return S_OK( sectionList )
    else:
      return S_ERROR( "Path %s does not exist or it's not a section" % sectionPath )

  def getOptions( self, sectionPath ):
    gRefresher.refreshConfigurationIfNeeded()
    optionList = gConfigurationData.getOptionsFromCFG( sectionPath )
    if optionList:
      return S_OK( optionList )
    else:
      return S_ERROR( "Path %s does not exist or it's not a section" % sectionPath )
