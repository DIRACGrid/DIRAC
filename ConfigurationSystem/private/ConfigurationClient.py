# $HeadURL$
__RCSID__ = "$Id$"

import types
import os
import DIRAC
from DIRAC.Core.Utilities import List
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.private.Refresher import gRefresher
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.Logger import gLogger

class ConfigurationClient:

  def __init__( self, fileToLoadList = None ):
    self.diracConfigFilePath = os.path.join( DIRAC.rootPath, "etc", "dirac.cfg" )
    if fileToLoadList and type( fileToLoadList ) == types.ListType:
      for fileName in fileToLoadList:
        gConfigurationData.loadFile( fileName )

  def loadFile( self, fileName ):
    return gConfigurationData.loadFile( fileName )

  def loadCFG( self, cfg ):
    return gConfigurationData.mergeWithLocal( cfg )

  def forceRefresh( self ):
    return gRefresher.forceRefresh()

  def dumpLocalCFGToFile( self, fileName ):
    return gConfigurationData.dumpLocalCFGToFile( fileName )

  def dumpRemoteCFGToFile( self, fileName ):
    return gConfigurationData.dumpRemoteCFGToFile( fileName )

  def addListenerToNewVersionEvent( self, functor ):
    gRefresher.addListenerToNewVersionEvent( functor )

  def dumpCFGAsLocalCache( self, fileName = None, raw = False ):
    cfg = gConfigurationData.mergedCFG.clone()
    try:
      if not raw and cfg.isSection( 'DIRAC' ):
        diracSec = cfg[ 'DIRAC' ]
        if diracSec.isSection( 'Configuration' ):
          confSec = diracSec[ 'Configuration' ]
          for opt in ( 'Servers', 'MasterServer' ):
            if confSec.isOption( opt ):
              confSec.deleteKey( opt )
      strData = str( cfg )
      if fileName:
        fd = open( fileName, "w" )
        fd.write( strData )
        fd.close()
    except Exception, e:
      return S_ERROR( "Can't write to file %s: %s" % ( fileName, str( e ) ) )
    return S_OK( strData )

  def getServersList( self ):
    return gConfigurationData.getServers()

  def useServerCertificate( self ):
    return gConfigurationData.useServerCertificate()

  # FIXME: to be removed
  def _useServerCertificate( self ):
    return gConfigurationData.useServerCertificate()

  def getValue( self, optionPath, defaultValue = None ):
    retVal = self.getOption( optionPath, defaultValue )
    if retVal[ 'OK' ]:
      return retVal[ 'Value' ]
    else:
      return defaultValue

#  def getSpecialValue( self, optionPath, defaultValue = None, vo = None, setup = None ):
#    """ Get a configuration option value for a specific vo and setup
#    """
#    voName = vo
#    if not vo:
#      voName = getVO()
#    setupName = setup
#    if not setup:
#      setupName = self.getValue( '/DIRAC/Setup', '' )
#
#    # Get the most specific defined value now
#    section = optionPath.split( '/' )[1]
#    oPath = '/'.join( optionPath.split( '/' )[2:] )
#    if voName:
#      if setupName:
#        value = self.getValue( section + '/' + voName + '/' + setupName + oPath, 'NotDefined' )
#        if value != 'NotDefined':
#          return value
#      value = self.getValue( section + '/' + voName + oPath, 'NotDefined' )
#      if value != 'NotDefined':
#        return value
#    value = self.getValue( optionPath, defaultValue )
#    return value

  def getOption( self, optionPath, typeValue = None ):
    gRefresher.refreshConfigurationIfNeeded()
    optionValue = gConfigurationData.extractOptionFromCFG( optionPath )

    if optionValue == None:
      return S_ERROR( "Path %s does not exist or it's not an option" % optionPath )

    #Value has been returned from the configuration
    if typeValue == None:
      return S_OK( optionValue )

    #Casting to typeValue's type
    requestedType = typeValue
    if not type( typeValue ) == types.TypeType:
      requestedType = type( typeValue )

    if requestedType == types.ListType:
      try:
        return S_OK( List.fromChar( optionValue, ',' ) )
      except Exception:
        return S_ERROR( "Can't convert value (%s) to comma separated list" % str( optionValue ) )
    elif requestedType == types.BooleanType:
      try:
        return S_OK( optionValue.lower() in ( "y", "yes", "true", "1" ) )
      except Exception:
        return S_ERROR( "Can't convert value (%s) to Boolean" % str( optionValue ) )
    else:
      try:
        return S_OK( requestedType( optionValue ) )
      except:
        return S_ERROR( "Type mismatch between default (%s) and configured value (%s) " % ( str( typeValue ), optionValue ) )


  def getSections( self, sectionPath, listOrdered = True ):
    gRefresher.refreshConfigurationIfNeeded()
    sectionList = gConfigurationData.getSectionsFromCFG( sectionPath, ordered = listOrdered )
    if type( sectionList ) == types.ListType:
      return S_OK( sectionList )
    else:
      return S_ERROR( "Path %s does not exist or it's not a section" % sectionPath )

  def getOptions( self, sectionPath, listOrdered = True ):
    gRefresher.refreshConfigurationIfNeeded()
    optionList = gConfigurationData.getOptionsFromCFG( sectionPath, ordered = listOrdered )
    if type( optionList ) == types.ListType:
      return S_OK( optionList )
    else:
      return S_ERROR( "Path %s does not exist or it's not a section" % sectionPath )

  def getOptionsDict( self, sectionPath ):
    gRefresher.refreshConfigurationIfNeeded()
    optionsDict = {}
    optionList = gConfigurationData.getOptionsFromCFG( sectionPath )
    if type( optionList ) == types.ListType:
      for option in optionList:
        optionsDict[ option ] = gConfigurationData.extractOptionFromCFG( "%s/%s" %
                                                              ( sectionPath, option ) )
      return S_OK( optionsDict )
    else:
      return S_ERROR( "Path %s does not exist or it's not a section" % sectionPath )

  def setOptionValue( self, optionPath, value ):
    """
    Set a value in the local configuration
    """
    gConfigurationData.setOptionInCFG( optionPath, value )
