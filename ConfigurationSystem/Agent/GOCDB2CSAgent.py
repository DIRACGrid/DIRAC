# $HeadURL$
"""
Module provides GOCDB2CSAgent functionality.
"""

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACGOCDictionary
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Config import gConfig

__RCSID__ = "$Id: $"

class GOCDB2CSAgent ( AgentModule ):
  """
  Class to retrieve information about service endpoints
  from GOCDB and update configuration stored by CS
  """

  def initialize( self ):

    # client to connect to GOCDB
    self.GOCDBClient = GOCDBClient()

    # API needed to update configuration stored by CS
    self.csAPI = CSAPI()
    return self.csAPI.initialize()

  def execute( self ):
    '''
    Execute GOCDB queries according to the function map
    and user request (options in configuration).
    '''

    # __functionMap is at the end of the class definition
    for option, functionCall in GOCDB2CSAgent.__functionMap.iteritems():
      optionValue = self.am_getOption( option, True )
      if optionValue:
        result = functionCall( self )
        if not result['OK']:
          gLogger.error( "%s() failed with message: %s" % ( functionCall.__name__, result['Message'] ) )
        else:
          gLogger.info( "Successfully executed %s" % functionCall.__name__ )

    return S_OK()

  def updatePerfSONARConfiguration( self ):
    '''
    Get current status of perfSONAR endpoints from GOCDB
    and update CS configuration accordingly.
    '''
    __functionName = '[updatePerfSONAREndpoints]'
    gLogger.debug( __functionName, 'Begin function ...' )

    # get endpoints
    result = self.__getPerfSONAREndpoints()
    if not result['OK']:
      gLogger.error( __functionName, "__getPerfSONAREndpoints() failed with message: %s" % result['Message'] )
      return S_ERROR( 'Unable to fetch perfSONAR endpoints from GOCDB.' )
    endpointList = result['Value']

    # add DIRAC site name
    result = self.__addDIRACSiteName( endpointList )
    if not result['OK']:
      gLogger.error( __functionName, "__addDIRACSiteName() failed with message: %s" % result['Message'] )
      return S_ERROR( 'Unable to extend the list with DIRAC site names.' )
    extendedEndpointList = result['Value']

    # prepare dictionary with new configuration
    result = self.__preparePerfSONARConfiguration( extendedEndpointList )
    if not result['OK']:
      gLogger.error( __functionName, "__preparePerfSONARConfiguration() failed with message: %s" % result['Message'] )
      return S_ERROR( 'Unable to prepare a new perfSONAR configuration.' )
    finalConfiguration = result['Value']

    # update configuration according to the final status of endpoints
    self.__updateConfiguration( finalConfiguration )
    gLogger.debug( __functionName, "Configuration updated succesfully" )

    gLogger.debug( __functionName, 'End function.' )
    return S_OK()

  def __getPerfSONAREndpoints( self ):
    '''
    Retrieve perfSONAR endpoint information directly form GOCDB.

    :return: List of perfSONAR endpoints (dictionaries) as stored by GOCDB.
    '''
    __functionName = '[__getPerfSONAREndpoints]'
    gLogger.debug( __functionName, 'Begin function ...' )

    # get perfSONAR endpoints (latency and bandwidth) form GOCDB
    endpointList = []
    for endpointType in ['Latency', 'Bandwidth']:
      result = self.GOCDBClient.getServiceEndpointInfo( 'service_type', 'net.perfSONAR.%s' % endpointType )

      if not result['OK']:
        gLogger.error( __functionName, "getServiceEndpointInfo() failed with message: %s" % result['Message'] )
        return S_ERROR( 'Could not fetch %s endpoints from GOCDB' % endpointType.lower() )

      gLogger.debug( __functionName, 'Number of %s endpoints: %s' % ( endpointType.lower(), len( result['Value'] ) ) )
      endpointList.extend( result['Value'] )

    gLogger.debug( __functionName, 'Number of perfSONAR endpoints: %s' % len( endpointList ) )
    gLogger.debug( __functionName, 'End function.' )
    return S_OK( endpointList )

  def __preparePerfSONARConfiguration( self, endpointList ):
    '''
    Prepare a dictionary with a new CS configuration of perfSONAR endpoints.

    :return: Dictionary where keys are configuration paths (options and sections)
             and values are values of corresponding options
             or None in case of a path pointing to a section.
    '''

    __functionName = '[__preparePerfSONARConfiguration]'
    gLogger.debug( __functionName, 'Begin function ...' )

    # static elements of path
    rootPath = '/Resources/Sites'
    extPath = 'Network/perfSONAR'
    optionName = 'Enabled'

    # enable GOCDB endpoints in configuration
    newConfiguration = {}
    for endpoint in endpointList:
      if endpoint['DIRACSITENAME'] is None:
        continue

      split = endpoint['DIRACSITENAME'].split( '.' )
      path = cfgPath( rootPath, split[0], endpoint['DIRACSITENAME'], extPath, endpoint['HOSTNAME'], optionName )
      newConfiguration[path] = 'True'

    # get current configuration
    result = gConfig.getConfigurationTree( rootPath, extPath + '/', '/' + optionName )
    if not result['OK']:
      gLogger.error( __functionName, "getConfigurationTree() failed with message: %s" % result['Message'] )
      return S_ERROR( 'Unable to fetch perfSONAR endpoints from CS.' )
    currentConfiguration = result['Value']

    # disable endpoints that disappeared in GOCDB
    removedElements = set( currentConfiguration ) - set( newConfiguration )
    newElements = set( newConfiguration ) - set( currentConfiguration )
    for path in removedElements:
      newConfiguration[path] = 'False'

    # inform what will be changed
    if len( newElements ) > 0:
      gLogger.info( "%s new perfSONAR endpoints will be added to the configuration" % len( newElements ) )

    if len( removedElements ) > 0:
      gLogger.info( "%s old perfSONAR endpoints will be disable in the configuration" % len( removedElements ) )

    gLogger.debug( __functionName, 'End function.' )
    return S_OK( newConfiguration )

  def __addDIRACSiteName( self, inputList ):
    '''
    Extend given list of GOCDB endpoints with DIRAC site name, i.e.
    add an entry "DIRACSITENAME" in dictionaries that describe endpoints.


    :return: List of perfSONAR endpoints (dictionary).
    '''
    __functionName = '[__addDIRACSiteName]'
    gLogger.debug( __functionName, 'Begin function ...' )

    # get site name dictionary
    result = getDIRACGOCDictionary()
    if not result['OK']:
      gLogger.error( __functionName, "getDIRACGOCDictionary() failed with message: %s" % result['Message'] )
      return S_ERROR( 'Could not get site name dictionary' )

    # reverse the dictionary (assume 1 to 1 relation)
    DIRACGOCDict = result['Value']
    GOCDIRACDict = dict( zip( DIRACGOCDict.values(), DIRACGOCDict.keys() ) )

    # add DIRAC site names
    outputList = []
    for entry in inputList:
      try:
        entry['DIRACSITENAME'] = GOCDIRACDict[entry['SITENAME']]
      except KeyError:
          gLogger.warn( __functionName, "No dictionary entry for %s. " % entry['SITENAME'] )
          entry['DIRACSITENAME'] = None
      outputList.append( entry )

    gLogger.debug( __functionName, 'End function.' )
    return S_OK( outputList )

  def __updateConfiguration( self, setElements = {}, delElements = [] ):
    '''
    Update configuration stored by CS.
    '''

    __functionName = '[__updateConfigurationInCS]'
    gLogger.debug( __functionName, 'Begin function ...' )

    # assure existence and proper value of a section or an option
    for path, value in setElements.iteritems():

      if value is None:
        section = path
      else:
        split = path.rsplit( '/', 1 )
        section = split[0]

      result = self.csAPI.createSection( section )
      if not result['OK']:
        gLogger.error( __functionName, "createSection() failed with message: %s" % result['Message'] )

      if value is not None:
        result = self.csAPI.setOption( path, value )
        if not result['OK']:
          gLogger.error( __functionName, "setOption() failed with message: %s" % result['Message'] )

    # delete elements in configuration
    for path in delElements:
      result = self.csAPI.delOption( path )
      if not result['OK']:
        gLogger.warn( __functionName, "csAPI.delOption() failed with message: %s" % result['Message'] )

        result = self.csAPI.delSection( path )
        if not result['OK']:
          gLogger.warn( __functionName, "csAPI.delSection() failed with message: %s" % result['Message'] )

    # update configuration stored by CS
    result = self.csAPI.commit()
    if not result['OK']:
      gLogger.error( "csAPI.commit() failed with message: %s" % result['Message'] )
      return S_ERROR( 'Could not commit changes to CS.' )

    gLogger.debug( __functionName, 'End function.' )
    return S_OK()


  # define mapping between agent option in CS and functionCall
  __functionMap = {
                    'UpdatePerfSONARS': updatePerfSONARConfiguration,

                  }

