"""
This module provides GOCDB2CSAgent code.

The agent is used to synchronize information between GOCDB and DIRAC configuration System (CS)
Right now it only adds perfosonar endpoints

The following options can be set for the GOCDB2CSAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN GOCDB2CSAgent
  :end-before: ##END
  :dedent: 2
  :caption: GOCDB2CSAgent options
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
from DIRAC.ConfigurationSystem.Client.Utilities import getDIRACGOCDictionary
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Config import gConfig


class GOCDB2CSAgent (AgentModule):
  """ Class to retrieve information about service endpoints
      from GOCDB and update configuration stored by CS
  """

  def __init__(self, *args, **kwargs):
    """ c'tor
    """
    super(GOCDB2CSAgent, self).__init__(*args, **kwargs)
    self.GOCDBClient = None
    self.csAPI = None
    self.dryRun = False

  def initialize(self):
    """ Run at the agent initialization (normally every 500 cycles)
    """
    # client to connect to GOCDB
    self.GOCDBClient = GOCDBClient()
    self.dryRun = self.am_getOption('DryRun', self.dryRun)

    # API needed to update configuration stored by CS
    self.csAPI = CSAPI()
    return self.csAPI.initialize()

  def execute(self):
    """
    Execute GOCDB queries according to the function map
    and user request (options in configuration).
    """

    # __functionMap is at the end of the class definition
    for option, functionCall in GOCDB2CSAgent.__functionMap.items():
      optionValue = self.am_getOption(option, True)
      if optionValue:
        result = functionCall(self)
        if not result['OK']:
          self.log.error("%s() failed with message: %s" % (functionCall.__name__, result['Message']))
        else:
          self.log.info("Successfully executed %s" % functionCall.__name__)

    return S_OK()

  def updatePerfSONARConfiguration(self):
    """
    Get current status of perfSONAR endpoints from GOCDB
    and update CS configuration accordingly.
    """
    log = self.log.getSubLogger('updatePerfSONAREndpoints')
    log.debug('Begin function ...')

    # get endpoints
    result = self.__getPerfSONAREndpoints()
    if not result['OK']:
      log.error("__getPerfSONAREndpoints() failed with message: %s" % result['Message'])
      return S_ERROR('Unable to fetch perfSONAR endpoints from GOCDB.')
    endpointList = result['Value']

    # add DIRAC site name
    result = self.__addDIRACSiteName(endpointList)
    if not result['OK']:
      log.error("__addDIRACSiteName() failed with message: %s" % result['Message'])
      return S_ERROR('Unable to extend the list with DIRAC site names.')
    extendedEndpointList = result['Value']

    # prepare dictionary with new configuration
    result = self.__preparePerfSONARConfiguration(extendedEndpointList)
    if not result['OK']:
      log.error("__preparePerfSONARConfiguration() failed with message: %s" % result['Message'])
      return S_ERROR('Unable to prepare a new perfSONAR configuration.')
    finalConfiguration = result['Value']

    # update configuration according to the final status of endpoints
    self.__updateConfiguration(finalConfiguration)
    log.debug("Configuration updated succesfully")

    log.debug('End function.')
    return S_OK()

  def __getPerfSONAREndpoints(self):
    """
    Retrieve perfSONAR endpoint information directly from GOCDB.

    :return: List of perfSONAR endpoints (dictionaries) as stored by GOCDB.
    """

    log = self.log.getSubLogger('__getPerfSONAREndpoints')
    log.debug('Begin function ...')

    # get perfSONAR endpoints (latency and bandwidth) form GOCDB
    endpointList = []
    for endpointType in ['Latency', 'Bandwidth']:
      result = self.GOCDBClient.getServiceEndpointInfo('service_type', 'net.perfSONAR.%s' % endpointType)

      if not result['OK']:
        log.error("getServiceEndpointInfo() failed with message: %s" % result['Message'])
        return S_ERROR('Could not fetch %s endpoints from GOCDB' % endpointType.lower())

      log.debug('Number of %s endpoints: %s' % (endpointType.lower(), len(result['Value'])))
      endpointList.extend(result['Value'])

    log.debug('Number of perfSONAR endpoints: %s' % len(endpointList))
    log.debug('End function.')
    return S_OK(endpointList)

  def __preparePerfSONARConfiguration(self, endpointList):
    """
    Prepare a dictionary with a new CS configuration of perfSONAR endpoints.

    :return: Dictionary where keys are configuration paths (options and sections)
             and values are values of corresponding options
             or None in case of a path pointing to a section.
    """

    log = self.log.getSubLogger('__preparePerfSONARConfiguration')
    log.debug('Begin function ...')

    # static elements of a path
    rootPath = '/Resources/Sites'
    extPath = 'Network'
    baseOptionName = 'Enabled'
    options = {baseOptionName: 'True', 'ServiceType': 'perfSONAR'}

    # enable GOCDB endpoints in configuration
    newConfiguration = {}
    for endpoint in endpointList:
      if endpoint['DIRACSITENAME'] is None:
        continue

      split = endpoint['DIRACSITENAME'].split('.')
      path = cfgPath(rootPath, split[0], endpoint['DIRACSITENAME'], extPath, endpoint['HOSTNAME'])
      for name, defaultValue in options.items():
        newConfiguration[cfgPath(path, name)] = defaultValue

    # get current configuration
    currentConfiguration = {}
    for option in options:
      result = gConfig.getConfigurationTree(rootPath, extPath + '/', '/' + option)
      if not result['OK']:
        log.error("getConfigurationTree() failed with message: %s" % result['Message'])
        return S_ERROR('Unable to fetch perfSONAR endpoints from CS.')
      currentConfiguration.update(result['Value'])

    # disable endpoints that disappeared in GOCDB
    removedElements = set(currentConfiguration) - set(newConfiguration)
    newElements = set(newConfiguration) - set(currentConfiguration)

    addedEndpoints = int(len(newElements) / len(options))
    disabledEndpoints = 0
    for path in removedElements:
      if baseOptionName in path:
        newConfiguration[path] = 'False'
        if currentConfiguration[path] != 'False':
          disabledEndpoints = disabledEndpoints + 1

    # inform what will be changed
    if addedEndpoints > 0:
      self.log.info("%s new perfSONAR endpoints will be added to the configuration" % addedEndpoints)

    if disabledEndpoints > 0:
      self.log.info("%s old perfSONAR endpoints will be disable in the configuration" % disabledEndpoints)

    if addedEndpoints == 0 and disabledEndpoints == 0:
      self.log.info("perfSONAR configuration is up-to-date")

    log.debug('End function.')
    return S_OK(newConfiguration)

  def __addDIRACSiteName(self, inputList):
    """
    Extend given list of GOCDB endpoints with DIRAC site name, i.e.
    add an entry "DIRACSITENAME" in dictionaries that describe endpoints.
    If given site name could not be found "DIRACSITENAME" is set to 'None'.

    :return: List of perfSONAR endpoints (dictionaries).
    """

    log = self.log.getSubLogger('__addDIRACSiteName')
    log.debug('Begin function ...')

    # get site name dictionary
    result = getDIRACGOCDictionary()
    if not result['OK']:
      log.error("getDIRACGOCDictionary() failed with message: %s" % result['Message'])
      return S_ERROR('Could not get site name dictionary')

    # reverse the dictionary (assume 1 to 1 relation)
    DIRACGOCDict = result['Value']
    GOCDIRACDict = dict(zip(DIRACGOCDict.values(), DIRACGOCDict))

    # add DIRAC site names
    outputList = []
    for entry in inputList:
      try:
        entry['DIRACSITENAME'] = GOCDIRACDict[entry['SITENAME']]
      except KeyError:
        self.log.warn("No dictionary entry for %s. " % entry['SITENAME'])
        entry['DIRACSITENAME'] = None
      outputList.append(entry)

    log.debug('End function.')
    return S_OK(outputList)

  def __updateConfiguration(self, setElements=None, delElements=None):
    """
    Update configuration stored by CS.
    """
    if setElements is None:
      setElements = {}
    if delElements is None:
      delElements = []

    log = self.log.getSubLogger('__updateConfiguration')
    log.debug('Begin function ...')

    # assure existence and proper value of a section or an option
    for path, value in setElements.items():

      if value is None:
        section = path
      else:
        split = path.rsplit('/', 1)
        section = split[0]

      try:
        result = self.csAPI.createSection(section)
        if not result['OK']:
          log.error("createSection() failed with message: %s" % result['Message'])
      except Exception as e:
        log.error("Exception in createSection(): %s" % repr(e).replace(',)', ')'))

      if value is not None:
        try:
          result = self.csAPI.setOption(path, value)
          if not result['OK']:
            log.error("setOption() failed with message: %s" % result['Message'])
        except Exception as e:
          log.error("Exception in setOption(): %s" % repr(e).replace(',)', ')'))

    # delete elements in the configuration
    for path in delElements:
      result = self.csAPI.delOption(path)
      if not result['OK']:
        log.warn("delOption() failed with message: %s" % result['Message'])

        result = self.csAPI.delSection(path)
        if not result['OK']:
          log.warn("delSection() failed with message: %s" % result['Message'])

    if self.dryRun:
      log.info("Dry Run: CS won't be updated")
      self.csAPI.showDiff()
    else:
      # update configuration stored by CS
      result = self.csAPI.commit()
      if not result['OK']:
        log.error("commit() failed with message: %s" % result['Message'])
        return S_ERROR("Could not commit changes to CS.")
      else:
        log.info("Committed changes to CS")

    log.debug('End function.')
    return S_OK()

  # define mapping between an agent option in the configuration and a function call
  __functionMap = {'UpdatePerfSONARS': updatePerfSONARConfiguration,
                   }
