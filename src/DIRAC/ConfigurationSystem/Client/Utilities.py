########################################################################
# Author : Andrei Tsaregorodtsev
########################################################################
"""
  Utilities for managing DIRAC configuration:
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import socket
import six

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs, getVOOption
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACSiteName
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection
from DIRAC.Core.Utilities.Glue2 import getGlue2CEInfo
from DIRAC.Core.Utilities.SiteSEMapping import getSEHosts
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers


def getGridVOs():
  """ Get all the VOMS VO names served by this DIRAC service

      :return: S_OK(list)/S_ERROR()
  """
  voNames = []
  result = getVOs()
  if not result['OK']:
    return result
  else:
    vos = result['Value']
    for vo in vos:
      vomsVO = getVOOption(vo, "VOMSName")
      if vomsVO:
        voNames.append(vomsVO)
  return S_OK(voNames)


def getGridCEs(vo, bdiiInfo=None, ceBlackList=None, hostURL=None):
  """ Get all the CEs available for a given VO and having queues in Production state

      :param str vo: VO name
      :param dict bddiInfo: information from BDII
      :param list ceBlackList: CEs from black list
      :param str hostURL: host URL

      :return: Dictionary with keys: OK, Value, BdiiInfo, UnknownCEs
  """
  knownCEs = set()
  cesInInformation = set()
  if ceBlackList is not None:
    knownCEs = knownCEs.union(set(ceBlackList))

  ceBdiiDict = bdiiInfo
  if bdiiInfo is None:
    result = getGlue2CEInfo(vo, host=hostURL)
    if not result['OK']:
      return result
    ceBdiiDict = result['Value']

  siteDict = {}
  for site in ceBdiiDict:
    siteCEs = set(ceBdiiDict[site]['CEs'].keys())
    cesInInformation.update(siteCEs)
    newCEs = siteCEs - knownCEs
    if not newCEs:
      continue

    ceFullDict = {}
    for ce in newCEs:
      ceDict = {}
      ceInfo = ceBdiiDict[site]['CEs'][ce]
      ceType = 'Unknown'
      ceDict['Queues'] = []
      for queue in ceInfo['Queues']:
        queueStatus = ceInfo['Queues'][queue].get('GlueCEStateStatus', 'UnknownStatus')
        if 'production' in queueStatus.lower():
          ceType = ceInfo['Queues'][queue].get('GlueCEImplementationName', '')
          ceDict['Queues'].append(queue)
      if not ceDict['Queues']:
        continue

      ceDict['CEType'] = ceType
      ceDict['GOCSite'] = site
      ceDict['CEID'] = ce
      systemName = ceInfo.get('GlueHostOperatingSystemName', 'Unknown')
      systemVersion = ceInfo.get('GlueHostOperatingSystemVersion', 'Unknown')
      systemRelease = ceInfo.get('GlueHostOperatingSystemRelease', 'Unknown')
      ceDict['System'] = (systemName, systemVersion, systemRelease)

      ceFullDict[ce] = ceDict

    siteDict[site] = ceFullDict

  result = S_OK(siteDict)
  result['BdiiInfo'] = ceBdiiDict

  unknownCEs = knownCEs - cesInInformation
  result['UnknownCEs'] = unknownCEs
  return result


def getSiteUpdates(vo, bdiiInfo=None, log=None):
  """ Get all the necessary updates for the already defined sites and CEs

      :param str vo: VO name
      :param dict bdiiInfo: information from DBII
      :param object log: logger

      :result: S_OK(set)/S_ERROR()
  """

  def addToChangeSet(entry, changeSet):
    """ Inner function to update changeSet with entry (a tuple)

        :param tuple entry: entry to add to changeSet
        :param set changeSet: set collecting stuff to change
    """
    _section, _option, value, new_value = entry
    if new_value and new_value != value:
      changeSet.add(entry)

  if log is None:
    log = gLogger

  ceBdiiDict = bdiiInfo
  if bdiiInfo is None:
    result = getGlue2CEInfo(vo)
    if not result['OK']:
      return result
    ceBdiiDict = result['Value']

  changeSet = set()
  for site in ceBdiiDict:
    result = getDIRACSiteName(site)
    if not result['OK']:
      continue
    siteNames = result['Value']
    for siteName in siteNames:
      siteSection = cfgPath('/Resources', 'Sites', siteName.split('.')[0], siteName)
      result = gConfig.getOptionsDict(siteSection)
      if not result['OK']:
        continue
      siteDict = result['Value']
      # Current CS values
      coor = siteDict.get('Coordinates', 'Unknown')
      mail = siteDict.get('Mail', 'Unknown').replace(' ', '')
      description = siteDict.get('Description', 'Unknown')
      description = description.replace(' ,', ',')

      longitude = ceBdiiDict[site].get('GlueSiteLongitude', '').strip()
      latitude = ceBdiiDict[site].get('GlueSiteLatitude', '').strip()

      # Current BDII value
      newcoor = ''
      if longitude and latitude:
        newcoor = "%s:%s" % (longitude, latitude)
      newmail = ceBdiiDict[site].get('GlueSiteSysAdminContact', '').replace('mailto:', '').strip()
      newdescription = ceBdiiDict[site].get('GlueSiteDescription', '').strip()
      newdescription = ", ".join([line.strip() for line in newdescription.split(",")])

      # Adding site data to the changes list
      addToChangeSet((siteSection, 'Coordinates', coor, newcoor), changeSet)
      addToChangeSet((siteSection, 'Mail', mail, newmail), changeSet)
      addToChangeSet((siteSection, 'Description', description, newdescription), changeSet)

      ces = gConfig.getSections(cfgPath(siteSection, 'CEs'))
      for ce in ces.get('Value', []):
        ceSection = cfgPath(siteSection, 'CEs', ce)
        ceDict = {}
        result = gConfig.getOptionsDict(ceSection)
        if result['OK']:
          ceDict = result['Value']
        else:
          if ceBdiiDict[site]['CEs'].get(ce, None):
            log.notice("Adding new CE", "%s to site %s/%s" % (ce, siteName, site))
        ceInfo = ceBdiiDict[site]['CEs'].get(ce, None)
        if ceInfo is None:
          ceType = ceDict.get('CEType', '')
          continue

        # Current CS CE info
        arch = ceDict.get('architecture', 'Unknown')
        OS = ceDict.get('OS', 'Unknown')
        si00 = ceDict.get('SI00', 'Unknown')
        ceType = ceDict.get('CEType', 'Unknown')
        ram = ceDict.get('MaxRAM', 'Unknown')
        submissionMode = ceDict.get('SubmissionMode', 'Unknown')

        # Current BDII CE info
        newarch = ceBdiiDict[site]['CEs'][ce].get('GlueHostArchitecturePlatformType', '').strip()
        systemName = ceInfo.get('GlueHostOperatingSystemName', '').strip()
        systemVersion = ceInfo.get('GlueHostOperatingSystemVersion', '').strip()
        systemRelease = ceInfo.get('GlueHostOperatingSystemRelease', '').strip()
        newOS = ''
        if systemName and systemVersion and systemRelease:
          newOS = '_'.join((systemName, systemVersion, systemRelease))
        newsi00 = ceInfo.get('GlueHostBenchmarkSI00', '').strip()
        newCEType = 'Unknown'
        for queue in ceInfo['Queues']:
          queueDict = ceInfo['Queues'][queue]
          newCEType = queueDict.get('GlueCEImplementationName', '').strip()
          if newCEType:
            break
        if newCEType == 'ARC-CE':
          newCEType = 'ARC'

        newSubmissionMode = None
        if newCEType in ['ARC', 'CREAM']:
          newSubmissionMode = "Direct"
        newRAM = ceInfo.get('GlueHostMainMemoryRAMSize', '').strip()
        # Protect from unreasonable values
        if newRAM and int(newRAM) > 150000:
          newRAM = ''

        # Adding CE data to the change list
        addToChangeSet((ceSection, 'architecture', arch, newarch), changeSet)
        addToChangeSet((ceSection, 'OS', OS, newOS), changeSet)
        addToChangeSet((ceSection, 'SI00', si00, newsi00), changeSet)
        addToChangeSet((ceSection, 'CEType', ceType, newCEType), changeSet)
        addToChangeSet((ceSection, 'MaxRAM', ram, newRAM), changeSet)
        if submissionMode == "Unknown" and newSubmissionMode:
          addToChangeSet((ceSection, 'SubmissionMode', submissionMode, newSubmissionMode), changeSet)

        for queue, queueInfo in ceInfo['Queues'].items():
          queueStatus = queueInfo['GlueCEStateStatus']
          queueSection = cfgPath(ceSection, 'Queues', queue)
          queueDict = {}
          result = gConfig.getOptionsDict(queueSection)
          if result['OK']:
            queueDict = result['Value']
          else:
            if queueStatus.lower() == "production":
              log.notice("Adding new queue", "%s to CE %s" % (queue, ce))
            else:
              continue

          # Current CS queue info
          maxCPUTime = queueDict.get('maxCPUTime', 'Unknown')
          si00 = queueDict.get('SI00', 'Unknown')
          maxTotalJobs = queueDict.get('MaxTotalJobs', 'Unknown')

          # Current BDII queue info
          newMaxCPUTime = queueInfo.get('GlueCEPolicyMaxCPUTime', '')
          if newMaxCPUTime == "4" * len(newMaxCPUTime) or newMaxCPUTime == "9" * len(newMaxCPUTime):
            newMaxCPUTime = ''
          wallTime = queueInfo.get('GlueCEPolicyMaxWallClockTime', '')
          if wallTime == "4" * len(wallTime) or wallTime == "9" * len(wallTime):
            wallTime = ''
          if wallTime and int(wallTime) > 0:
            if not newMaxCPUTime:
              newMaxCPUTime = str(int(0.8 * int(wallTime)))
            else:
              if int(wallTime) <= int(newMaxCPUTime):
                newMaxCPUTime = str(int(0.8 * int(wallTime)))
          newSI00 = ''
          caps = queueInfo.get('GlueCECapability', [])
          if isinstance(caps, six.string_types):
            caps = [caps]
          for cap in caps:
            if 'CPUScalingReferenceSI00' in cap:
              newSI00 = cap.split('=')[-1]

          # tags, processors, localCEType
          tag = queueDict.get('Tag', '')
          # LocalCEType can be empty (equivalent to "InProcess")
          # or "Pool", "Singularity", but also "Pool/Singularity"
          localCEType = queueDict.get('LocalCEType', '')
          try:
            localCEType_inner = localCEType.split('/')[1]
          except IndexError:
            localCEType_inner = ''

          numberOfProcessors = int(queueDict.get('NumberOfProcessors', 0))
          newNOP = int(queueInfo.get('NumberOfProcessors', 1))

          # Adding queue info to the CS
          addToChangeSet((queueSection, 'maxCPUTime', maxCPUTime, newMaxCPUTime), changeSet)
          addToChangeSet((queueSection, 'SI00', si00, newSI00), changeSet)
          if newNOP != numberOfProcessors:
            addToChangeSet((queueSection, 'NumberOfProcessors', numberOfProcessors, newNOP), changeSet)
            if newNOP > 1:
              # if larger than one, add MultiProcessor to site tags, and LocalCEType=Pool
              newTag = ','.join(sorted(set(tag.split(',')).union({'MultiProcessor'}))).strip(',')
              addToChangeSet((queueSection, 'Tag', tag, newTag), changeSet)
              if localCEType_inner:
                newLocalCEType = 'Pool/' + localCEType_inner
              else:
                newLocalCEType = 'Pool'
              addToChangeSet((queueSection, 'LocalCEType', localCEType, newLocalCEType), changeSet)
            else:
              # if not larger than one, drop MultiProcessor Tag.
              # Here we do not change the LocalCEType as Pool CE would still be perfectly valid.
              newTag = ','.join(sorted(set(tag.split(',')).difference({'MultiProcessor'}))).strip(',')
              changeSet.add((queueSection, 'Tag', tag, newTag))
          if maxTotalJobs == "Unknown":
            newTotalJobs = min(1000, int(int(queueInfo.get('GlueCEInfoTotalCPUs', 0)) / 2))
            newWaitingJobs = max(2, int(newTotalJobs * 0.1))
            newTotalJobs = str(newTotalJobs)
            newWaitingJobs = str(newWaitingJobs)
            addToChangeSet((queueSection, 'MaxTotalJobs', '', newTotalJobs), changeSet)
            addToChangeSet((queueSection, 'MaxWaitingJobs', '', newWaitingJobs), changeSet)

          # Updating eligible VO list
          VOs = set()
          if queueDict.get('VO', ''):
            VOs = set([q.strip() for q in queueDict.get('VO', '').split(',') if q])
          if vo not in VOs:
            VOs.add(vo)
            VOs = list(VOs)
            newVOs = ','.join(VOs)
            addToChangeSet((queueSection, 'VO', '', newVOs), changeSet)

  return S_OK(changeSet)


def getDBParameters(fullname):
  """ Retrieve Database parameters from CS

      :param str fullname: should be of the form <System>/<DBname>
             defaultHost is the host to return if the option is not found in the CS.
             Not used as the method will fail if it cannot be found
             defaultPort is the port to return if the option is not found in the CS
             defaultUser is the user to return if the option is not found in the CS.
             Not usePassword is the password to return if the option is not found in the CS.
             Not used as the method will fail if it cannot be found
             defaultDB is the db to return if the option is not found in the CS.
             Not used as the method will fail if it cannot be found
             defaultQueueSize is the QueueSize to return if the option is not found in the CS

      :return: S_OK(dict)/S_ERROR() - dictionary with the keys: 'host', 'port', 'user', 'password',
                                      'db' and 'queueSize'
  """

  cs_path = getDatabaseSection(fullname)
  parameters = {}

  result = gConfig.getOption(cs_path + '/Host')
  if not result['OK']:
    # No host name found, try at the common place
    result = gConfig.getOption('/Systems/Databases/Host')
    if not result['OK']:
      return S_ERROR('Failed to get the configuration parameter: Host')
  dbHost = result['Value']
  # Check if the host is the local one and then set it to 'localhost' to use
  # a socket connection
  if dbHost != 'localhost':
    localHostName = socket.getfqdn()
    if localHostName == dbHost:
      dbHost = 'localhost'
  parameters['Host'] = dbHost

  # Mysql standard
  dbPort = 3306
  result = gConfig.getOption(cs_path + '/Port')
  if not result['OK']:
    # No individual port number found, try at the common place
    result = gConfig.getOption('/Systems/Databases/Port')
    if result['OK']:
      dbPort = int(result['Value'])
  else:
    dbPort = int(result['Value'])
  parameters['Port'] = dbPort

  result = gConfig.getOption(cs_path + '/User')
  if not result['OK']:
    # No individual user name found, try at the common place
    result = gConfig.getOption('/Systems/Databases/User')
    if not result['OK']:
      return S_ERROR('Failed to get the configuration parameter: User')
  dbUser = result['Value']
  parameters['User'] = dbUser

  result = gConfig.getOption(cs_path + '/Password')
  if not result['OK']:
    # No individual password found, try at the common place
    result = gConfig.getOption('/Systems/Databases/Password')
    if not result['OK']:
      return S_ERROR('Failed to get the configuration parameter: Password')
  dbPass = result['Value']
  parameters['Password'] = dbPass

  result = gConfig.getOption(cs_path + '/DBName')
  if not result['OK']:
    return S_ERROR('Failed to get the configuration parameter: DBName')
  dbName = result['Value']
  parameters['DBName'] = dbName

  return S_OK(parameters)


def getElasticDBParameters(fullname):
  """ Retrieve Database parameters from CS

      :param str fullname: should be of the form <System>/<DBname>

      :return: S_OK(dict)/S_ERROR()
  """

  cs_path = getDatabaseSection(fullname)
  parameters = {}

  result = gConfig.getOption(cs_path + '/Host')
  if not result['OK']:
    # No host name found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/Host')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: Host. Using localhost")
      dbHost = 'localhost'
    else:
      dbHost = result['Value']
  else:
    dbHost = result['Value']
  # Check if the host is the local one and then set it to 'localhost' to use
  # a socket connection
  if dbHost != 'localhost':
    localHostName = socket.getfqdn()
    if localHostName == dbHost:
      dbHost = 'localhost'
  parameters['Host'] = dbHost

  # Elasticsearch standard port
  result = gConfig.getOption(cs_path + '/Port')
  if not result['OK']:
    # No individual port number found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/Port')
    if not result['OK']:
      gLogger.warn("No configuration parameter set for Port, assuming URL points to right location")
      dbPort = None
    else:
      dbPort = int(result['Value'])
  else:
    dbPort = int(result['Value'])
  parameters['Port'] = dbPort

  result = gConfig.getOption(cs_path + '/User')
  if not result['OK']:
    # No individual user name found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/User')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: User. Assuming no user/password is provided/needed")
      dbUser = None
    else:
      dbUser = result['Value']
  else:
    dbUser = result['Value']
  parameters['User'] = dbUser

  result = gConfig.getOption(cs_path + '/Password')
  if not result['OK']:
    # No individual password found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/Password')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: Password. Assuming no user/password is provided/needed")
      dbPass = None
    else:
      dbPass = result['Value']
  else:
    dbPass = result['Value']
  parameters['Password'] = dbPass

  result = gConfig.getOption(cs_path + '/SSL')
  if not result['OK']:
    # No SSL option found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/SSL')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: SSL. Assuming SSL is needed")
      ssl = True
    else:
      ssl = False if result['Value'].lower() in ('false', 'no', 'n') else True
  else:
    ssl = False if result['Value'].lower() in ('false', 'no', 'n') else True
  parameters['SSL'] = ssl

  # Elasticsearch use certs
  result = gConfig.getOption(cs_path + '/CRT')
  if not result['OK']:
    # No CRT option found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/CRT')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: CRT. Using False")
      certs = False
    else:
      certs = result['Value']
  else:
    certs = result['Value']
  parameters['CRT'] = certs

  # Elasticsearch ca_certs
  result = gConfig.getOption(cs_path + '/ca_certs')
  if not result['OK']:
    # No CA certificate found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/ca_certs')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: ca_certs. Using None")
      ca_certs = None
    else:
      ca_certs = result['Value']
  else:
    ca_certs = result['Value']
  parameters['ca_certs'] = ca_certs

  # Elasticsearch client_key
  result = gConfig.getOption(cs_path + '/client_key')
  if not result['OK']:
    # No client private key found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/client_key')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: client_key. Using None")
      client_key = None
    else:
      client_key = result['Value']
  else:
    client_key = result['Value']
  parameters['client_key'] = client_key

  # Elasticsearch client_cert
  result = gConfig.getOption(cs_path + '/client_cert')
  if not result['OK']:
    # No cient certificate found, try at the common place
    result = gConfig.getOption('/Systems/NoSQLDatabases/client_cert')
    if not result['OK']:
      gLogger.warn("Failed to get the configuration parameter: client_cert. Using None")
      client_cert = None
    else:
      client_cert = result['Value']
  else:
    client_cert = result['Value']
  parameters['client_cert'] = client_cert

  return S_OK(parameters)


def getOAuthAPI(instance='Production'):
  """ Get OAuth API url

      :param str instance: instance

      :return: str
  """
  return gConfig.getValue("/Systems/Framework/%s/URLs/OAuthAPI" % instance)


def getDIRACGOCDictionary():
  """
  Create a dictionary containing DIRAC site names and GOCDB site names
  using a configuration provided by CS.

  :return:  A dictionary of DIRAC site names (key) and GOCDB site names (value).
  """

  log = gLogger.getSubLogger('getDIRACGOCDictionary')
  log.debug('Begin function ...')

  result = gConfig.getConfigurationTree('/Resources/Sites', 'Name')
  if not result['OK']:
    log.error("getConfigurationTree() failed with message: %s" % result['Message'])
    return S_ERROR('Configuration is corrupted')
  siteNamesTree = result['Value']

  dictionary = dict()
  PATHELEMENTS = 6  # site names have 6 elements in the path, i.e.:
  #    /Resource/Sites/<GRID NAME>/<DIRAC SITE NAME>/Name
  # [0]/[1]     /[2]  /[3]        /[4]              /[5]

  for path, gocdbSiteName in siteNamesTree.items():  # can be an iterator
    elements = path.split('/')
    if len(elements) != PATHELEMENTS:
      continue

    diracSiteName = elements[PATHELEMENTS - 2]
    dictionary[diracSiteName] = gocdbSiteName

  log.debug('End function.')
  return S_OK(dictionary)
