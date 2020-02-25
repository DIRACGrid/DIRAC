########################################################################
# Author : Andrei Tsaregorodtsev
########################################################################
"""
  Utilities for managing DIRAC configuration:

  getCEsFromCS
  getUnusedGridCEs
  getUnusedGridSEs
  getSiteUpdates
  getSEUpdates
"""

__RCSID__ = "$Id$"

import six
import re
import socket
from urlparse import urlparse

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.Grid import getBdiiCEInfo, getBdiiSEInfo, ldapService
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName, getDIRACSesForHostName
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs, getVOOption
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection


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


def getCEsFromCS():
  """ Get all the CEs defined in the CS

      :return: S_OK(list)/S_ERROR()
  """
  knownCEs = []
  result = gConfig.getSections('/Resources/Sites')
  if not result['OK']:
    return result
  grids = result['Value']

  for grid in grids:
    result = gConfig.getSections('/Resources/Sites/%s' % grid)
    if not result['OK']:
      return result
    sites = result['Value']

    for site in sites:
      opt = gConfig.getOptionsDict('/Resources/Sites/%s/%s' % (grid, site))['Value']
      ces = List.fromChar(opt.get('CE', ''))
      knownCEs += ces

  return S_OK(knownCEs)


def getSEsFromCS(protocol='srm'):
  """ Get all the SEs defined in the CS

      :param str protocol: storage protocol

      :return: S_OK(dict)/S_ERROR()
  """
  knownSEs = {}
  result = gConfig.getSections('/Resources/StorageElements')
  if not result['OK']:
    return result
  ses = result['Value']
  for se in ses:
    seSection = '/Resources/StorageElements/%s' % se
    result = gConfig.getSections(seSection)
    if not result['OK']:
      continue
    accesses = result['Value']
    for access in accesses:
      seProtocol = gConfig.getValue(cfgPath(seSection, access, 'Protocol'), '')
      if seProtocol.lower() == protocol.lower() or protocol == 'any':
        host = gConfig.getValue(cfgPath(seSection, access, 'Host'), '')
        knownSEs.setdefault(host, [])
        knownSEs[host].append(se)
      else:
        continue

  return S_OK(knownSEs)


def getGridCEs(vo, bdiiInfo=None, ceBlackList=None, hostURL=None, glue2=False):
  """ Get all the CEs available for a given VO and having queues in Production state

      :param str vo: VO name
      :param dict bddiInfo: information from BDII
      :param list ceBlackList: CEs from black list
      :param str hostURL: host URL
      :param bool glue2: use glue2

      :return: S_OK(set)/S_ERROR()
  """
  knownCEs = set()
  if ceBlackList is not None:
    knownCEs = knownCEs.union(set(ceBlackList))

  ceBdiiDict = bdiiInfo
  if bdiiInfo is None:
    result = getBdiiCEInfo(vo, host=hostURL, glue2=glue2)
    if not result['OK']:
      return result
    ceBdiiDict = result['Value']

  siteDict = {}
  for site in ceBdiiDict:
    siteCEs = set(ceBdiiDict[site]['CEs'].keys())
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
  return result


def getSiteUpdates(vo, bdiiInfo=None, log=None):
  """ Get all the necessary updates for the already defined sites and CEs

      :param str vo: VO name
      :param dict bdiiInfo: information from DBII
      :param log: logger

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
    result = getBdiiCEInfo(vo)
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

      ces = gConfig.getValue(cfgPath(siteSection, 'CE'), [])
      for ce in ces:
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

        queues = ceInfo['Queues'].keys()
        for queue in queues:
          queueInfo = ceInfo['Queues'][queue]
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

          # Adding queue info to the CS
          addToChangeSet((queueSection, 'maxCPUTime', maxCPUTime, newMaxCPUTime), changeSet)
          addToChangeSet((queueSection, 'SI00', si00, newSI00), changeSet)
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


def getGridSEs(vo, bdiiInfo=None, seBlackList=None):
  """ Get all the SEs available for a given VO

      :param str vo: VO name
      :param dict bdiiInfo: information from BDII
      :param list seBlackList: SEs from black list

      :return: S_OK(dict)/S_ERROR()
  """
  seBdiiDict = bdiiInfo
  if bdiiInfo is None:
    result = getBdiiSEInfo(vo)
    if not result['OK']:
      return result
    seBdiiDict = result['Value']

  knownSEs = set()
  if seBlackList is not None:
    knownSEs = knownSEs.union(set(seBlackList))

  siteDict = {}
  for site in seBdiiDict:
    for gridSE in seBdiiDict[site]['SEs']:
      seDict = seBdiiDict[site]['SEs'][gridSE]

      # if "lhcb" in seDict['GlueSAName']:
      #  print '+'*80
      #  print gridSE
      #  for k,v in seDict.items():
      #    print k,'\t',v

      if gridSE not in knownSEs:
        siteDict.setdefault(site, {})
        if isinstance(seDict['GlueSAAccessControlBaseRule'], list):
          voList = [re.sub('^VO:', '', s) for s in seDict['GlueSAAccessControlBaseRule']]
        else:
          voList = [re.sub('^VO:', '', seDict['GlueSAAccessControlBaseRule'])]
        siteDict[site][gridSE] = {'GridSite': seDict['GlueSiteUniqueID'],
                                  'BackendType': seDict['GlueSEImplementationName'],
                                  'Description': seDict.get('GlueSEName', '-'),
                                  'VOs': voList
                                  }

  result = S_OK(siteDict)
  result['BdiiInfo'] = seBdiiDict
  return result


def getGridSRMs(vo, bdiiInfo=None, srmBlackList=None, unUsed=False):
  """ Get all the SRMs available for a given VO

      :param str vo: VO name
      :param dict bdiiInfo: information from BDII
      :param list srmBlackList: SRMs from black list
      :param bool unUsed: unused

      :return: S_OK(dict)/S_ERROR()
  """
  result = ldapService(serviceType='SRM', vo=vo)
  if not result['OK']:
    return result
  srmBdiiDict = result['Value']

  knownSRMs = set()
  if srmBlackList is not None:
    knownSRMs = knownSRMs.union(set(srmBlackList))

  siteSRMDict = {}
  for srm in srmBdiiDict:
    srm = dict(srm)
    endPoint = srm.get('GlueServiceEndpoint', '')
    srmHost = ''
    if endPoint:
      srmHost = urlparse(endPoint).hostname
    if not srmHost:
      continue

    if srmHost in knownSRMs:
      continue

    if unUsed:
      result = getDIRACSesForHostName(srmHost)
      if not result['OK']:
        return result
      diracSEs = result['Value']
      if diracSEs:
        # If it is a known SRM and only new SRMs are requested, continue
        continue
    site = srm.get('GlueForeignKey', '').replace('GlueSiteUniqueID=', '')
    siteSRMDict.setdefault(site, {})
    siteSRMDict[site][srmHost] = srm

  if bdiiInfo is None:
    result = getBdiiSEInfo(vo)
    if not result['OK']:
      return result
    seBdiiDict = dict(result['Value'])
  else:
    seBdiiDict = dict(bdiiInfo)

  srmSeDict = {}
  for site in siteSRMDict:
    srms = siteSRMDict[site].keys()
    for srm in srms:
      if seBdiiDict.get(site, {}).get('SEs', {}).get(srm, {}):
        srmSeDict.setdefault(site, {})
        srmSeDict[site].setdefault(srm, {})
        srmSeDict[site][srm]['SRM'] = siteSRMDict[site][srm]
        srmSeDict[site][srm]['SE'] = seBdiiDict[site]['SEs'][srm]

  return S_OK(srmSeDict)


def getSRMUpdates(vo, bdiiInfo=None):
  """ Get SRM updates

      :param str vo: VO name
      :param dict bdiiInfo: information from BDII

      :return: S_OK(set)/S_ERROR()
  """
  changeSet = set()

  def addToChangeSet(entry, changeSet):
    _section, _option, value, new_value = entry
    if new_value and new_value != value:
      changeSet.add(entry)

  result = getGridSRMs(vo, bdiiInfo=bdiiInfo)
  if not result['OK']:
    return result
  srmBdiiDict = result['Value']

  result = getSEsFromCS()
  if not result['OK']:
    return result
  seDict = result['Value']

  result = getVOs()
  if result['OK']:
    csVOs = set(result['Value'])
  else:
    csVOs = set([vo])

  for seHost, diracSE in seDict.items():
    seSection = '/Resources/StorageElements/%s' % diracSE[0]
    # Look up existing values first
    description = gConfig.getValue(cfgPath(seSection, 'Description'), 'Unknown')
    backend = gConfig.getValue(cfgPath(seSection, 'BackendType'), 'Unknown')
    vos = gConfig.getValue(cfgPath(seSection, 'VO'), 'Unknown').replace(' ', '')
    size = gConfig.getValue(cfgPath(seSection, 'TotalSize'), 'Unknown')
    # Look up current BDII values
    srmDict = {}
    seBdiiDict = {}
    for site in srmBdiiDict:
      if seHost in srmBdiiDict[site]:
        srmDict = srmBdiiDict[site][seHost]['SRM']
        seBdiiDict = srmBdiiDict[site][seHost]['SE']
        break

    if not srmDict or not seBdiiDict:
      continue

    newDescription = seBdiiDict.get('GlueSEName', 'Unknown')
    newBackend = seBdiiDict.get('GlueSEImplementationName', 'Unknown')
    newSize = seBdiiDict.get('GlueSESizeTotal', 'Unknown')
    addToChangeSet((seSection, 'Description', description, newDescription), changeSet)
    addToChangeSet((seSection, 'BackendType', backend, newBackend), changeSet)
    addToChangeSet((seSection, 'TotalSize', size, newSize), changeSet)

    # Evaluate VOs if no space token defined, otherwise this is VO specific
    spaceToken = ''
    for i in range(1, 10):
      protocol = gConfig.getValue(cfgPath(seSection, 'AccessProtocol.%d' % i, 'Protocol'), '')
      if protocol.lower() == 'srm':
        spaceToken = gConfig.getValue(cfgPath(seSection, 'AccessProtocol.%d' % i, 'SpaceToken'), '')
        break
    if not spaceToken:
      bdiiVOs = srmDict.get('GlueServiceAccessControlBaseRule', [])
      bdiiVOs = set([re.sub('^VO:', '', rule) for rule in bdiiVOs])
      seVOs = csVOs.intersection(bdiiVOs)
      newVOs = ','.join(seVOs)
      addToChangeSet((seSection, 'VO', vos, newVOs), changeSet)

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

  return S_OK(parameters)


def getAuthAPI(instance='Production'):
  """ Get OAuth API url

      :param str instance: instance

      :return: str
  """
  return gConfig.getValue("/Systems/Framework/%s/URLs/AuthAPI" % instance)