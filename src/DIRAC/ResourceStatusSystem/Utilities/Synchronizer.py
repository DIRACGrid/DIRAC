''' Synchronizer

  Module that keeps the database synchronized with the CS
  Module that updates the RSS database ( ResourceStatusDB ) with the information
  in the Resources section. If there are additions in the CS, those are incorporated
  to the DB. If there are deletions, entries in RSS tables for those elements are
  deleted ( except the Logs table ).

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from DIRAC import gLogger, S_OK
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.SiteSEMapping import getStorageElementsHosts
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getSites, getFTS3Servers, getCESiteMapping
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceURL
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers
from DIRAC.ResourceStatusSystem.Utilities.RssConfiguration import RssConfiguration
from DIRAC.ResourceStatusSystem.Utilities import Utils

ResourceManagementClient = getattr(Utils.voimport(
    'DIRAC.ResourceStatusSystem.Client.ResourceManagementClient'), 'ResourceManagementClient')


class Synchronizer(object):
  '''
  Every time there is a successful write on the CS, Synchronizer().sync() is
  executed. It updates the database with the values on the CS.

  '''

  def __init__(self, rStatus=None, rManagement=None, defaultStatus="Unknown"):

    # Warm up local CS
    CSHelpers.warmUp()

    if rStatus is None:
      self.rStatus = ResourceStatusClient()
    if rManagement is None:
      self.rManagement = ResourceManagementClient()
    self.defaultStatus = defaultStatus

    self.rssConfig = RssConfiguration()

    # this just sets the main owner, "rs_svc" just mean "RSS service"
    self.tokenOwner = "rs_svc"

    # if we are running this script as a user (from a CLI),
    # the username found the proxy will be used as tokenOwner
    result = getProxyInfo()
    if result['OK']:
      self.tokenOwner = result['Value']['username']

  def sync(self, _eventName, _params):
    '''
    Main synchronizer method. It synchronizes the three types of elements: Sites,
    Resources and Nodes. Each _syncX method returns a dictionary with the additions
    and deletions.

    examples:
      >>> s.sync( None, None )
          S_OK()

    :Parameters:
      **_eventName** - any
        this parameter is ignored, but needed by caller function.
      **_params** - any
        this parameter is ignored, but needed by caller function.

    :return: S_OK
    '''

    syncSites = self._syncSites()
    if not syncSites['OK']:
      gLogger.error(syncSites['Message'])

    syncResources = self._syncResources()
    if not syncResources['OK']:
      gLogger.error(syncResources['Message'])

    syncNodes = self._syncNodes()
    if not syncNodes['OK']:
      gLogger.error(syncNodes['Message'])

    return S_OK()

  def _syncSites(self):
    '''
      Sync sites: compares CS with DB and does the necessary modifications.
    '''

    gLogger.info('-- Synchronizing sites --')

    # sites in CS
    res = getSites()
    if not res['OK']:
      return res
    sitesCS = res['Value']

    gLogger.verbose('%s sites found in CS' % len(sitesCS))

    # sites in RSS
    result = self.rStatus.selectStatusElement('Site', 'Status',
                                              meta={'columns': ['Name']})
    if not result['OK']:
      return result
    sitesDB = [siteDB[0] for siteDB in result['Value']]

    # Sites that are in DB but not (anymore) in CS
    toBeDeleted = list(set(sitesDB).difference(set(sitesCS)))
    gLogger.verbose('%s sites to be deleted' % len(toBeDeleted))

    # Delete sites
    for siteName in toBeDeleted:
      deleteQuery = self.rStatus._extermineStatusElement(
          'Site', siteName)
      gLogger.verbose('Deleting site %s' % siteName)
      if not deleteQuery['OK']:
        return deleteQuery

    # Sites that are in CS but not (anymore) in DB
    toBeAdded = list(set(sitesCS).difference(set(sitesDB)))
    gLogger.verbose('%s site entries to be added' % len(toBeAdded))

    for site in toBeAdded:
      query = self.rStatus.addIfNotThereStatusElement('Site', 'Status',
                                                      name=site,
                                                      statusType='all',
                                                      status=self.defaultStatus,
                                                      elementType='Site',
                                                      tokenOwner=self.tokenOwner,
                                                      reason='Synchronized')
      if not query['OK']:
        return query

    return S_OK()

  def _syncResources(self):
    '''
      Sync resources: compares CS with DB and does the necessary modifications.
      ( StorageElements, FTS, FileCatalogs and ComputingElements )
    '''

    gLogger.info('-- Synchronizing Resources --')

    gLogger.verbose('-> StorageElements')
    ses = self.__syncStorageElements()
    if not ses['OK']:
      gLogger.error(ses['Message'])

    gLogger.verbose('-> FTS')
    fts = self.__syncFTS()
    if not fts['OK']:
      gLogger.error(fts['Message'])

    gLogger.verbose('-> FileCatalogs')
    fileCatalogs = self.__syncFileCatalogs()
    if not fileCatalogs['OK']:
      gLogger.error(fileCatalogs['Message'])

    gLogger.verbose('-> ComputingElements')
    computingElements = self.__syncComputingElements()
    if not computingElements['OK']:
      gLogger.error(computingElements['Message'])

    gLogger.verbose('-> removing resources that no longer exist in the CS')
    removingResources = self.__removeNonExistingResourcesFromRM()
    if not removingResources['OK']:
      gLogger.error(removingResources['Message'])

    # FIXME: VOMS

    return S_OK()

  def _syncNodes(self):
    '''
      Sync resources: compares CS with DB and does the necessary modifications.
      ( Queues )
    '''
    gLogger.info('-- Synchronizing Nodes --')

    gLogger.verbose('-> Queues')
    queues = self.__syncQueues()
    if not queues['OK']:
      gLogger.error(queues['Message'])

    return S_OK()

  def __removeNonExistingResourcesFromRM(self):
    '''
      Remove resources from DowntimeCache table that no longer exist in the CS.
    '''

    if not getServiceURL("ResourceStatus/ResourceManagement"):
      gLogger.verbose(
          'ResourceManagement is not installed, skipping removal of non existing resources...')
      return S_OK()

    sesHosts = getStorageElementsHosts()
    if not sesHosts['OK']:
      return sesHosts
    sesHosts = sesHosts['Value']

    resources = sesHosts

    ftsServer = getFTS3Servers(hostOnly=True)
    if ftsServer['OK']:
      resources.extend(ftsServer['Value'])

    res = getCESiteMapping()
    if res['OK']:
      resources.extend(list(res['Value']))

    downtimes = self.rManagement.selectDowntimeCache()

    if not downtimes['OK']:
      return downtimes

    # Remove hosts that no longer exist in the CS
    for host in downtimes['Value']:
      gLogger.verbose('Checking if %s is still in the CS' % host[0])
      if host[0] not in resources:
        gLogger.verbose(
            '%s is no longer in CS, removing entry...' % host[0])
        result = self.rManagement.deleteDowntimeCache(name=host[0])

        if not result['OK']:
          return result

    return S_OK()

  def __syncComputingElements(self):
    '''
      Sync ComputingElements: compares CS with DB and does the necessary modifications.
    '''

    res = getCESiteMapping()
    if not res['OK']:
      return res
    cesCS = list(res['Value'])

    gLogger.verbose('%s Computing elements found in CS' % len(cesCS))

    cesDB = self.rStatus.selectStatusElement('Resource', 'Status',
                                             elementType='ComputingElement',
                                             meta={'columns': ['Name']})
    if not cesDB['OK']:
      return cesDB
    cesDB = [ceDB[0] for ceDB in cesDB['Value']]

    # ComputingElements that are in DB but not in CS
    toBeDeleted = list(set(cesDB).difference(set(cesCS)))
    gLogger.verbose('%s Computing elements to be deleted' %
                    len(toBeDeleted))

    # Delete storage elements
    for ceName in toBeDeleted:

      deleteQuery = self.rStatus._extermineStatusElement(
          'Resource', ceName)

      gLogger.verbose('... %s' % ceName)
      if not deleteQuery['OK']:
        return deleteQuery

    # statusTypes = RssConfiguration.getValidStatusTypes()[ 'Resource' ]
    statusTypes = self.rssConfig.getConfigStatusType('ComputingElement')

    result = self.rStatus.selectStatusElement('Resource', 'Status',
                                              elementType='ComputingElement',
                                              meta={'columns': ['Name', 'StatusType']})
    if not result['OK']:
      return result
    cesTuple = [(x[0], x[1]) for x in result['Value']]

    # For each ( se, statusType ) tuple not present in the DB, add it.
    cesStatusTuples = [(se, statusType)
                       for se in cesCS for statusType in statusTypes]
    toBeAdded = list(set(cesStatusTuples).difference(set(cesTuple)))

    gLogger.debug('%s Computing elements entries to be added' %
                  len(toBeAdded))

    for ceTuple in toBeAdded:

      _name = ceTuple[0]
      _statusType = ceTuple[1]
      _status = self.defaultStatus
      _reason = 'Synchronized'
      _elementType = 'ComputingElement'

      query = self.rStatus.addIfNotThereStatusElement('Resource', 'Status', name=_name,
                                                      statusType=_statusType,
                                                      status=_status,
                                                      elementType=_elementType,
                                                      tokenOwner=self.tokenOwner,
                                                      reason=_reason)
      if not query['OK']:
        return query

    return S_OK()

  def __syncFileCatalogs(self):
    '''
      Sync FileCatalogs: compares CS with DB and does the necessary modifications.
    '''

    catalogsCS = CSHelpers.getFileCatalogs()
    if not catalogsCS['OK']:
      return catalogsCS
    catalogsCS = catalogsCS['Value']

    gLogger.verbose('%s File catalogs found in CS' % len(catalogsCS))

    catalogsDB = self.rStatus.selectStatusElement('Resource', 'Status',
                                                  elementType='Catalog',
                                                  meta={'columns': ['Name']})
    if not catalogsDB['OK']:
      return catalogsDB
    catalogsDB = [catalogDB[0] for catalogDB in catalogsDB['Value']]

    # StorageElements that are in DB but not in CS
    toBeDeleted = list(set(catalogsDB).difference(set(catalogsCS)))
    gLogger.verbose('%s File catalogs to be deleted' % len(toBeDeleted))

    # Delete storage elements
    for catalogName in toBeDeleted:

      deleteQuery = self.rStatus._extermineStatusElement(
          'Resource', catalogName)

      gLogger.verbose('... %s' % catalogName)
      if not deleteQuery['OK']:
        return deleteQuery

    # statusTypes = RssConfiguration.getValidStatusTypes()[ 'Resource' ]
    statusTypes = self.rssConfig.getConfigStatusType('Catalog')

    result = self.rStatus.selectStatusElement('Resource', 'Status',
                                              elementType='Catalog',
                                              meta={'columns': ['Name', 'StatusType']})
    if not result['OK']:
      return result
    sesTuple = [(x[0], x[1]) for x in result['Value']]

    # For each ( se, statusType ) tuple not present in the DB, add it.
    catalogsStatusTuples = [(se, statusType)
                            for se in catalogsCS for statusType in statusTypes]
    toBeAdded = list(set(catalogsStatusTuples).difference(set(sesTuple)))

    gLogger.verbose('%s File catalogs entries to be added' %
                    len(toBeAdded))

    for catalogTuple in toBeAdded:

      _name = catalogTuple[0]
      _statusType = catalogTuple[1]
      _status = self.defaultStatus
      _reason = 'Synchronized'
      _elementType = 'Catalog'

      query = self.rStatus.addIfNotThereStatusElement('Resource', 'Status', name=_name,
                                                      statusType=_statusType,
                                                      status=_status,
                                                      elementType=_elementType,
                                                      tokenOwner=self.tokenOwner,
                                                      reason=_reason)
      if not query['OK']:
        return query

    return S_OK()

  def __syncFTS(self):
    '''
      Sync FTS: compares CS with DB and does the necessary modifications.
    '''

    ftsCS = CSHelpers.getFTS()
    if not ftsCS['OK']:
      return ftsCS
    ftsCS = ftsCS['Value']

    gLogger.verbose('%s FTS endpoints found in CS' % len(ftsCS))

    ftsDB = self.rStatus.selectStatusElement('Resource', 'Status',
                                             elementType='FTS',
                                             meta={'columns': ['Name']})
    if not ftsDB['OK']:
      return ftsDB
    ftsDB = [fts[0] for fts in ftsDB['Value']]

    # StorageElements that are in DB but not in CS
    toBeDeleted = list(set(ftsDB).difference(set(ftsCS)))
    gLogger.verbose('%s FTS endpoints to be deleted' % len(toBeDeleted))

    # Delete storage elements
    for ftsName in toBeDeleted:

      deleteQuery = self.rStatus._extermineStatusElement(
          'Resource', ftsName)

      gLogger.verbose('... %s' % ftsName)
      if not deleteQuery['OK']:
        return deleteQuery

    statusTypes = self.rssConfig.getConfigStatusType('FTS')
    # statusTypes = RssConfiguration.getValidStatusTypes()[ 'Resource' ]

    result = self.rStatus.selectStatusElement('Resource', 'Status',
                                              elementType='FTS',
                                              meta={'columns': ['Name', 'StatusType']})
    if not result['OK']:
      return result
    sesTuple = [(x[0], x[1]) for x in result['Value']]

    # For each ( se, statusType ) tuple not present in the DB, add it.
    ftsStatusTuples = [(se, statusType)
                       for se in ftsCS for statusType in statusTypes]
    toBeAdded = list(set(ftsStatusTuples).difference(set(sesTuple)))

    gLogger.verbose('%s FTS endpoints entries to be added' %
                    len(toBeAdded))

    for ftsTuple in toBeAdded:

      _name = ftsTuple[0]
      _statusType = ftsTuple[1]
      _status = self.defaultStatus
      _reason = 'Synchronized'
      _elementType = 'FTS'

      query = self.rStatus.addIfNotThereStatusElement('Resource', 'Status', name=_name,
                                                      statusType=_statusType,
                                                      status=_status,
                                                      elementType=_elementType,
                                                      tokenOwner=self.tokenOwner,
                                                      reason=_reason)
      if not query['OK']:
        return query

    return S_OK()

  def __syncStorageElements(self):
    '''
      Sync StorageElements: compares CS with DB and does the necessary modifications.
    '''

    sesCS = DMSHelpers().getStorageElements()

    gLogger.verbose('%s storage elements found in CS' % len(sesCS))

    sesDB = self.rStatus.selectStatusElement('Resource', 'Status',
                                             elementType='StorageElement',
                                             meta={'columns': ['Name']})
    if not sesDB['OK']:
      return sesDB
    sesDB = [seDB[0] for seDB in sesDB['Value']]

    # StorageElements that are in DB but not in CS
    toBeDeleted = list(set(sesDB).difference(set(sesCS)))
    gLogger.verbose('%s storage elements to be deleted' % len(toBeDeleted))

    # Delete storage elements
    for sesName in toBeDeleted:

      deleteQuery = self.rStatus._extermineStatusElement(
          'Resource', sesName)

      gLogger.verbose('... %s' % sesName)
      if not deleteQuery['OK']:
        return deleteQuery

    statusTypes = self.rssConfig.getConfigStatusType('StorageElement')
    # statusTypes = RssConfiguration.getValidStatusTypes()[ 'Resource' ]

    result = self.rStatus.selectStatusElement('Resource', 'Status',
                                              elementType='StorageElement',
                                              meta={'columns': ['Name', 'StatusType']})
    if not result['OK']:
      return result
    sesTuple = [(x[0], x[1]) for x in result['Value']]

    # For each ( se, statusType ) tuple not present in the DB, add it.
    sesStatusTuples = [(se, statusType)
                       for se in sesCS for statusType in statusTypes]
    toBeAdded = list(set(sesStatusTuples).difference(set(sesTuple)))

    gLogger.verbose('%s storage element entries to be added' %
                    len(toBeAdded))

    for seTuple in toBeAdded:

      _name = seTuple[0]
      _statusType = seTuple[1]
      _status = self.defaultStatus
      _reason = 'Synchronized'
      _elementType = 'StorageElement'

      query = self.rStatus.addIfNotThereStatusElement('Resource', 'Status', name=_name,
                                                      statusType=_statusType,
                                                      status=_status,
                                                      elementType=_elementType,
                                                      tokenOwner=self.tokenOwner,
                                                      reason=_reason)
      if not query['OK']:
        return query

    return S_OK()

  def __syncQueues(self):
    '''
      Sync Queues: compares CS with DB and does the necessary modifications.
    '''

    queuesCS = CSHelpers.getQueuesRSS()
    if not queuesCS['OK']:
      return queuesCS
    queuesCS = queuesCS['Value']

    gLogger.verbose('%s Queues found in CS' % len(queuesCS))

    queuesDB = self.rStatus.selectStatusElement('Node', 'Status',
                                                elementType='Queue',
                                                meta={'columns': ['Name']})
    if not queuesDB['OK']:
      return queuesDB
    queuesDB = [queueDB[0] for queueDB in queuesDB['Value']]

    # ComputingElements that are in DB but not in CS
    toBeDeleted = list(set(queuesDB).difference(set(queuesCS)))
    gLogger.verbose('%s Queues to be deleted' % len(toBeDeleted))

    # Delete storage elements
    for queueName in toBeDeleted:

      deleteQuery = self.rStatus._extermineStatusElement(
          'Node', queueName)

      gLogger.verbose('... %s' % queueName)
      if not deleteQuery['OK']:
        return deleteQuery

    statusTypes = self.rssConfig.getConfigStatusType('Queue')
    # statusTypes = RssConfiguration.getValidStatusTypes()[ 'Node' ]

    result = self.rStatus.selectStatusElement('Node', 'Status',
                                              elementType='Queue',
                                              meta={'columns': ['Name', 'StatusType']})
    if not result['OK']:
      return result
    queueTuple = [(x[0], x[1]) for x in result['Value']]

    # For each ( se, statusType ) tuple not present in the DB, add it.
    queueStatusTuples = [(se, statusType)
                         for se in queuesCS for statusType in statusTypes]
    toBeAdded = list(set(queueStatusTuples).difference(set(queueTuple)))

    gLogger.verbose('%s Queue entries to be added' % len(toBeAdded))

    for queueTuple in toBeAdded:

      _name = queueTuple[0]
      _statusType = queueTuple[1]
      _status = self.defaultStatus
      _reason = 'Synchronized'
      _elementType = 'Queue'

      query = self.rStatus.addIfNotThereStatusElement('Node', 'Status', name=_name,
                                                      statusType=_statusType,
                                                      status=_status,
                                                      elementType=_elementType,
                                                      tokenOwner=self.tokenOwner,
                                                      reason=_reason)
      if not query['OK']:
        return query

    return S_OK()
