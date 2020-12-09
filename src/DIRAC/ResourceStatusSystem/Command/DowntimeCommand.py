""" DowntimeCommand module will look into GOC DB to find announced downtimes for RSS-managed sites and resources.
    If found, downtimes are added to the internal RSS cache using ResourceManagementClient.

    GOCDB downtimes that are modified or deleted are also synced.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

from six.moves.urllib_error import URLError
import re

from datetime import datetime, timedelta
from operator import itemgetter

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.LCG.GOCDBClient import GOCDBClient
from DIRAC.Core.Utilities.SiteSEMapping import getSEHosts, getStorageElementsHosts
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTS3Servers, getGOCSiteName,\
    getGOCSites, getGOCFTSName, getCESiteMapping
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command import Command


class DowntimeCommand(Command):
  """
    Downtime "master" Command or removed DTs.
  """

  def __init__(self, args=None, clients=None):

    super(DowntimeCommand, self).__init__(args, clients)

    if 'GOCDBClient' in self.apis:
      self.gClient = self.apis['GOCDBClient']
    else:
      self.gClient = GOCDBClient()

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis['ResourceManagementClient']
    else:
      self.rmClient = ResourceManagementClient()

  def _storeCommand(self, result):
    """
      Stores the results of doNew method on the database.
    """

    for dt in result:
      resQuery = self.rmClient.addOrModifyDowntimeCache(downtimeID=dt['DowntimeID'],
                                                        element=dt['Element'],
                                                        name=dt['Name'],
                                                        startDate=dt['StartDate'],
                                                        endDate=dt['EndDate'],
                                                        severity=dt['Severity'],
                                                        description=dt['Description'],
                                                        link=dt['Link'],
                                                        gOCDBServiceType=dt['gOCDBServiceType'])
    return resQuery

  def _cleanCommand(self, element, elementNames):
    """
      Clear Cache from expired DT.
    """

    resQuery = []

    for elementName in elementNames:
      # get the list of all DTs stored in the cache
      result = self.rmClient.selectDowntimeCache(element=element,
                                                 name=elementName)

      if not result['OK']:
        return result

      uniformResult = [dict(zip(result['Columns'], res)) for res in result['Value']]

      currentDate = datetime.utcnow()

      if not uniformResult:
        continue

      # get the list of all ongoing DTs from GocDB
      gDTLinkList = self.gClient.getCurrentDTLinkList()
      if not gDTLinkList['OK']:
        return gDTLinkList

      for dt in uniformResult:
        # if DT expired or DT not in the list of current DTs, then we remove it from the cache
        if dt['EndDate'] < currentDate or dt['Link'] not in gDTLinkList['Value']:
          result = self.rmClient.deleteDowntimeCache(downtimeID=dt['DowntimeID'])
          resQuery.append(result)

    return S_OK(resQuery)

  def _prepareCommand(self):
    """
      DowntimeCommand requires four arguments:
      - name : <str>
      - element : Site / Resource
      - elementType: <str>

      If the elements are Site(s), we need to get their GOCDB names. They may
      not have, so we ignore them if they do not have.
    """

    if 'name' not in self.args:
      return S_ERROR('"name" not found in self.args')
    elementName = self.args['name']

    if 'element' not in self.args:
      return S_ERROR('"element" not found in self.args')
    element = self.args['element']

    if 'elementType' not in self.args:
      return S_ERROR('"elementType" not found in self.args')
    elementType = self.args['elementType']

    if element not in ['Site', 'Resource']:
      return S_ERROR('element is neither Site nor Resource')

    hours = None
    if 'hours' in self.args:
      hours = self.args['hours']

    gOCDBServiceType = None

    # Transform DIRAC site names into GOCDB topics
    if element == 'Site':

      gocSite = getGOCSiteName(elementName)
      if not gocSite['OK']:  # The site is most probably is not a grid site - not an issue, of course
        pass  # so, elementName remains unchanged
      else:
        elementName = gocSite['Value']

    # The DIRAC se names mean nothing on the grid, but their hosts do mean.
    elif elementType == 'StorageElement':
      # for SRM and SRM only, we need to distinguish if it's tape or disk
      # if it's not SRM, then gOCDBServiceType will be None (and we'll use them all)
      try:
        se = StorageElement(elementName)
        seOptions = se.options
        seProtocols = set(se.localAccessProtocolList) | set(se.localWriteProtocolList)
      except AttributeError:  # Sometimes the SE can't be instantiated properly
        self.log.error("Failure instantiating StorageElement object", elementName)
        return S_ERROR("Failure instantiating StorageElement")
      if 'SEType' in seOptions and 'srm' in seProtocols:
        # Type should follow the convention TXDY
        seType = seOptions['SEType']
        diskSE = re.search('D[1-9]', seType) is not None
        tapeSE = re.search('T[1-9]', seType) is not None
        if tapeSE:
          gOCDBServiceType = "srm.nearline"
        elif diskSE:
          gOCDBServiceType = "srm"

      res = getSEHosts(elementName)
      if not res['OK']:
        return res
      seHosts = res['Value']

      if not seHosts:
        return S_ERROR('No seHost(s) for %s' % elementName)
      elementName = seHosts  # in this case it will return a list, because there might be more than one host only

    elif elementType in ['FTS', 'FTS3']:
      gOCDBServiceType = 'FTS'
      # WARNING: this method presupposes that the server is an FTS3 type
      gocSite = getGOCFTSName(elementName)
      if not gocSite['OK']:
        self.log.warn("FTS not in Resources/FTSEndpoints/FTS3 ?", elementName)
      else:
        elementName = gocSite['Value']

    return S_OK((element, elementName, hours, gOCDBServiceType))

  def doNew(self, masterParams=None):
    """
      Gets the parameters to run, either from the master method or from its
      own arguments.

      For every elementName, unless it is given a list, in which case it contacts
      the gocdb client. The server is not very stable, so in case of failure tries
      a second time.

      If there are downtimes, are recorded and then returned.
    """

    if masterParams is not None:
      element, elementNames = masterParams
      hours = 120
      elementName = None
      gOCDBServiceType = None

    else:
      params = self._prepareCommand()
      if not params['OK']:
        return params
      element, elementName, hours, gOCDBServiceType = params['Value']
      if not isinstance(elementName, list):
        elementNames = [elementName]
      else:
        elementNames = elementName

    # WARNING: checking all the DT that are ongoing or starting in given <hours> from now
    try:
      results = self.gClient.getStatus(element, name=elementNames, startingInHours=hours)
    except URLError:
      try:
        # Let's give it a second chance..
        results = self.gClient.getStatus(element, name=elementNames, startingInHours=hours)
      except URLError as e:
        return S_ERROR(e)

    if not results['OK']:
      return results
    results = results['Value']

    if results is None:  # no downtimes found
      return S_OK(None)

    # cleaning the Cache
    if elementNames:
      cleanRes = self._cleanCommand(element, elementNames)
      if not cleanRes['OK']:
        return cleanRes

    uniformResult = []

    # Humanize the results into a dictionary, not the most optimal, but readable
    for downtime, downDic in results.items():  # can be an iterator

      dt = {}

      dt['Name'] = downDic.get('URL', downDic.get('HOSTNAME', downDic.get('SITENAME')))
      if not dt['Name']:
        return S_ERROR("URL, SITENAME and HOSTNAME are missing from downtime dictionary")

      dt['gOCDBServiceType'] = downDic.get('SERVICE_TYPE')

      if dt['gOCDBServiceType'] and gOCDBServiceType:
        if gOCDBServiceType.lower() != downDic['SERVICE_TYPE'].lower():
          self.log.warn("SERVICE_TYPE mismatch",
                        "between GOCDB (%s) and CS (%s) for %s" % (gOCDBServiceType,
                                                                   downDic['SERVICE_TYPE'],
                                                                   dt['Name']))

      dt['DowntimeID'] = downtime
      dt['Element'] = element
      dt['StartDate'] = downDic['FORMATED_START_DATE']
      dt['EndDate'] = downDic['FORMATED_END_DATE']
      dt['Severity'] = downDic['SEVERITY']
      dt['Description'] = downDic['DESCRIPTION'].replace('\'', '')
      dt['Link'] = downDic['GOCDB_PORTAL_URL']

      uniformResult.append(dt)

    storeRes = self._storeCommand(uniformResult)
    if not storeRes['OK']:
      return storeRes

    return S_OK()

  def doCache(self):
    """
      Method that reads the cache table and tries to read from it. It will
      return a list with one dictionary describing the DT if there are results.
    """

    params = self._prepareCommand()
    if not params['OK']:
      return params
    element, elementName, hours, gOCDBServiceType = params['Value']

    result = self.rmClient.selectDowntimeCache(element=element, name=elementName,
                                               gOCDBServiceType=gOCDBServiceType)

    if not result['OK']:
      return result

    uniformResult = [dict(zip(result['Columns'], res)) for res in result['Value']]

    # 'targetDate' can be either now or some 'hours' later in the future
    targetDate = datetime.utcnow()

    # dtOverlapping is a buffer to assure only one dt is returned
    # when there are overlapping outage/warning dt for same element
    # on top of the buffer we put the most recent outages
    # while at the bottom the most recent warnings,
    # assumption: uniformResult list is already ordered by resource/site name, severity, startdate
    dtOverlapping = []

    if hours is not None:
      # IN THE FUTURE
      targetDate = targetDate + timedelta(hours=hours)
      # sorting by 'StartDate' b/c if we look for DTs in the future
      # then we are interested in the earliest DTs
      uniformResult.sort(key=itemgetter('Name', 'Severity', 'StartDate'))

      for dt in uniformResult:
        if (dt['StartDate'] < targetDate) and (dt['EndDate'] > targetDate):
          # the list is already ordered in a way that outages come first over warnings
          # and the earliest outages are on top of other outages and warnings
          # while the earliest warnings are on top of the other warnings
          # so what ever comes first in the list is also what we are looking for
          dtOverlapping = [dt]
          break
    else:
      # IN THE PRESENT
      # sorting by 'EndDate' b/c if we look for DTs in the present
      # then we are interested in those DTs that last longer
      uniformResult.sort(key=itemgetter('Name', 'Severity', 'EndDate'))

      for dt in uniformResult:
        if (dt['StartDate'] < targetDate) and (dt['EndDate'] > targetDate):
          # if outage, we put it on top of the overlapping buffer
          # i.e. the latest ending outage is on top
          if dt['Severity'].upper() == 'OUTAGE':
            dtOverlapping = [dt] + dtOverlapping
          # if warning, we put it at the bottom of the overlapping buffer
          # i.e. the latest ending warning is at the bottom
          elif dt['Severity'].upper() == 'WARNING':
            dtOverlapping.append(dt)

    result = None
    if dtOverlapping:
      dtTop = dtOverlapping[0]
      dtBottom = dtOverlapping[-1]
      if dtTop['Severity'].upper() == 'OUTAGE':
        result = dtTop
      else:
        result = dtBottom

    return S_OK(result)

  def doMaster(self):
    """ Master method, which looks little bit spaghetti code, sorry !
        - It gets all sites and transforms them into gocSites.
        - It gets all the storage elements and transforms them into their hosts
        - It gets the the CEs (FTS and file catalogs will come).
    """

    gocSites = getGOCSites()
    if not gocSites['OK']:
      return gocSites
    gocSites = gocSites['Value']

    sesHosts = getStorageElementsHosts()
    if not sesHosts['OK']:
      return sesHosts
    sesHosts = sesHosts['Value']

    resources = sesHosts if sesHosts else []

    ftsServer = getFTS3Servers(hostOnly=True)
    if ftsServer['OK'] and ftsServer['Value']:
      resources.extend(ftsServer['Value'])

    # TODO: file catalogs need also to use their hosts

    # fc = CSHelpers.getFileCatalogs()
    # if fc[ 'OK' ]:
    #  resources = resources + fc[ 'Value' ]

    res = getCESiteMapping()
    if res['OK'] and res['Value']:
      resources.extend(list(res['Value']))

    self.log.verbose('Processing Sites', ', '.join(gocSites if gocSites else ['NONE']))

    siteRes = self.doNew(('Site', gocSites))
    if not siteRes['OK']:
      self.metrics['failed'].append(siteRes['Message'])

    self.log.verbose('Processing Resources', ', '.join(resources if resources else ['NONE']))

    resourceRes = self.doNew(('Resource', resources))
    if not resourceRes['OK']:
      self.metrics['failed'].append(resourceRes['Message'])

    return S_OK(self.metrics)
