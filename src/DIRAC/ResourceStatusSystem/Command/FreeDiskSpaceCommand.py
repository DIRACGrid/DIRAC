''' FreeDiskSpaceCommand
    The Command gets the free space that is left in a Storage Element

    Note: there are, still, many references to "space tokens",
    for example ResourceManagementClient().selectSpaceTokenOccupancyCache(token=elementName)
    This is for historical reasons, and shoud be fixed one day.
    For the moment, when you see "token" or "space token" here, just read "StorageElement".

'''
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = '$Id$'

import sys
import errno

from datetime import datetime

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.File import convertSizeUnits
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.AccountingSystem.Client.Types.StorageOccupancy import StorageOccupancy
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient


class FreeDiskSpaceCommand(Command):
  '''
  Uses diskSpace method to get the free space
  '''

  def __init__(self, args=None, clients=None):

    super(FreeDiskSpaceCommand, self).__init__(args, clients=clients)

    self.rmClient = ResourceManagementClient()

  def _prepareCommand(self):
    '''
      FreeDiskSpaceCommand requires one argument:
      - name : <str>
    '''

    if 'name' not in self.args:
      return S_ERROR('"name" not found in self.args')
    elementName = self.args['name']

    # We keep TB as default as this is what was used (and will still be used)
    # in the policy for "space tokens" ("real", "data" SEs)
    unit = self.args.get('unit', 'TB')

    return S_OK((elementName, unit))

  def doNew(self, masterParams=None):
    """
    Gets the parameters to run, either from the master method or from its
    own arguments.

    Gets the total and the free disk space of a storage element
    and inserts the results in the SpaceTokenOccupancyCache table
    of ResourceManagementDB database.

    The result is also returned to the caller, not only inserted.
    What is inserted in the DB will normally be in MB,
    what is returned will be in the specified unit.
    """

    if masterParams is not None:
      elementName, unit = masterParams
    else:
      params = self._prepareCommand()
      if not params['OK']:
        return params
      elementName, unit = params['Value']

    se = StorageElement(elementName)
    occupancyResult = se.getOccupancy(unit=unit)
    if not occupancyResult['OK']:
      return occupancyResult
    occupancy = occupancyResult['Value']
    free = occupancy['Free']
    total = occupancy['Total']

    endpointResult = CSHelpers.getStorageElementEndpoint(elementName)
    if not endpointResult['OK']:
      return endpointResult
    # We only take the first endpoint, in case there are severals of them (which is normal).
    # Most probably not ideal, because it would be nice to stay consistent, but well...
    endpoint = endpointResult['Value'][0]

    results = {'Endpoint': endpoint,
               'Free': free,
               'Total': total,
               'ElementName': elementName}
    result = self._storeCommand(results)
    if not result['OK']:
      return result

    return S_OK({'Free': free, 'Total': total})

  def _storeCommand(self, results):
    """
    Stores the results in the cache (SpaceTokenOccupancyCache),
    and adds records to the StorageOccupancy accounting.

    :param dict results: something like {'ElementName': 'CERN-HIST-EOS',
                                         'Endpoint': 'httpg://srm-eoslhcb-bis.cern.ch:8443/srm/v2/server',
                                         'Free': 3264963586.10073,
                                         'Total': 8000000000.0}
    :returns: S_OK/S_ERROR dict
    """

    # Stores in cache
    res = self.rmClient.addOrModifySpaceTokenOccupancyCache(endpoint=results['Endpoint'],
                                                            lastCheckTime=datetime.utcnow(),
                                                            free=results['Free'],
                                                            total=results['Total'],
                                                            token=results['ElementName'])
    if not res['OK']:
      self.log.error("Error calling addOrModifySpaceTokenOccupancyCache", res['Message'])
      return res

    # Now proceed with the accounting
    siteRes = DMSHelpers().getLocalSiteForSE(results['ElementName'])
    if not siteRes['OK']:
      return siteRes

    accountingDict = {
        'StorageElement': results['ElementName'],
        'Endpoint': results['Endpoint'],
        'Site': siteRes['Value'] if siteRes['Value'] else 'unassigned'
    }

    results['Used'] = results['Total'] - results['Free']

    for sType in ['Total', 'Free', 'Used']:
      spaceTokenAccounting = StorageOccupancy()
      spaceTokenAccounting.setNowAsStartAndEndTime()
      spaceTokenAccounting.setValuesFromDict(accountingDict)
      spaceTokenAccounting.setValueByKey('SpaceType', sType)
      spaceTokenAccounting.setValueByKey('Space', int(convertSizeUnits(results[sType], 'MB', 'B')))

      res = gDataStoreClient.addRegister(spaceTokenAccounting)
      if not res['OK']:
        self.log.warn("Could not commit register", res['Message'])
        continue

    return gDataStoreClient.commit()

  def doCache(self):
    """
    This is a method that gets the element's details from the spaceTokenOccupancyCache DB table.
    It will return a dictionary with th results, converted to "correct" unit.
    """

    params = self._prepareCommand()
    if not params['OK']:
      return params
    elementName, unit = params['Value']

    result = self.rmClient.selectSpaceTokenOccupancyCache(token=elementName)

    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR(errno.ENODATA, "No occupancy recorded")

    # results are normally in 'MB'
    free = result['Value'][0][3]
    total = result['Value'][0][4]

    free = convertSizeUnits(free, 'MB', unit)
    total = convertSizeUnits(total, 'MB', unit)

    if free == -sys.maxsize or total == -sys.maxsize:
      return S_ERROR("No valid unit specified")

    return S_OK({'Free': free, 'Total': total})

  def doMaster(self):
    """
    This method calls the doNew method for each storage element
    that exists in the CS.
    """

    for name in DMSHelpers().getStorageElements():
      try:
        # keeping TB as default
        diskSpace = self.doNew((name, 'MB'))
        if not diskSpace['OK']:
          self.log.warn("Unable to calculate free/total disk space", "name: %s" % name)
          self.log.warn(diskSpace['Message'])
          continue
      except Exception as excp:  # pylint: disable=broad-except
        self.log.error("Failed to get SE FreeDiskSpace information ==> SE skipped", name)
        self.log.exception("Operation finished with exception: ", lException=excp)

    # Clear the cache
    return self._cleanCommand()

  def _cleanCommand(self, toDelete=None):
    """ Clean the spaceTokenOccupancy table from old endpoints

        :param tuple toDelete: endpoint to remove (endpoint, storage_element_name),
                               e.g. ('httpg://srm-lhcb.cern.ch:8443/srm/managerv2', CERN-RAW)
    """
    if not toDelete:
      toDelete = []

      res = self.rmClient.selectSpaceTokenOccupancyCache()
      if not res['OK']:
        return res
      storedSEsSet = set([(sse[0], sse[1]) for sse in res['Value']])

      currentSEsSet = set()
      currentSEs = DMSHelpers().getStorageElements()
      for cse in currentSEs:
        res = CSHelpers.getStorageElementEndpoint(cse)
        if not res['OK']:
          self.log.warn("Could not get endpoint", res['Message'])
          continue
        endpoint = res['Value'][0]

        currentSEsSet.add((endpoint, cse))
      toDelete = list(storedSEsSet - currentSEsSet)

    else:
      toDelete = [toDelete]

    for ep in toDelete:
      res = self.rmClient.deleteSpaceTokenOccupancyCache(ep[0], ep[1])
      if not res['OK']:
        self.log.warn("Could not delete entry from SpaceTokenOccupancyCache", res['Message'])

    return S_OK()
