''' FreeDiskSpaceCommand
    The Command gets the free space that is left in a Storage Element

    Note: there are, still, many references to "space tokens",
    for example ResourceManagementClient().selectSpaceTokenOccupancyCache(token=elementName)
    This is for historical reasons, and shoud be fixed one day.
    For the moment, when you see "token" or "space token" here, just read "StorageElement".

'''

__RCSID__ = '$Id$'

import sys
import errno

from datetime import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.File import convertSizeUnits
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

    endpointResult = CSHelpers.getStorageElementEndpoint(elementName)
    if not endpointResult['OK']:
      return endpointResult

    se = StorageElement(elementName)
    occupancyResult = se.getOccupancy(unit=unit)
    if not occupancyResult['OK']:
      return occupancyResult
    occupancy = occupancyResult['Value']
    free = occupancy['Free']
    total = occupancy['Total']
    spaceReservation = occupancy.get('SpaceReservation', '')
    # We only take the first one, in case there are severals.
    # Most probably not ideal, because it would be nice to stay
    # consistent, but well...
    endpoint = endpointResult['Value'][0]

    results = {'Endpoint': endpoint,
               'Free': free,
               'Total': total,
               'SpaceReservation': spaceReservation,
               'ElementName': elementName}
    result = self._storeCommand(results)
    if not result['OK']:
      return result

    return S_OK({'Free': free, 'Total': total})

  def _storeCommand(self, results):
    """ Here purely for extensibility
    """
    return self.rmClient.addOrModifySpaceTokenOccupancyCache(endpoint=results['Endpoint'],
                                                             lastCheckTime=datetime.utcnow(),
                                                             free=results['Free'],
                                                             total=results['Total'],
                                                             token=results['ElementName'])

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
          gLogger.warn("Unable to calculate free/total disk space", "name: %s" % name)
          gLogger.warn(diskSpace['Message'])
          continue
      except Exception as excp:  # pylint: disable=broad-except
        gLogger.error("Failed to get SE %s FreeDiskSpace information (SE skipped) " % name)
        gLogger.exception("Operation finished  with exception: ", lException=excp)

    return S_OK()
