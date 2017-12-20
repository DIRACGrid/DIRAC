''' FreeDiskSpaceCommand
    The Command gets the free space that is left in a Storage Element

    Note: there are, still, many references to "space tokens",
    for example ResourceManagementClient().selectSpaceTokenOccupancyCache(token=elementName)
    This is for historical reasons, and shoud be fixed one day.
    For the moment, when you see "token" or "space token" here, just read "StorageElement".

'''

__RCSID__ = '$Id:$'

from datetime import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient


UNIT_CONVERSION = {"MB": 1, "GB": 1024, "TB": 1024 * 1024}


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
    What is inserted in the DB will be in MB,
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
    occupancyResult = se.getOccupancy()
    if not occupancyResult['OK']:
      return occupancyResult
    occupancy = occupancyResult['Value']
    free = occupancy['Free']
    total = occupancy['Total']

    result = self.rmClient.addOrModifySpaceTokenOccupancyCache(endpoint=endpointResult['Value'],
                                                               lastCheckTime=datetime.utcnow(),
                                                               free=free,
                                                               total=total,
                                                               token=elementName)
    if not result['OK']:
      return result

    # results are normally in 'MB'
    unit = unit.upper()
    if unit not in UNIT_CONVERSION:
      return S_ERROR("No valid unit specified")
    convert = UNIT_CONVERSION[unit]
    return S_OK({'Free': float(free)/float(convert), 'Total': float(total)/float(convert)})

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

    # results are normally in 'MB'
    free = result['Value'][0][3]
    total = result['Value'][0][4]
    unit = unit.upper()
    if unit not in UNIT_CONVERSION:
      return S_ERROR("No valid unit specified")
    convert = UNIT_CONVERSION[unit]
    return S_OK({'Free': float(free)/float(convert), 'Total': float(total)/float(convert)})

  def doMaster(self):
    """
    This method calls the doNew method for each storage element
    that exists in the CS.
    """

    elements = CSHelpers.getStorageElements()

    for name in elements['Value']:
      diskSpace = self.doNew(name)
      if not diskSpace['OK']:
        gLogger.error("Unable to calculate free/total disk space", "name: %s" % name)
        continue

    return S_OK()
