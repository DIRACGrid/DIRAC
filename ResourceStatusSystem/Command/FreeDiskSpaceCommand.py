''' FreeDiskSpaceCommand

  The Command gets the free space that is left in a Storage Element

'''

__RCSID__ = '$Id:$'

from datetime import datetime

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities import CSHelpers
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

#FIXME: use unit!

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

    unit = 'TB'
    if 'unit' in self.args:
      unit = self.args['unit']

    return S_OK((elementName, unit))

  def doNew(self, masterParams=None):
    """
    Gets the parameters to run, either from the master method or from its
    own arguments.

    Gets the total and the free disk space of a storage element
    and inserts the results in the SpaceTokenOccupancyCache table
    of ResourceManagementDB database.
    """

    if masterParams is not None:
      elementName = masterParams
      unit = 'TB'
    else:
      elementName, unit = self._prepareCommand()
      if not elementName['OK']:
        return elementName

    endpointResult = CSHelpers.getStorageElementEndpoint(elementName)
    if not endpointResult['OK']:
      return endpointResult

    se = StorageElement(elementName)
    occupancyResult = se.getOccupancy()
    if not occupancyResult['OK']:
      return occupancyResult
    occupancy = occupancyResult['Value']

    result = self.rmClient.addOrModifySpaceTokenOccupancyCache(endpoint=endpointResult['Value'],
                                                               lastCheckTime=datetime.utcnow(),
                                                               free=occupancy['Free'],
                                                               total=occupancy['Total'],
                                                               token=elementName)
    if not result['OK']:
      return result

    return S_OK()

  def doCache(self):
    """
    This is a method that gets the element's details from the spaceTokenOccupancy cache.
    It will return a list of dictionaries if there are results.
    """

    elementName, unit = self._prepareCommand()
    if not elementName['OK']:
      return elementName

    result = self.rmClient.selectSpaceTokenOccupancyCache(token=elementName)

    if not result['OK']:
      return result

    return S_OK(result)

  def doMaster(self):
    """
    This method calls the doNew method for each storage element
    that exists in the CS.
    """

    elements = CSHelpers.getStorageElements()

    for name in elements['Value']:
      diskSpace = self.doNew(name)
      if not diskSpace['OK']:
        gLogger.error("Unable to calculate free disk space", "name: %s" % name)
        continue

    return S_OK()
