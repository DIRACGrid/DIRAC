''' FreeDiskSpaceCommand

  The Command gets the free space that is left in a DIRAC Storage Element

'''

from datetime                                                   import datetime
from DIRAC                                                      import S_OK, S_ERROR, gLogger
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers
from DIRAC.Resources.Storage.StorageElement                     import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id:  $'


class FreeDiskSpaceCommand( Command ):
  '''
  Uses diskSpace method to get the free space
  '''

  def __init__( self, args = None, clients = None ):

    super( FreeDiskSpaceCommand, self ).__init__( args, clients = clients )

    self.rpc = None
    self.rsClient = ResourceManagementClient()

  def _prepareCommand( self ):
    '''
      FreeDiskSpaceCommand requires one argument:
      - name : <str>
    '''

    if 'name' not in self.args:
      return S_ERROR( '"name" not found in self.args' )
    elementName = self.args[ 'name' ]

    return S_OK( elementName )

  def doNew( self, masterParams = None ):
    """
    Gets the total and the free disk space of a DIPS storage element that
    is found in the CS and inserts the results in the SpaceTokenOccupancyCache table
    of ResourceManagementDB database.
    """

    if masterParams is not None:
      elementName = masterParams
    else:
      elementName = self._prepareCommand()
      if not elementName[ 'OK' ]:
        return elementName

    se = StorageElement(elementName)

    elementURL = se.getStorageParameters(protocol = "dips")

    if elementURL['OK']:
      elementURL = se.getStorageParameters(protocol = "dips")['Value']['URLBase']
    else:
      gLogger.verbose( "Not a DIPS storage element, skipping..." )
      return S_OK()

    self.rpc = RPCClient( elementURL, timeout=120 )

    free = self.rpc.getFreeDiskSpace("/")

    if not free[ 'OK' ]:
      return free
    free = free['Value']

    total = self.rpc.getTotalDiskSpace("/")

    if not total[ 'OK' ]:
      return total
    total = total['Value']

    if free and free < 1:
      free = 1
    if total and total < 1:
      total = 1

    result = self.rsClient.addOrModifySpaceTokenOccupancyCache( endpoint = elementURL, lastCheckTime = datetime.utcnow(),
                                                                free = free, total = total,
                                                                token = elementName )
    if not result[ 'OK' ]:
      return result

    return S_OK()

  def doCache( self ):
    """
    This is a method that gets the element's details from the spaceTokenOccupancy cache.
    """

    elementName = self._prepareCommand()
    if not elementName[ 'OK' ]:
      return elementName

    result = self.rsClient.selectSpaceTokenOccupancyCache(token = elementName)

    if not result[ 'OK' ]:
      return result

    return S_OK( result )

  def doMaster( self ):
    """
    This method calls the doNew method for each storage element
    that exists in the CS.
    """

    elements = CSHelpers.getStorageElements()

    for name in elements['Value']:
      diskSpace = self.doNew( name )
      if not diskSpace[ 'OK' ]:
        gLogger.error( "Unable to calculate free disk space" )
        continue

    return S_OK()
