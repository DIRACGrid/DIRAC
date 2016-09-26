''' FreeDiskSpaceCommand

  The Command gets the free space that is left in a DIRAC Storage Element

'''

from datetime                                                   import datetime
from DIRAC                                                      import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'


class FreeDiskSpaceCommand( Command ):
  '''
  Uses diskSpace method to get the free space
  '''

  def __init__( self, args = None ):

    super( FreeDiskSpaceCommand, self ).__init__( args )

    self.rpc = None
    self.rsClient = None

  def getUrl(self, SE, protocol = None ):
    """
    Gets the url of a storage element from the CS.
    If protocol is set, then it is going to fetch the
    url only if it uses the given protocol.

    :param SE: String
    :param protocol: String
    :return: String
    """
    attributes = [ "Protocol", "Host", "Port", "Path"]

    result = ""
    for attribute in attributes:
      res = gConfig.getValue( "/Resources/StorageElements/%s/AccessProtocol.1/%s" % ( SE, attribute ) )

      if protocol:

        # Not case-sensitive
        protocol = protocol.lower()

        if attribute is "Protocol" and res != protocol:
          result = None
          break

      if attribute is "Protocol":
        result += res + "://"
      elif attribute is "Port":
        result += ":" + res
      else:
        result += res

    return result

  def _prepareCommand( self ):
    '''
      FreeDiskSpaceCommand requires one argument:
      - element : Resource

    '''

    if 'element' not in self.args:
      return S_ERROR( '"element" not found in self.args' )
    element = self.args[ 'element' ]

    return S_OK( element )

  def doNew( self, masterParams = None ):
    """
    Gets the total and the free disk space of all DIPS storage elements that
    are found in the CS and inserts the results in the SpaceTokenOccupancyCache table
    of ResourceManagementDB database.
    """

    if masterParams is not None:
      element = masterParams
    else:
      element = self._prepareCommand()
      if not element[ 'OK' ]:
        return element

    elementURL = self.getUrl(element, "dips")

    if not elementURL:
      gLogger.info( "Not a DIPS storage element, skipping..." )
      return S_OK()

    self.rpc = RPCClient( elementURL, timeout=120 )
    free = self.rpc.getFreeDiskSpace("/")
    total = self.rpc.getTotalDiskSpace("/")
    self.rsClient.addOrModifySpaceTokenOccupancyCache(endpoint = elementURL, lastCheckTime = datetime.utcnow(),
                                                      free = free, total = total, token = element )

    return S_OK()

  def doCache( self ):
    return S_OK()

  def doMaster( self ):

    elements = CSHelpers.getStorageElements()

    for name in elements:
      diskSpace = self.doNew( name )
      if not diskSpace[ 'OK' ]:
        S_ERROR( "doNew command failed %s"  % diskSpace )

    return S_OK()
