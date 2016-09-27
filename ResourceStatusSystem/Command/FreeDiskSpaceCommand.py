''' FreeDiskSpaceCommand

  The Command gets the free space that is left in a DIRAC Storage Element

'''

from datetime                                                   import datetime
from DIRAC                                                      import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.Core.Utilities                                       import DErrno
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

    elementURL = self.getUrl(elementName, "dips")

    if not elementURL:
      gLogger.info( "Not a DIPS storage element, skipping..." )
      return S_OK()

    self.rpc = RPCClient( elementURL, timeout=120 )
    free = self.rpc.getFreeDiskSpace("/")
    total = self.rpc.getTotalDiskSpace("/")
    result = self.rsClient.addOrModifySpaceTokenOccupancyCache(endpoint = elementURL, lastCheckTime = datetime.utcnow(),
                                                               free = free, total = total, token = elementName )
    if not result[ 'OK' ]:
      return S_ERROR( DErrno.EMYSQL, "Query failed %s" % result )

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
      return S_ERROR( DErrno.EMYSQL, "Query failed %s" % result )

    return S_OK( result )

  def doMaster( self ):
    """
    This method calls the doNew method for each storage element
    that exists in the CS.
    """

    elements = CSHelpers.getStorageElements()

    for name in elements:
      diskSpace = self.doNew( name )
      if not diskSpace[ 'OK' ]:
        S_ERROR( "doNew command failed %s"  % diskSpace )

    return S_OK()
