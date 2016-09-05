''' FreeDiskSpaceCommand

  The Command gets the free space that is left in a DIRAC Storage Element

'''

from datetime                                                   import datetime
from DIRAC                                                      import S_OK, S_ERROR, gConfig
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.Core.DISET.RPCClient                                 import RPCClient

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

  def getAllUrls(self, protocol = None ):
    """
    Gets all the urls of storage elements from CS.
    If protocol is set, then it is going to fetch only
    the urls that use the given protocol.

    :param protocol: String
    :return: Dictionary of { StorageElementName : Url }
    """

    if protocol:
      # Not case-sensitive
      protocol = protocol.lower()

    SEs = gConfig.getSections("/Resources/StorageElements/")

    urls = {}
    for SE in SEs['Value']:
      res = self.getUrl( SE, protocol )
      if res:
        urls.update( {SE: res} )

    return urls

  def doCommand( self ):
    """
    Gets the total and the free disk space of all DIPS storage elements that
    are found in the CS and inserts the results in the SpaceTokenOccupancyCache table
    of ResourceManagementDB database.
    """

    DIPSurls = self.getAllUrls( "dips" )

    if DIPSurls:
      for name in DIPSurls:
        self.rpc = RPCClient( DIPSurls[name], timeout=120 )
        free = self.rpc.getFreeDiskSpace("/")
        total = self.rpc.getTotalDiskSpace("/")
        self.rsClient.addOrModifySpaceTokenOccupancyCache(endpoint = DIPSurls[name], lastCheckTime = datetime.utcnow(),
                                                          free = free, total = total, token = name )

    return S_OK()
