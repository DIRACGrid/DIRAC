""" Client-side transfer class for monitoring system
"""

import time
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_ERROR, S_OK

class SiteMapClient:

  ###########################################################################
  def __init__( self, getRPCClient = None ):
    self.getRPCClient = getRPCClient
    self.lastDataRetrievalTime = 0
    
    self.sitesData = {}

  def __getRPCClient( self ):
    if self.getRPCClient:
      return self.getRPCClient( "Monitoring/SiteMap" )
    return RPCClient( "Monitoring/SiteMap" )
  
  ###########################################################################  
  def getSitesData( self ):
    """ Retrieves a single file and puts it in the output directory
    """
    if self.lastDataRetrievalTime - time.time() < 300:
      result = self.__getRPCClient().getSitesData()
      if 'rpcStub' in result:
        del( result[ 'rpcStub' ] )
      if not result[ 'OK' ]:
        return result
      self.sitesData = result[ 'Value' ]
      if self.sitesData:
        self.lastDataRetrievalTime = time.time()
    return S_OK( self.sitesData )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#


