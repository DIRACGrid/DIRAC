########################################################################
# $Id: $
########################################################################
"""

"""
__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient      import RPCClient

class MonitoringClient( object ):
  """ This class expose the methods of the ReportGenerator Service"""

  def __init__( self, rpcClient = None ):
    self.rpcClient = rpcClient

  def __getServer( self, timeout = 3600 ):
    """It returns the access protocol to the ReportGenerator service"""
    if self.rpcClient:
      return self.rpcClient
    else:
      return RPCClient( 'Monitoring/ReportGenerator', timeout = timeout )

  #############################################################################
  def echo( self, string ):
    """It print the string"""
    server = self.__getServer()
    res = server.echo( string )
    print res