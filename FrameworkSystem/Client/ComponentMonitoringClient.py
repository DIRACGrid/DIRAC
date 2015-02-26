"""
Class for making requests to a ComponentMonitoring Service
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client

class ComponentMonitoringClient( Client ):

  def __init__( self, host, port = None, **kwargs ):
    """
    Constructor function. Takes a mandatory host parameter
    """

    Client.__init__( self, **kwargs )
    if not port:
      port = 3426

    self.setServer( 'dips://%s:%s/Framework/ComponentMonitoring' % ( host, port ) )

    if 'testing' in kwargs.keys():
        result = self.setCommit( not kwargs[ 'testing' ] )
        if not result[ 'OK' ]:
          raise Exception( "Could not set the commit flag on the service" )
