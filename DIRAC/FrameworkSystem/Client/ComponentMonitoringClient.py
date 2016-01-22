"""
Class for making requests to a ComponentMonitoring Service
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client

class ComponentMonitoringClient( Client ):

  def __init__( self, **kwargs ):
    """
    Constructor function
    """

    Client.__init__( self, **kwargs )
    self.setServer( 'Framework/ComponentMonitoring' )
