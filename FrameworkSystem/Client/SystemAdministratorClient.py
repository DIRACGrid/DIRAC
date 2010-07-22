########################################################################
# $HeadURL$
########################################################################

""" The SystemAdministratorClient is a class representing the client of the DIRAC
    SystemAdministrator service. It has also methods to update the Configuration
    Service with the DIRAC components options
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client

class SystemAdministratorClient( Client ):

  def __init__( self, host, port = None ):
    """ Constructor function. Takes a mandatory host parameter 
    """
    if not port:
      port = 9162
    self.setServer( 'dips://%s:%s/Framework/SystemAdministrator' % ( host, port ) )
