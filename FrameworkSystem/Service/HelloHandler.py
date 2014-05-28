""" Hello Service is an example of how to build services in the DIRAC framework
"""

__RCSID__ = "$Id: $"

import types
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time

class HelloHandler( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    """ Handler initialization
    """
    cls.defaultWhom = "World"
    return S_OK()

  def initialize(self):
    """ Response initialization
    """
    self.requestDefaultWhom = self.srv_getCSOption( "DefaultWhom", HelloHandler.defaultWhom )

  auth_sayHello = [ 'all' ]
  types_sayHello = [ types.StringTypes ]
  def export_sayHello( self, whom ):
    """ Say hello to somebody
    """
    if not whom:
      whom = self.requestDefaultWhom
    return S_OK( "Hello " + whom )

