########################################################################
# $HeadURL$
########################################################################

""" FutureHandler is the implementation of a future
    service in the DISET framework

"""

__RCSID__ = "$Id$"

from types import StringType
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import S_OK

def initializeFutureHandler( serviceInfo ):

  return S_OK()

class FutureHandler( RequestHandler ):
  """ Dummy service class
  """
  ###########################################################################
  types_echo = [StringType]
  def export_echo(self, inputparam):
    """ Echo input to output
    """

    return S_OK(inputparam) 

