""" GGUSTicketsClient class is a client for the GGUS Tickets DB.
"""

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class GGUSTicketsClient:
  
#############################################################################

  def getTicketsNumber(self, args):
    """  return opened tickets of entity in args
        - args[0] should be the name of the site

        returns:
          {
            'GGUSTickets': n'
          }
    """
    pass