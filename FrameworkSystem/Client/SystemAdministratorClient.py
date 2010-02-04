########################################################################
# $HeadURL:  $
########################################################################

""" The SystemAdministratorClient is a class representing the client of the DIRAC
    SystemAdministrator service
""" 

__RCSID__ = "$Id:  $"

import re, time, random, os, types

from DIRAC import S_OK, S_ERROR, gLogger,gConfig
from DIRAC.Core.Base.Client import Client


class SystemAdministratorClient(Client):

  def __init__(self,url=None):
    """ Constructor function.
    """
    
    if url:
      self.serverURL = url
    else:  
      self.serverURL = 'Framework/SystemAdministrator'