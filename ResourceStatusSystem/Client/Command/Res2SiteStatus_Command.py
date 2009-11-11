""" The Res2Site_Command class is a command class to derive the status
    of the site by the status of its resources
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class Res2SiteStatus_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from GOC DB Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
      c = ResourceStatusClient()
      
    return c.getCollectiveResStatus(args)
