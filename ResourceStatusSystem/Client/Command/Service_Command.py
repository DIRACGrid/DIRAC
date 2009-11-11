""" The Service_Command class is a command class to know about 
    present service stats
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class ServiceStats_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getServiceStats from ResourceStatusClient  

        input:
          - args[0] should be a ServiceType
          - args[1] should be the name of the Site
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
      c = ResourceStatusClient()
      
    return c.getServiceStats(args[0], args[1])

#############################################################################
