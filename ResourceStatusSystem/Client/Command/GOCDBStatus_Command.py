""" The GOCDBStatus_Command class is a command class to know about 
    present downtimes
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class GOCDBStatus_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from GOC DB Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.GOCDBClient import GOCDBClient   
      c = GOCDBClient()
      
    return c.getStatus(args)
