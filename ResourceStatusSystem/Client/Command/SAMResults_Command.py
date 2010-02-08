""" The SAMResults_Command class is a command class to know about 
    present SAM status
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class SAMResults_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from SAM Results Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GOC DB Client
      from DIRAC.ResourceStatusSystem.Client.SAMResultsClient import SAMResultsClient   
      c = SAMResultsClient()
      
    if len(args) == 2:
      res = c.getStatus(args[0], args[1])
    elif len(args) == 3:
      res = c.getStatus(args[0], args[1], args[2])
    elif len(args) == 4:
      res = c.getStatus(args[0], args[1], args[2], args[3])

    return res