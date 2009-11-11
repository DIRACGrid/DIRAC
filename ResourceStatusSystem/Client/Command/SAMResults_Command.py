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
      
    return c.getStatus(args)
