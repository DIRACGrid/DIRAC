""" The GGUSTickets_Command class is a command class to know about 
    the number of active present opened tickets
"""

#TODO

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class GGUSTickets_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from GGUSTickets Client  
        - args[0] should be the name of the Site
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GGUSTickets Client
      from DIRAC.ResourceStatusSystem.Client.GGUSTicketsClient import GGUSTicketsClient   
      c = GGUSTicketsClient()
      
    return c.getTicketsNumber(args)
