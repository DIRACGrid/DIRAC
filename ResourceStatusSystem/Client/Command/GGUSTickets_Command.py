""" The GGUSTickets_Command class is a command class to know about 
    the number of active present opened tickets
"""

#TODO

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

class GGUSTickets_Open(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getStatus from GGUSTickets Client  

       :params:
         :attr:`args`: 
           - args[0]: string: should be the name of the site
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      # use standard GGUSTickets Client
      from DIRAC.ResourceStatusSystem.Client.GGUSTicketsClient import GGUSTicketsClient   
      c = GGUSTicketsClient()
      
    name = args[0]
    
    name = getSiteRealName(name)
    
    openTickets = c.getTicketsList(name, ticketStatus = 'open')
        
    return {'GGUS_Info': openTickets, 'OpenT': len(openTickets)} 
