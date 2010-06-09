""" The GGUSTickets_Command class is a command class to know about 
    the number of active present opened tickets
"""

import urllib2

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Utils import getSiteRealName

def callClient(args, clientIn = None):

  if clientIn is not None:
    c = clientIn
  else:
    # use standard GGUSTickets Client
    from DIRAC.ResourceStatusSystem.Client.GGUSTicketsClient import GGUSTicketsClient   
    c = GGUSTicketsClient()
    
  name = args[1]
  
  name = getSiteRealName(name)
  
  try:
    openTickets = c.getTicketsList(name)
  except urllib2.URLError:
    gLogger.error("GGUSTicketsClient timed out for " + name)
    return 'Unknown'
  except:
    gLogger.exception("Exception when calling GGUSTicketsClient for " + name)
    return 'Unknown'
    
  return openTickets
        
#############################################################################

class GGUSTickets_Open(Command):
  
  def doCommand(self):
    """ 
    Return getTicketsList from GGUSTickets Client  
    `args`: 
      - args[0]: string: should be the name of the site
    """
    super(GGUSTickets_Open, self).doCommand()

    openTickets = callClient(self.args, self.client)
    if openTickets == 'Unknown':
      return {'Result':'Unknown'}
    
    return {'Result': openTickets[0]['open']} 

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class GGUSTickets_Link(Command):
  
  def doCommand(self):
    """ 
    Use CallClient to get GGUS link  

   :attr:`args`: 
     - args[0]: string: should be the name of the site
    """
    super(GGUSTickets_Link, self).doCommand()

    openTickets = callClient(self.args, self.client)
    if openTickets == 'Unknown':
      return {'GGUS_Link':'Unknown'}
    
    return {'Result': openTickets[1]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class GGUSTickets_Info(Command):
  
  def doCommand(self):
    """ 
    Use callClient to get GGUS info  

   :attr:`args`: 
     - args[0]: string: should be the name of the site
    """
    super(GGUSTickets_Info, self).doCommand()

    openTickets = callClient(self.args, self.client)
    if openTickets == 'Unknown':
      return {'GGUS_Info':'Unknown'}
    
    return {'Result': openTickets[2]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################
