""" The GGUSTickets_Command class is a command class to know about 
    the number of active present opened tickets
"""

import urllib2

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

def callClient(args, clientIn = None):

  if clientIn is not None:
    c = clientIn
  else:
    # use standard GGUSTickets Client
    from DIRAC.ResourceStatusSystem.Client.GGUSTicketsClient import GGUSTicketsClient   
    c = GGUSTicketsClient()
    
  name = args[0]
  
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
  
  def doCommand(self, args, clientIn=None):
    """ Return getTicketsList from GGUSTickets Client  

       :params:
         :attr:`args`: 
           - args[0]: string: should be the name of the site
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
  
    openTickets = callClient(args, clientIn)
    if openTickets == 'Unknown':
      return {'OpenT':'Unknown'}
    
    return {'OpenT': openTickets[0]['open']} 

#############################################################################

class GGUSTickets_Link(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Use CallClient to get GGUS link  

       :params:
         :attr:`args`: 
           - args[0]: string: should be the name of the site
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
  
    openTickets = callClient(args, clientIn)
    if openTickets == 'Unknown':
      return {'GGUS_Link':'Unknown'}
    
    return {'GGUS_Link': openTickets[1]}

#############################################################################

class GGUSTickets_Info(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Use callClient to get GGUS info  

       :params:
         :attr:`args`: 
           - args[0]: string: should be the name of the site
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
  
    openTickets = callClient(args, clientIn)
    if openTickets == 'Unknown':
      return {'GGUS_Info':'Unknown'}
    
    return {'GGUS_Info': openTickets[2]}

#############################################################################
