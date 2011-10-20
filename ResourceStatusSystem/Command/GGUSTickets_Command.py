################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The GGUSTickets_Command class is a command class to know about 
  the number of active present opened tickets
"""

import urllib2

from DIRAC import gLogger
from DIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName

from DIRAC.ResourceStatusSystem.Command.Command import *

def callClient( args, clientIn ):
   
  name = args[1]
  name = getGOCSiteName(name)
  if not name['OK']:
    raise RSSException, name['Message']
  name = name['Value']
  
  try:
    openTickets = clientIn.getTicketsList(name)
    if not openTickets['OK']:
      return 'Unknown'
    return openTickets['Value']
  except urllib2.URLError:
    gLogger.error("GGUSTicketsClient timed out for " + name)
    return 'Unknown'
  except:
    gLogger.exception("Exception when calling GGUSTicketsClient for " + name)
    return 'Unknown'
    
################################################################################
################################################################################

class GGUSTickets_Open( Command ):
  
  __APIs__ = [ 'GGUSTicketsClient' ]
  
  def doCommand( self ):
    """ 
    Return getTicketsList from GGUSTickets Client  
    `args`: 
      - args[0]: string: should be the name of the site
    """
    
    super( GGUSTickets_Open, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    openTickets = callClient( self.args, self.APIs[ 'GGUSTicketsClient' ] )
    if openTickets == 'Unknown':
      return { 'Result' : 'Unknown' }
    
    return { 'Result' : openTickets[ 0 ][ 'open' ] } 

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class GGUSTickets_Link(Command):
  
  __APIs__ = [ 'GGUSTicketsClient' ]
  
  def doCommand( self ):
    """ 
    Use CallClient to get GGUS link  

   :attr:`args`: 
     - args[0]: string: should be the name of the site
    """
    
    super( GGUSTickets_Link, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    openTickets = callClient( self.args, self.APIs[ 'GGUSTicketsClient' ] )
    if openTickets == 'Unknown':
      return { 'GGUS_Link':'Unknown' }
    
    return { 'Result': openTickets[1] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class GGUSTickets_Info(Command):
  
  __APIs__ = [ 'GGUSTicketsClient' ]
  
  def doCommand(self):
    """ 
    Use callClient to get GGUS info  

   :attr:`args`: 
     - args[0]: string: should be the name of the site
    """
    
    super( GGUSTickets_Info, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    openTickets = callClient( self.args, self.APIs[ 'GGUSTicketsClient' ] )
    if openTickets == 'Unknown':
      return { 'GGUS_Info' : 'Unknown' }
    
    return { 'Result' : openTickets[ 2 ] }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF