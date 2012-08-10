# $HeadURL:  $
''' GGUSTicketsCommand
 
  The GGUSTickets_Command class is a command class to know about 
  the number of active present opened tickets.
  
'''

from DIRAC                                       import S_OK, S_ERROR
from DIRAC.Core.LCG.GGUSTicketsClient            import GGUSTicketsClient
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName
from DIRAC.ResourceStatusSystem.Command.Command  import Command

__RCSID__ = '$Id:  $'

################################################################################
################################################################################

class GGUSTicketsCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( GGUSTicketsCommand, self ).__init__( args, clients )
    
    if 'GGUSTicketsClient' in self.apis:
      self.gClient = self.apis[ 'GGUSTicketsClient' ]
    else:
      self.gClient = GGUSTicketsClient() 
  
  def doCommand( self ):
    """ 
    Return getTicketsList from GGUSTickets Client  
    `args`: 
      - args[0]: string: should be the name of the site
    """
    
    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    name = self.args[ 'name' ]

    name    = getGOCSiteName( name )[ 'Value' ] 
    return self.gClient.getTicketsList( name )

################################################################################
################################################################################

class GGUSTicketsOpen( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( GGUSTicketsOpen, self ).__init__( args, clients )
    
    if 'GGUSTicketsClient' in self.apis:
      self.gClient = self.apis[ 'GGUSTicketsClient' ]
    else:
      self.gClient = GGUSTicketsClient() 
  
  def doCommand( self ):
    """ 
    Return getTicketsList from GGUSTickets Client  
    `args`: 
      - args[0]: string: should be the name of the site
    """
    
    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    name = self.args[ 'name' ]

    name    = getGOCSiteName( name )[ 'Value' ] 
    results = self.gClient.getTicketsList( name )
        
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]

#    if not len( results ) > 0:
#      return S_ERROR( 'No tickets to open (0)' ) 
    if not 'open' in results:
      return S_ERROR( 'Missing open key' )

    return  S_OK( results[ 'open' ] ) 
    
################################################################################
################################################################################

class GGUSTicketsLink( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( GGUSTicketsLink, self ).__init__( args, clients )
    
    if 'GGUSTicketsClient' in self.apis:
      self.gClient = self.apis[ 'GGUSTicketsClient' ]
    else:
      self.gClient = GGUSTicketsClient() 
  
  def doCommand( self ):
    """ 
    Use CallClient to get GGUS link  

   :attr:`args`: 
     - args[0]: string: should be the name of the site
    """

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args')
    name = self.args[ 'name' ]

    name    = getGOCSiteName( name )[ 'Value' ] 
    results = self.gClient.getTicketsList( name )
    #if openTickets == 'Unknown':
    #  return { 'GGUS_Link':'Unknown' }
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]
    
    if not len( results ) >= 1:
      return S_ERROR( 'No tickets to open (1)' )
    
    return S_OK( results[ 1 ] ) 
    
################################################################################
################################################################################

class GGUSTicketsInfo( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( GGUSTicketsInfo, self ).__init__( args, clients )
    
    if 'GGUSTicketsClient' in self.apis:
      self.gClient = self.apis[ 'GGUSTicketsClient' ]
    else:
      self.gClient = GGUSTicketsClient() 
  
  def doCommand( self ):
    """ 
    Use callClient to get GGUS info  

   :attr:`args`: 
     - args[0]: string: should be the name of the site
    """

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    name = self.args[ 'name' ]
     
    name    = getGOCSiteName( name )[ 'Value' ] 
    results = self.gClient.getTicketsList( name )
#    if openTickets == 'Unknown':
#      return { 'GGUS_Info' : 'Unknown' }
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]

    if not len( results ) >= 2:
      return S_ERROR( 'No tickets to open (2)')
    
    return S_OK( results[ 2 ] ) 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF