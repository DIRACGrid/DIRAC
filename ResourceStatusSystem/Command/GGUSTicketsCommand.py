# $HeadURL:  $
''' GGUSTicketsCommand
 
  The GGUSTickets_Command class is a command class to know about 
  the number of active present opened tickets.
  
'''

from DIRAC                                                      import S_ERROR, S_OK
from DIRAC.Core.LCG.GGUSTicketsClient                           import GGUSTicketsClient
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping                import getGOCSiteName
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utiltities                      import CSHelpers


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
      return self.returnERROR( S_ERROR( '"name" not found in self.args' ) )
    name = self.args[ 'name' ]
     
    gocName = getGOCSiteName( name )[ 'Value' ]
    res = self.gClient.getTicketsList( gocName )
    if not res[ 'OK' ]:
      return self.returnERROR( res )
            
    return res  

################################################################################
################################################################################

class GGUSTicketsSitesCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( GGUSTicketsSitesCommand, self ).__init__( args, clients )
    
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
      return self.returnERROR( S_ERROR( '"name" not found in self.args' ) )
    name = self.args[ 'name' ]

    if name is None:
      name = CSHelpers.getSites()
      if not name[ 'OK' ]:
        return self.returnERROR( name )
      name = name[ 'Value' ]

    if not isinstance( name, list ):
      name = [ name ]

    gocNames = []
    
    for siteName in name:
      
      gocName = getGOCSiteName( siteName )[ 'Value' ]
      gocNames.append( gocName )
       
    gocNames = list( set( gocNames ) )
    
    results = {}
    
    for gocName in gocNames:
       
      res = self.gClient.getTicketsList( gocName )
      if not res[ 'OK' ]:
        continue
      res = res[ 'Value' ]
      results[ gocName ] = res 
            
    return S_OK( results )

################################################################################
################################################################################

class GGUSTicketsCacheCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( GGUSTicketsCacheCommand, self ).__init__( args, clients )
    
    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient() 

################################################################################
################################################################################

#class GGUSTicketsOpen( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( GGUSTicketsOpen, self ).__init__( args, clients )
#    
#    if 'GGUSTicketsClient' in self.apis:
#      self.gClient = self.apis[ 'GGUSTicketsClient' ]
#    else:
#      self.gClient = GGUSTicketsClient() 
#  
#  def doCommand( self ):
#    """ 
#    Return getTicketsList from GGUSTickets Client  
#    `args`: 
#      - args[0]: string: should be the name of the site
#    """
#    
#    if not 'name' in self.args:
#      return self.returnERROR( S_ERROR( '"name" not found in self.args' ) )
#    name = self.args[ 'name' ]
#
#    name    = getGOCSiteName( name )[ 'Value' ] 
#    results = self.gClient.getTicketsList( name )
#        
#    if not results[ 'OK' ]:
#      return self.returnERROR( results )
#    results = results[ 'Value' ]
#
##    if not len( results ) > 0:
##      return S_ERROR( 'No tickets to open (0)' ) 
#    if not 'open' in results:
#      return self.returnERROR( S_ERROR( 'Missing open key' ) )
#
#    return  S_OK( results[ 'open' ] ) 
    
################################################################################
################################################################################

#class GGUSTicketsLink( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( GGUSTicketsLink, self ).__init__( args, clients )
#    
#    if 'GGUSTicketsClient' in self.apis:
#      self.gClient = self.apis[ 'GGUSTicketsClient' ]
#    else:
#      self.gClient = GGUSTicketsClient() 
#  
#  def doCommand( self ):
#    """ 
#    Use CallClient to get GGUS link  
#
#   :attr:`args`: 
#     - args[0]: string: should be the name of the site
#    """
#
#    if not 'name' in self.args:
#      return self.returnERROR( S_ERROR( '"name" not found in self.args') )
#    name = self.args[ 'name' ]
#
#    name    = getGOCSiteName( name )[ 'Value' ] 
#    results = self.gClient.getTicketsList( name )
#    #if openTickets == 'Unknown':
#    #  return { 'GGUS_Link':'Unknown' }
#    if not results[ 'OK' ]:
#      return self.returnERROR( results )
#    results = results[ 'Value' ]
#    
#    if not len( results ) >= 1:
#      return self.returnERROR( S_ERROR( 'No tickets to open (1)' ) )
#    
#    return S_OK( results[ 1 ] ) 
    
################################################################################
################################################################################

#class GGUSTicketsInfo( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( GGUSTicketsInfo, self ).__init__( args, clients )
#    
#    if 'GGUSTicketsClient' in self.apis:
#      self.gClient = self.apis[ 'GGUSTicketsClient' ]
#    else:
#      self.gClient = GGUSTicketsClient() 
#  
#  def doCommand( self ):
#    """ 
#    Use callClient to get GGUS info  
#
#   :attr:`args`: 
#     - args[0]: string: should be the name of the site
#    """
#
#    if not 'name' in self.args:
#      return self.returnERROR( S_ERROR( '"name" not found in self.args' ) )
#    name = self.args[ 'name' ]
#     
#    name    = getGOCSiteName( name )[ 'Value' ] 
#    results = self.gClient.getTicketsList( name )
##    if openTickets == 'Unknown':
##      return { 'GGUS_Info' : 'Unknown' }
#    if not results[ 'OK' ]:
#      return self.returnERROR( results )
#    results = results[ 'Value' ]
#
#    if not len( results ) >= 2:
#      return self.returnERROR( S_ERROR( 'No tickets to open (2)' ) )
#    
#    return S_OK( results[ 2 ] ) 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF