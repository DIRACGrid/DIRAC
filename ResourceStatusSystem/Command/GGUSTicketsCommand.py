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
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers


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

class GGUSTicketsMasterCommand( Command ):
  
  def __init__( self, args = None, clients = None ):
    
    super( GGUSTicketsMasterCommand, self ).__init__( args, clients )
    
    if 'GGUSTicketsClient' in self.apis:
      self.gClient = self.apis[ 'GGUSTicketsClient' ]
    else:
      self.gClient = GGUSTicketsClient() 
      
    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()   
  
  def storeCommand( self, gocName, result ):
       
    ticketsCount, link, tickets = result
    openTickets = ticketsCount[ 'open' ]
      
    resQuery = self.rmClient.addOrModifyGGUSTicketsCache( gocName, link, 
                                                          openTickets, tickets ) 

    return resQuery
  
  def doCommand( self ):
    """ 
    Return getTicketsList from GGUSTickets Client  
    `args`: 
      - args[0]: string: should be the name of the site
    """
    
    failed = []
    
    sites = CSHelpers.getSites()
    if not sites[ 'OK' ]:
      return self.returnERROR( sites )
    sites = sites[ 'Value' ]

    gocNames = []
    
    for siteName in sites:
      
      gocName = getGOCSiteName( siteName )
      if not gocName[ 'OK' ]:
        failed.append( gocName[ 'Message' ] )
        continue
      
      gocNames.append( gocName[ 'Value' ] )
       
    gocNames = list( set( gocNames ) )
    
    for gocName in gocNames:
       
      res = self.gClient.getTicketsList( gocName )
      if not res[ 'OK' ]:
        failed.append( res[ 'Message' ] )
        continue
      res = res[ 'Value' ]
       
      storeCo = self.storeCommand( gocName, res )
      if not storeCo[ 'OK' ]:
        failed.append( storeCo[ 'Message' ] ) 
       
    return S_OK( failed )

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