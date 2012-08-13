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

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()  

  def _storeCommand( self, gocName, result ):
       
    ticketsCount, link, tickets = result
    openTickets = ticketsCount[ 'open' ]
      
    resQuery = self.rmClient.addOrModifyGGUSTicketsCache( gocName, link, 
                                                          openTickets, tickets ) 
    return resQuery
  
  def _prepareCommand( self ):

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    name = self.args[ 'name' ]
     
    gocName = getGOCSiteName( name )
  
    return S_OK( gocName )

  def doNew( self, masterParams = None ):
    """ 
    Return getTicketsList from GGUSTickets Client  
    `args`: 
      - args[0]: string: should be the name of the site
    """
    
    if masterParams is not None:
      gocName = masterParams
      
    else:
      gocName = self._prepareCommand()
      if not gocName[ 'OK' ]:
        return gocName
      gocName = gocName[ 'Value' ] 
      
    result = self.gClient.getTicketsList( gocName )
    if not result[ 'OK' ]:
      return result
             
    storeRes = self._storeCommand( gocName, result[ 'Value' ] )
    if not storeRes[ 'OK' ]:
      return storeRes
    
    return S_OK( result )

  def doCache( self ):
    """ 
    Return getTicketsList from GGUSTickets Client  
    `args`: 
      - args[0]: string: should be the name of the site
    """

    gocName = self._prepareCommand()
    if not gocName[ 'OK' ]:
      return gocName
    gocName = gocName[ 'Value' ] 
    
    result = self.rmClient.selectGGUSTicketsCache( gocSite = gocName )         
    return result  

  def doMaster( self ):
    
    sites = CSHelpers.getSites()
    if not sites[ 'OK' ]:
      return sites
    sites = sites[ 'Value' ]

    gocNames = []
    
    for siteName in sites:
      
      gocName = getGOCSiteName( siteName )
      if not gocName[ 'OK' ]:
        self.metrics['failed'].append( gocName[ 'Message' ] )
        continue
      
      gocNames.append( gocName[ 'Value' ] )
  
    self.metrics[ 'total' ] = len( gocNames )
           
    resQuery = self.rmClient.selectGGUSTicketsCache( meta = { 'columns' : [ 'GocSite' ] } )
    if not resQuery[ 'OK' ]:
      return resQuery
    resQuery = resQuery[ 'Value' ]
    
    gocNamesToQuery = set( gocNames ).difference( set( resQuery ) )   

    self.metrics[ 'processed' ] = len( gocNamesToQuery )
    
    for gocNameToQuery in gocNamesToQuery:
      
      if gocNameToQuery is None:
        self.metrics[ 'failed' ].append( 'None result' )
        continue
      
      result = self.doNew( gocNameToQuery )
      
      if not result[ 'OK' ]:
        self.metrics['failed'].append( result )
      else:
        self.metrics[ 'successful' ] += 1  
       
    return S_OK( self.metrics )    

################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################

#class GGUSTicketsCommand2( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( GGUSTicketsCommand, self ).__init__( args, clients )
#    
#    if 'GGUSTicketsClient' in self.apis:
#      self.gClient = self.apis[ 'GGUSTicketsClient' ]
#    else:
#      self.gClient = GGUSTicketsClient() 
#
#    if 'ResourceManagementClient' in self.apis:
#      self.rmClient = self.apis[ 'ResourceManagementClient' ]
#    else:
#      self.rmClient = ResourceManagementClient()  
#
#  def storeCommand( self, gocName, result ):
#       
#    ticketsCount, link, tickets = result
#    openTickets = ticketsCount[ 'open' ]
#      
#    resQuery = self.rmClient.addOrModifyGGUSTicketsCache( gocName, link, 
#                                                          openTickets, tickets ) 
#
#    return resQuery
#  
#  def prepareCommand( self ):
#
#    if not 'name' in self.args:
#      return S_ERROR( '"name" not found in self.args' )
#    name = self.args[ 'name' ]
#     
#    gocName = getGOCSiteName( name )
#  
#    return S_OK( gocName ) 
#  
#  def doCommand( self, masterParams = None ):
#    """ 
#    Return getTicketsList from GGUSTickets Client  
#    `args`: 
#      - args[0]: string: should be the name of the site
#    """
#    
#    if masterParams is not None:
#      gocName = masterParams
#      
#    else:
#      gocName = self.prepareCommand()
#      if not gocName[ 'OK' ]:
#        return self.returnERROR( gocName )
#      gocName = gocName[ 'Value' ] 
#      
#    result = self.gClient.getTicketsList( gocName )
#    if not result[ 'OK' ]:
#      return self.returnERROR( result )
#             
#    storeRes = self.storeCommand( gocName, result[ 'Value' ] )
#    if not storeRes[ 'OK' ]:
#      return self.returnERROR( storeRes )
#    
#    return S_OK( result )  
#
#################################################################################
#################################################################################
#
#class GGUSTicketsMasterCommand( GGUSTicketsCommand ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( GGUSTicketsMasterCommand, self ).__init__( args, clients )
#    
#    self.failed  = []
#    self.metrics = { 'successful' : 0, 'total' : 0, 'processed' : 0 }
#  
#  def doCommand( self, masterParams = None ):
#    """ 
#    Return getTicketsList from GGUSTickets Client  
#    `args`: 
#      - args[0]: string: should be the name of the site
#    """
#    
#    sites = CSHelpers.getSites()
#    if not sites[ 'OK' ]:
#      return self.returnERROR( sites )
#    sites = sites[ 'Value' ]
#
#    gocNames = []
#    
#    for siteName in sites:
#      
#      gocName = getGOCSiteName( siteName )
#      if not gocName[ 'OK' ]:
#        self.failed.append( gocName[ 'Message' ] )
#        continue
#      
#      gocNames.append( gocName[ 'Value' ] )
#  
#    self.metrics[ 'total' ] = len( gocNames )
#           
#    resQuery = self.rmClient.selectGGUSTicketsCache( meta = { 'columns' : [ 'GocSite' ] } )
#    if not resQuery[ 'OK' ]:
#      return self.returnERROR( resQuery )
#    resQuery = resQuery[ 'Value' ]
#    
#    gocNamesToQuery = set( gocNames ).difference( set( resQuery ) )   
#
#    self.metrics[ 'processed' ] = len( gocNamesToQuery )
#    
#    for gocNameToQuery in gocNamesToQuery:
#      
#      result = super( GGUSTicketsMasterCommand, self ).doCommand( gocNameToQuery )
#      
#      if not result[ 'OK' ]:
#        self.failed.append( result )
#      else:
#        self.metrics[ 'successful' ] += 1  
#       
#    return S_OK( ( self.failed, self.metrics ) )
#
#################################################################################
#################################################################################
#
#class GGUSTicketsCacheCommand( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( GGUSTicketsCacheCommand, self ).__init__( args, clients )
#    
#    if 'ResourceManagementClient' in self.apis:
#      self.rmClient = self.apis[ 'ResourceManagementClient' ]
#    else:
#      self.rmClient = ResourceManagementClient() 
#
#  def prepareCommand( self ):
#
#    if not 'name' in self.args:
#      return S_ERROR( '"name" not found in self.args' )
#    name = self.args[ 'name' ]
#     
#    gocName = getGOCSiteName( name )
#  
#    return S_OK( gocName )
#
#  def doCommand( self ):
#    """ 
#    Return getTicketsList from GGUSTickets Client  
#    `args`: 
#      - args[0]: string: should be the name of the site
#    """
#
#    gocName = self.prepareCommand()
#    if not gocName[ 'OK' ]:
#      return self.returnERROR( gocName )
#    gocName = gocName[ 'Value' ] 
#    
#    result = self.rmClient.selectGGUSTicketsCache( gocSite = gocName )
#    if not result[ 'OK' ]:
#      return self.returnERROR( result )
#             
#    return result  
#
#################################################################################
#################################################################################
#
##class GGUSTicketsOpen( Command ):
##  
##  def __init__( self, args = None, clients = None ):
##    
##    super( GGUSTicketsOpen, self ).__init__( args, clients )
##    
##    if 'GGUSTicketsClient' in self.apis:
##      self.gClient = self.apis[ 'GGUSTicketsClient' ]
##    else:
##      self.gClient = GGUSTicketsClient() 
##  
##  def doCommand( self ):
##    """ 
##    Return getTicketsList from GGUSTickets Client  
##    `args`: 
##      - args[0]: string: should be the name of the site
##    """
##    
##    if not 'name' in self.args:
##      return self.returnERROR( S_ERROR( '"name" not found in self.args' ) )
##    name = self.args[ 'name' ]
##
##    name    = getGOCSiteName( name )[ 'Value' ] 
##    results = self.gClient.getTicketsList( name )
##        
##    if not results[ 'OK' ]:
##      return self.returnERROR( results )
##    results = results[ 'Value' ]
##
###    if not len( results ) > 0:
###      return S_ERROR( 'No tickets to open (0)' ) 
##    if not 'open' in results:
##      return self.returnERROR( S_ERROR( 'Missing open key' ) )
##
##    return  S_OK( results[ 'open' ] ) 
#    
#################################################################################
#################################################################################
#
##class GGUSTicketsLink( Command ):
##  
##  def __init__( self, args = None, clients = None ):
##    
##    super( GGUSTicketsLink, self ).__init__( args, clients )
##    
##    if 'GGUSTicketsClient' in self.apis:
##      self.gClient = self.apis[ 'GGUSTicketsClient' ]
##    else:
##      self.gClient = GGUSTicketsClient() 
##  
##  def doCommand( self ):
##    """ 
##    Use CallClient to get GGUS link  
##
##   :attr:`args`: 
##     - args[0]: string: should be the name of the site
##    """
##
##    if not 'name' in self.args:
##      return self.returnERROR( S_ERROR( '"name" not found in self.args') )
##    name = self.args[ 'name' ]
##
##    name    = getGOCSiteName( name )[ 'Value' ] 
##    results = self.gClient.getTicketsList( name )
##    #if openTickets == 'Unknown':
##    #  return { 'GGUS_Link':'Unknown' }
##    if not results[ 'OK' ]:
##      return self.returnERROR( results )
##    results = results[ 'Value' ]
##    
##    if not len( results ) >= 1:
##      return self.returnERROR( S_ERROR( 'No tickets to open (1)' ) )
##    
##    return S_OK( results[ 1 ] ) 
#    
#################################################################################
#################################################################################
#
##class GGUSTicketsInfo( Command ):
##  
##  def __init__( self, args = None, clients = None ):
##    
##    super( GGUSTicketsInfo, self ).__init__( args, clients )
##    
##    if 'GGUSTicketsClient' in self.apis:
##      self.gClient = self.apis[ 'GGUSTicketsClient' ]
##    else:
##      self.gClient = GGUSTicketsClient() 
##  
##  def doCommand( self ):
##    """ 
##    Use callClient to get GGUS info  
##
##   :attr:`args`: 
##     - args[0]: string: should be the name of the site
##    """
##
##    if not 'name' in self.args:
##      return self.returnERROR( S_ERROR( '"name" not found in self.args' ) )
##    name = self.args[ 'name' ]
##     
##    name    = getGOCSiteName( name )[ 'Value' ] 
##    results = self.gClient.getTicketsList( name )
###    if openTickets == 'Unknown':
###      return { 'GGUS_Info' : 'Unknown' }
##    if not results[ 'OK' ]:
##      return self.returnERROR( results )
##    results = results[ 'Value' ]
##
##    if not len( results ) >= 2:
##      return self.returnERROR( S_ERROR( 'No tickets to open (2)' ) )
##    
##    return S_OK( results[ 2 ] ) 
#
#################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF