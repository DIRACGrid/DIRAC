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

  def _storeCommand( self, result ):
          
    resQuery = self.rmClient.addOrModifyGGUSTicketsCache( result[ 'GocSite' ], 
                                                          result[ 'Link' ],
                                                          result[ 'OpenTickets' ],
                                                          result[ 'Tickets' ] ) 
    return resQuery
  
  def _prepareCommand( self ):

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args' )
    name = self.args[ 'name' ]
     
    gocName = getGOCSiteName( name )
  
    return S_OK( gocName )

  def doNew( self, masterParams = None ):
    
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
             
    ticketsCount, link, tickets = result[ 'Value' ]
    openTickets = ticketsCount[ 'open' ]
    
    uniformResult = {}
  
    uniformResult[ 'GocSite' ]     = gocName
    uniformResult[ 'Link' ]        = link
    uniformResult[ 'OpenTickets' ] = openTickets
    uniformResult[ 'Tickets' ]     = tickets

    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes
    
    return S_OK( uniformResult )
  
  def doCache( self ):

    gocName = self._prepareCommand()
    if not gocName[ 'OK' ]:
      return gocName
    gocName = gocName[ 'Value' ] 
    
    result = self.rmClient.selectGGUSTicketsCache( gocSite = gocName )  
    if result[ 'OK' ]:
      result = S_OK( dict( zip( result[ 'Columns' ], result[ 'Value' ] ) ) )
           
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
        self.metrics[ 'failed' ].append( gocName[ 'Message' ] )
        continue
      
      gocNames.append( gocName[ 'Value' ] )
           
    resQuery = self.rmClient.selectGGUSTicketsCache( meta = { 'columns' : [ 'GocSite' ] } )
    if not resQuery[ 'OK' ]:
      return resQuery
    resQuery = [ element[0] for element in resQuery[ 'Value' ] ]
    
    gocNamesToQuery = set( gocNames ).difference( set( resQuery ) )   
    
    for gocNameToQuery in gocNamesToQuery:
      
      if gocNameToQuery is None:
        self.metrics[ 'failed' ].append( 'None result' )
        continue
      
      result = self.doNew( gocNameToQuery )
      
      if not result[ 'OK' ]:
        self.metrics[ 'failed' ].append( result )
       
    return S_OK( self.metrics )    

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF