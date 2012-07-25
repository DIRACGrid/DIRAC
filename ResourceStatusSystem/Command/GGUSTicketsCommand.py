# $HeadURL:  $
''' GGUSTicketsCommand
 
  The GGUSTickets_Command class is a command class to know about 
  the number of active present opened tickets.
  
'''

import urllib2

from DIRAC                                        import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping  import getGOCSiteName
from DIRAC.ResourceStatusSystem.Command.Command   import Command
#from DIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs
from DIRAC.Core.LCG.GGUSTicketsClient import GGUSTicketsClient

__RCSID__ = '$Id:  $'

#def callClient( name, clientIn ):
#   
#  name = getGOCSiteName( name )[ 'Value' ]
#    
#  openTickets = clientIn.getTicketsList( name )
#  return openTickets
    
################################################################################
################################################################################

class GGUSTicketsOpen( Command ):
  
#  __APIs__ = [ 'GGUSTicketsClient' ]
 
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
    
#    super( GGUSTickets_Open, self ).doCommand()
#    self.apis = initAPIs( self.__APIs__, self.apis )

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args')

    name = self.args[ 'name' ]

#    try:

    name = getGOCSiteName( name )[ 'Value' ] 
    res = self.gClient.getTicketsList( name )
        
    if res[ 'OK' ]:
      res =  S_OK( res[ 'Value' ][ 0 ][ 'open' ] ) 

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res
    
################################################################################
################################################################################

class GGUSTicketsLink( Command ):
  
#  __APIs__ = [ 'GGUSTicketsClient' ]

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
    
#    super( GGUSTickets_Link, self ).doCommand()
#    self.apis = initAPIs( self.__APIs__, self.apis )

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args')

    name = self.args[ 'name' ]

#    try: 
      
    name = getGOCSiteName( name )[ 'Value' ] 
    res = self.gClient.getTicketsList( name )
    #if openTickets == 'Unknown':
    #  return { 'GGUS_Link':'Unknown' }
    if res[ 'OK' ]:
      res = S_OK( res[ 'Value' ][ 1 ] ) 

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class GGUSTicketsInfo( Command ):
  
#  __APIs__ = [ 'GGUSTicketsClient' ]

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
    
#    super( GGUSTickets_Info, self ).doCommand()
#    self.apis = initAPIs( self.__APIs__, self.apis )

    if not 'name' in self.args:
      return S_ERROR( '"name" not found in self.args')

    name = self.args[ 'name' ]

#    try: 
      
    name = getGOCSiteName( name )[ 'Value' ] 
    res = self.gClient.getTicketsList( name )
#    if openTickets == 'Unknown':
#      return { 'GGUS_Info' : 'Unknown' }
    if res[ 'OK' ]:
      res = S_OK( res[ 'Value' ][ 2 ] ) 

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF