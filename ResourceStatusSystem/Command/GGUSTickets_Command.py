################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The GGUSTickets_Command class is a command class to know about 
  the number of active present opened tickets
"""

import urllib2

from DIRAC                                        import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping  import getGOCSiteName

from DIRAC.ResourceStatusSystem.Command.Command   import *

def callClient( name, clientIn ):
   
  name = getGOCSiteName( name )[ 'Value' ]
    
  openTickets = clientIn.getTicketsList( name )
  return openTickets
    
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

    try:

      res = callClient( self.args[1], self.APIs[ 'GGUSTicketsClient' ] )
        
      if res[ 'OK' ]:
        res =  S_OK( res[ 'Value' ][ 0 ][ 'open' ] ) 

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }

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

    try: 
      
      res = callClient( self.args[ 1 ], self.APIs[ 'GGUSTicketsClient' ] )
    #if openTickets == 'Unknown':
    #  return { 'GGUS_Link':'Unknown' }
      if res[ 'OK' ]:
        res = S_OK( res[ 'Value' ][ 1 ] ) 

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }

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

    try: 
      
      res = callClient( self.args[ 1 ], self.APIs[ 'GGUSTicketsClient' ] )     
#    if openTickets == 'Unknown':
#      return { 'GGUS_Info' : 'Unknown' }
      if res[ 'OK' ]:
        res = S_OK( res[ 'Value' ][ 2 ] ) 

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF