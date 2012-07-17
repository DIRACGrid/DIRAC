# $HeadURL:  $
''' PilotsCommand
 
  The PilotsCommand class is a command class to know about
  present pilots efficiency.
  
'''

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
#from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
#from DIRAC.ResourceStatusSystem.Utilities.Utils      import where
from DIRAC.ResourceStatusSystem.Client.PilotsClient             import PilotsClient 
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient     import ResourceStatusClient 
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient 


__RCSID__ = '$Id:  $'

################################################################################
################################################################################

class PilotsStatsCommand( Command ):

#  __APIs__ = [ 'PilotsClient' ]

  def __init__( self, args = None, clients = None ):
    
    super( PilotsStatsCommand, self ).__init__( args, clients )
    
    if 'PilotsClient' in self.APIs:
      self.pClient = self.APIs[ 'PilotsClient' ]
    else:
      self.pClient = PilotsClient()     

  def doCommand( self ):
    """
    Return getPilotStats from Pilots Client
    """
    
#    try:
      
    res = self.pClient.getPilotsStats( self.args[0], self.args[1], self.args[2] )
      
#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class PilotsEffCommand( Command ):

#  __APIs__ = [ 'PilotsClient' ]

  def __init__( self, args = None, clients = None ):
    
    super( PilotsEffCommand, self ).__init__( args, clients )
    
    if 'PilotsClient' in self.APIs:
      self.pClient = self.APIs[ 'PilotsClient' ]
    else:
      self.pClient = PilotsClient()  

  def doCommand( self ):
    """
    Return getPilotsEff from Pilots Client
    """
    
#    super( PilotsEff_Command, self ).doCommand()
#    self.APIs = initAPIs( self.__APIs__, self.APIs )

#    try:
      
    res = self.pClient.getPilotsEff( self.args[0], self.args[1], self.args[2] )
       
#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res
  
#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class PilotsEffSimpleCommand( Command ):

#  __APIs__ = [ 'ResourceStatusClient', 'PilotsClient' ]

  def __init__( self, args = None, clients = None ):
    
    super( PilotsEffSimpleCommand, self ).__init__( args, clients )
    
    if 'ResourceStatusClient' in self.APIs:
      self.rsClient = self.APIs[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()      
    
    if 'PilotsClient' in self.APIs:
      self.pClient = self.APIs[ 'PilotsClient' ]
    else:
      self.pClient = PilotsClient()  

  def doCommand( self, RSClientIn = None ):
    """
    Returns simple pilots efficiency

    :attr:`args`:
        - args[0]: string - should be a ValidElement

        - args[1]: string - should be the name of the ValidElement

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    
#    super( PilotsEffSimple_Command, self ).doCommand()
#    self.APIs = initAPIs( self.__APIs__, self.APIs )    

#    try:

    if self.args[ 0 ] == 'Service':
      name = self.rsClient.getGeneralName( self.args[0], self.args[1], 'Site' )
      name        = name[ 'Value' ][ 0 ]
      granularity = 'Site'
    elif self.args[0] in [ 'Site', 'Resource' ]:
      name        = self.args[1]
      granularity = self.args[0]
    else:
      return S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] )

    res = self.pClient.getPilotsSimpleEff( granularity, name )
    if res is None:
      res = 'Idle'
    elif res[ name ] is None:
      res = 'Idle'
    else:
      res = res[ name ] 

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return S_OK( res )

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class PilotsEffSimpleCachedCommand( Command ):

#  __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]

  def __init__( self, args = None, clients = None ):
    
    super( PilotsEffSimpleCachedCommand, self ).__init__( args, clients )
    
    if 'ResourceStatusClient' in self.APIs:
      self.rsClient = self.APIs[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient()      
    
    if 'ResourceManagementClient' in self.APIs:
      self.rmClient = self.APIs[ 'ResourceManagementClient' ]
    else:
      self.emClient = ResourceManagementClient()  

  def doCommand( self ):
    """
    Returns simple pilots efficiency

    :attr:`args`:
       - args[0]: string: should be a ValidElement

       - args[1]: string should be the name of the ValidElement

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    
#    super( PilotsEffSimpleCached_Command, self ).doCommand()
#    self.APIs = initAPIs( self.__APIs__, self.APIs )    

#    try:

    if self.args[0] == 'Service':
      name = self.rsClient.getGeneralName( self.args[0], self.args[1], 'Site' )
      name        = name[ 'Value' ][ 0 ]
      granularity = 'Site'
    elif self.args[0] == 'Site':
      name        = self.args[1]
      granularity = self.args[0]
    else:
      return S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] )

    clientDict = { 
                  'name'        : name,
                  'commandName' : 'PilotsEffSimpleEverySites',
                  'value'       : 'PE_S',
                  'opt_ID'      : 'NULL',
                  'meta'        : { 'columns'     : 'Result' }
                  }
      
    res = self.rmClient.getClientCache( **clientDict )
      
    if res[ 'OK' ]:               
      res = res[ 'Value' ] 
      if res == None or res == []:
        res = S_OK( 'Idle' )
      else:
        res = S_OK( res[ 0 ] )

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

    return res

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  