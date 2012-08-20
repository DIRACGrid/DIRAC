# $HeadURL $
''' Pilots_Command
 
  The Pilots_Command class is a command class to know about
  present pilots efficiency.
  
'''

from DIRAC                                           import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command      import Command
from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where

__RCSID__ = '$Id: $'

################################################################################
################################################################################

class PilotsStats_Command( Command ):

  __APIs__ = [ 'PilotsClient' ]

  def doCommand( self ):
    """
    Return getPilotStats from Pilots Client
    """
    
    super( PilotsStats_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      
      res = self.APIs[ 'PilotsClient' ].getPilotsStats( self.args[0], self.args[1], self.args[2] )
      
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class PilotsEff_Command( Command ):

  __APIs__ = [ 'PilotsClient' ]

  def doCommand( self ):
    """
    Return getPilotsEff from Pilots Client
    """
    
    super( PilotsEff_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      
      res = self.APIs[ 'PilotsClient' ].getPilotsEff( self.args[0], self.args[1], self.args[2] )
       
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }
  
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class PilotsEffSimple_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient', 'PilotsClient' ]

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
    
    super( PilotsEffSimple_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )    

    try:

      if self.args[ 0 ] == 'Service':
        name = self.APIs[ 'ResourceStatusClient' ].getGeneralName( self.args[0], self.args[1], 'Site' )
        name        = name[ 'Value' ][ 0 ]
        granularity = 'Site'
      elif self.args[0] in [ 'Site', 'Resource' ]:
        name        = self.args[1]
        granularity = self.args[0]
      else:
        return { 'Result' : S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] ) }

      res = self.APIs[ 'PilotsClient' ].getPilotsSimpleEff( granularity, name )
      if res is None:
        res = 'Idle'
      elif res[ name ] is None:
        res = 'Idle'
      else:
        res = res[ name ] 

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : S_OK( res ) }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class PilotsEffSimpleCached_Command( Command ):

  __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]

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
    
    super( PilotsEffSimpleCached_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )    

    try:

      if self.args[0] == 'Service':
        name = self.APIs[ 'ResourceStatusClient' ].getGeneralName( self.args[0], self.args[1], 'Site' )
        name        = name[ 'Value' ][ 0 ]
        granularity = 'Site'
      elif self.args[0] == 'Site':
        name        = self.args[1]
        granularity = self.args[0]
      else:
        return { 'Result' : S_ERROR( '%s is not a valid granularity' % self.args[ 0 ] ) }

      clientDict = { 
                     'name'        : name,
                     'commandName' : 'PilotsEffSimpleEverySites',
                     'value'       : 'PE_S',
                     'opt_ID'      : 'NULL',
                     'meta'        : { 'columns'     : 'Result' }
                   }
      
      res = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )
      
      if res[ 'OK' ]:               
        res = res[ 'Value' ] 
        if res == None or res == []:
          res = S_OK( 'Idle' )
        else:
          res = S_OK( res[ 0 ] )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  