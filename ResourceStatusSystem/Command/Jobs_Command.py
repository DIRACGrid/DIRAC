# $HeadURL $
''' Jobs_Command
 
  The Jobs_Command class is a command class to know about 
  present jobs efficiency
  
'''

from DIRAC                                           import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command      import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where

__RCSID__ = '$Id: $'

################################################################################
################################################################################

class JobsStats_Command(Command):
  
  __APIs__ = [ 'JobsClient' ]
  
  def doCommand(self):
    """ 
    Return getJobStats from Jobs Client  
    
   :attr:`args`: 
     - args[0]: string: should be a ValidElement

     - args[1]: string: should be the name of the ValidElement

  returns:
    {
      'MeanProcessedJobs': X
    }
    """

    super(JobsStats_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )
      
    try:

      res = self.APIs[ 'JobsClient' ].getJobsStats( self.args[0], self.args[1], self.args[2] )
      
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : S_OK( res ) }   

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class JobsEff_Command(Command):

  __APIs__ = [ 'JobsClient' ]  
  
  def doCommand(self):
    """ 
    Return getJobsEff from Jobs Client  
    
   :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string: should be the name of the ValidElement

    returns:
      {
        'JobsEff': X
      }
    """
    
    super(JobsEff_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )    

    try:
      
      res = self.APIs[ 'JobsClient' ].getJobsEff( self.args[0], self.args[1], self.args[2] )
       
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : S_OK( res ) }   

################################################################################
################################################################################

class SystemCharge_Command(Command):
  
  __APIs__ = [ 'JobsClient' ]
  
  def doCommand(self):
    """ Returns last hour system charge, and the system charge of an hour before

        returns:
          {
            'LastHour': n_lastHour
            'anHourBefore': n_anHourBefore
          }
    """
    
    super(SystemCharge_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs ) 
     
    try:
      
      res = self.APIs[ 'JobsClient' ].getSystemCharge()
       
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : S_OK( res ) }   
       
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class JobsEffSimple_Command(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'JobsClient' ]
  
  def doCommand(self ):
    """ 
    Returns simple jobs efficiency

    :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string should be the name of the ValidElement

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    
    super (JobsEffSimple_Command, self).doCommand()
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
         
      res = self.APIs[ 'JobsClient' ].getJobsSimpleEff( name )
     
      if res == None:
        res = S_OK( 'Idle' )
      else:
        res = S_OK( res[ name ] ) 
    
    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return { 'Result' : S_ERROR( _msg ) }

    return { 'Result' : res }

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class JobsEffSimpleCached_Command(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]
  
  def doCommand(self):
    """ 
    Returns simple jobs efficiency

    :attr:`args`: 
       - args[0]: string: should be a ValidElement
  
       - args[1]: string should be the name of the ValidElement

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    
    super(JobsEffSimpleCached_Command, self).doCommand()
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
                     'commandName' : 'JobsEffSimpleEveryOne',
                     'value'       : 'JE_S',
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