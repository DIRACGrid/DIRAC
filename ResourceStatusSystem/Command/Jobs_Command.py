################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

""" 
  The Jobs_Command class is a command class to know about 
  present jobs efficiency
"""

from DIRAC                                           import gLogger

from DIRAC.ResourceStatusSystem.Command.Command      import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs    import initAPIs
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities.Utils      import where

################################################################################
################################################################################

class JobsStats_Command(Command):
  
  __APIs__ = [ 'JobsClient' ]
  
  def doCommand(self):
    """ 
    Return getJobStats from Jobs Client  
    
   :attr:`args`: 
     - args[0]: string: should be a ValidRes

     - args[1]: string: should be the name of the ValidRes

  returns:
    {
      'MeanProcessedJobs': X
    }
    """

    super(JobsStats_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )
      
    try:
      res = self.APIs[ 'JobsClient' ].getJobsStats(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling JobsClient for %s %s" %(self.args[0], self.args[1]))
      return {'Result':'Unknown'}
    
    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class JobsEff_Command(Command):

  __APIs__ = [ 'JobsClient' ]  
  
  def doCommand(self):
    """ 
    Return getJobsEff from Jobs Client  
    
   :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string: should be the name of the ValidRes

    returns:
      {
        'JobsEff': X
      }
    """
    
    super(JobsEff_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )    

    try:
      res = self.APIs[ 'JobsClient' ].getJobsEff(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling JobsClient")
      return {'Result':'Unknown'}
    
    return {'Result':res}

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
    except:
      gLogger.exception("Exception when calling JobsClient")
      return {'Result':'Unknown'}
    
    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class JobsEffSimple_Command(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'JobsClient' ]
  
  def doCommand(self ):
    """ 
    Returns simple jobs efficiency

    :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string should be the name of the ValidRes

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    
    super (JobsEffSimple_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )
    
    if self.args[0] == 'Service':
      try:
        name = self.APIs[ 'ResourceStatusClient' ].getGeneralName(self.args[0], self.args[1], 'Site')['Value'][0]
      except:
        gLogger.error("JobsEffSimple_Command: Can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'Result':'Unknown'}      
      granularity = 'Site'
    elif self.args[0] == 'Site':
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)
         
    try:
      res = self.APIs[ 'JobsClient' ].getJobsSimpleEff( name )#, timeout = self.timeout)
      if res == None:
        return {'Result':'Idle'}
    except:
      gLogger.exception("Exception when calling JobsClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}
    
    return {'Result':res[name]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
################################################################################

class JobsEffSimpleCached_Command(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'ResourceManagementClient' ]
  
  def doCommand(self):
    """ 
    Returns simple jobs efficiency

    :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string should be the name of the ValidRes

    returns:
      {
        'Result': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """
    
    super(JobsEffSimpleCached_Command, self).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )
      
    if self.args[0] == 'Service':
      try:
        name = self.APIs[ 'ResourceStatusClient' ].getGeneralName( self.args[0], self.args[1], 'Site' )['Value'][0]
      except:
        gLogger.error("JobsEffSimpleCached_Command: can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'Result':'Unknown'}      
      granularity = 'Site'
    elif self.args[0] == 'Site':
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
      
      clientDict = { 
                     'name'        : name,
                     'commandName' : 'JobsEffSimpleEveryOne',
                     'value'       : 'JE_S',
                     'opt_ID'      : 'NULL',
                     'meta'        : { 'columns'     : 'Result' }
                   }
      
      res = self.APIs[ 'ResourceManagementClient' ].getClientCache( **clientDict )[ 'Value' ]
      if res == None:
        return {'Result':'Idle'}
      if res == []:
        return {'Result':'Idle'}
    except:
      gLogger.exception("Exception when calling ResourceStatusClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}
    
    return {'Result':res[0]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF