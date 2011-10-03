""" The Jobs_Command class is a command class to know about 
    present jobs efficiency
"""

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Command.Command import *
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

#############################################################################

class JobsStats_Command(Command):
  
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
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      self.client = JobsClient()
      
    try:
      res = self.client.getJobsStats(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling JobsClient for %s %s" %(self.args[0], self.args[1]))
      return {'Result':'Unknown'}
    
    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class JobsEff_Command(Command):
  
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

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      self.client = JobsClient()
      
    try:
      res = self.client.getJobsEff(self.args[0], self.args[1], self.args[2])
    except:
      gLogger.exception("Exception when calling JobsClient")
      return {'Result':'Unknown'}
    
    return {'Result':res}


#############################################################################

class SystemCharge_Command(Command):
  
  def doCommand(self):
    """ Returns last hour system charge, and the system charge of an hour before

        returns:
          {
            'LastHour': n_lastHour
            'anHourBefore': n_anHourBefore
          }
    """
    super(SystemCharge_Command, self).doCommand()

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      self.client = JobsClient()
      
    try:
      res = self.client.getSystemCharge()
    except:
      gLogger.exception("Exception when calling JobsClient")
      return {'Result':'Unknown'}
    
    return {'Result':res}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class JobsEffSimple_Command(Command):
  
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

    if self.args[0] == 'Service':
      if self.rsClient is None:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
        self.rsClient = ResourceStatusClient()
      try:
        name = self.rsClient.getGeneralName(self.args[0], self.args[1], 'Site')['Value'][0]
      except:
        gLogger.error("JobsEffSimple_Command: Can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'Result':'Unknown'}      
      granularity = 'Site'
    elif self.args[0] == 'Site':
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      self.client = JobsClient()
      
    try:
      res = self.client.getJobsSimpleEff(name, timeout = self.timeout)
      if res == None:
        return {'Result':'Idle'}
    except:
      gLogger.exception("Exception when calling JobsClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}
    
    return {'Result':res[name]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class JobsEffSimpleCached_Command(Command):
  
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

    client = self.client

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()
      
    if self.args[0] == 'Service':
      try:
        name = self.rsClient.getGeneralName( self.args[0], self.args[1], 'Site' )['Value'][0]
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
      if client is None:  
        from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
        self.client = ResourceManagementClient(timeout = self.timeout)
      
      res = self.client.getCachedResult(name, 'JobsEffSimpleEveryOne', 'JE_S', 'NULL')['Value']
      if res == None:
        return {'Result':'Idle'}
      if res == []:
        return {'Result':'Idle'}
    except:
      gLogger.exception("Exception when calling ResourceStatusClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}
    
    return {'Result':res[0]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################
