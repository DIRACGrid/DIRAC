""" The Jobs_Command class is a command class to know about 
    present jobs efficiency
"""

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

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
  
  def doCommand(self, RSClientIn = None):
    """ 
    Returns simple jobs efficiency

    :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string should be the name of the ValidRes

    returns:
      {
        'jobsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """

    if self.args[0] in ('Service', 'Services'):
      if RSClientIn is not None:
        rsc = RSClientIn
      else:
        from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient   
        rsc = ResourceStatusClient()
      try:
        name = rsc.getGeneralName(self.args[0], self.args[1], 'Site')
      except:
        gLogger.error("Can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'Result':'Unknown'}      
      granularity = 'Site'
    elif self.args[0] in ('Site', 'Sites'):
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      self.client = JobsClient()
      
    try:
      res = self.client.getJobsSimpleEff(name)
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
        'jobsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
      }
    """

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.client = ResourceStatusClient()
      
    if self.args[0] in ('Service', 'Services'):
      try:
        name = self.client.getGeneralName(self.args[0], self.args[1], 'Site')
      except:
        gLogger.error("Can't get a general name for %s %s" %(self.args[0], self.args[1]))
        return {'Result':'Unknown'}      
      granularity = 'Site'
    elif self.args[0] in ('Site', 'Sites'):
      name = self.args[1]
      granularity = self.args[0]
    else:
      raise InvalidRes, where(self, self.doCommand)
    
    try:
      res = self.client.getCachedResult(name, 'JobsEffSimpleEveryOne')
      if res == None:
        return {'Result':'Idle'}
      if res == []:
        return {'Result':'Unknown'}
    except:
      gLogger.exception("Exception when calling ResourceStatusClient for %s %s" %(granularity, name))
      return {'Result':'Unknown'}
    
    return {'Result':res[0]}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################
