""" The Jobs_Command class is a command class to know about 
    present jobs efficiency
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class JobsStats_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getJobStats from Jobs Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      c = JobsClient()
      
    return c.getJobsStats(args[0], args[1], args[2])


#############################################################################

class JobsEff_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Return getJobsEff from Jobs Client  
    """

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      c = JobsClient()
      
    return c.getJobsEff(args[0], args[1], args[2])


#############################################################################

class SystemCharge_Command(Command):
  
  def doCommand(self, clientIn=None):
    """ Returns last hour system charge, and the system charge of an hour before

        returns:
          {
            'LastHour': n_lastHour
            'anHourBefore': n_anHourBefore
          }
    """

    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      c = JobsClient()
      
    return c.getSystemCharge()

#############################################################################

class JobsEffSimple_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Returns simple jobs efficiency
        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes

        returns:
          {
            'jobsEff': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'
          }
    """

    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.doCommand)
    
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if clientIn is not None:
      c = clientIn
    else:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      c = JobsClient()
      
    return c.getJobsSimpleEff(args[0], args[1])

#############################################################################
