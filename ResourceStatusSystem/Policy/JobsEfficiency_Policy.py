""" The JobsEfficiency_Policy class is a policy class 
    that checks the efficiency of the jobs
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

class JobsEfficiency_Policy(PolicyBase):
  
  def evaluate(self, args, knownInfo=None, commandPeriods=None, commandStats=None, 
               commandEff=None, commandCharge=None):
    """ evaluate policy on jobs stats, using args (tuple). 
        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        - args[2] should be the present status
        
        returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'JobsEff:low|JobsEff:med|JobsEff:good',
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'JobsEff' in knownInfo.keys():
        status = knownInfo
    else:

      if args[2] == 'Banned':
        return {'SAT':None}
      
      systemCharge = self._getSystemCharge((), commandIn=commandCharge)
      if systemCharge['LastHour'] > 3*systemCharge['anHourBefore']:
        return {'SAT':None}
      
      periods = self._getPeriods(args, commandIn=commandPeriods)
      jobsStats = self._getJobsStats((args[0], args[1]), periods, commandIn=commandStats)
      periodsForJobsEff = self._getPeriods(args, jobsStats['MeanProcessedJobs'], 
                                           commandIn=commandPeriods)
      if len(periodsForJobsEff) != 1:
        return {'SAT':None}
      status = self._getJobsEff((args[0], args[1]), periodsForJobsEff, commandIn=commandEff)
      
    result = {}
    if args[2] == 'Active':
      if status['JobsEff'] > Configurations.GOOD_JOBS_EFFICIENCY:
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'JobsEff:good'
      else:
        if status['JobsEff'] < Configurations.MEDIUM_JOBS_EFFICIENCY:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'JobsEff:low'
        elif status['JobsEff'] > Configurations.MEDIUM_JOBS_EFFICIENCY and status['JobsEff'] < Configurations.GOOD_JOBS_EFFICIENCY:
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['Reason'] = 'JobsEff:med'
    
    elif args[2] == 'Probing':
      if status['JobsEff'] > Configurations.GOOD_JOBS_EFFICIENCY: 
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'JobsEff:good'
      else:
        if status['JobsEff'] < Configurations.MEDIUM_JOBS_EFFICIENCY:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'JobsEff:low'
        elif status['JobsEff'] > Configurations.MEDIUM_JOBS_EFFICIENCY and status['JobsEff'] < Configurations.GOOD_JOBS_EFFICIENCY:
          result['SAT'] = False
          result['Status'] = 'Probing'
          result['Reason'] = 'JobsEff:med'
    
    return result




  def _getPeriods(self, args, meanProcessedJobs=None, commandIn=None):
    """ Returns a list of periods of time where args[1] was in status args[2]

        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        - args[2] should be the present status

        - meanProcessedJobs determines the periods window

        returns:
          {
            'Periods': [list of periods]
          }
    """

    if meanProcessedJobs is not None:
      if meanProcessedJobs > Configurations.HIGH_JOBS_NUMBER:
        hours = Configurations.SHORT_JOBS_PERIOD_WINDOW
      elif  meanProcessedJobs < Configurations.HIGH_JOBS_NUMBER and meanProcessedJobs > Configurations.MEDIUM_JOBS_NUMBER:
        hours = Configurations.MEDIUM_JOBS_PERIOD_WINDOW
      else:
        hours = Configurations.LARGE_JOBS_PERIOD_WINDOW
    else:
      hours = Configurations.MAX_JOBS_PERIOD_WINDOW

    if commandIn is not None:
      command = commandIn
    else:
      # use standard command
      from DIRAC.ResourceStatusSystem.Client.Command.RS_Command import RSPeriods_Command
      command = RSPeriods_Command()
      
    clientsInvoker = ClientsInvoker()
    clientsInvoker.setCommand(command)
    periods = clientsInvoker.doCommand(args + (hours, ))
    
    return periods
    

      
  def _getJobsStats(self, args, periods, commandIn=None):
    """ Returns jobs stats invoking jobs client

        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes

        - periods contains the periods to consider in the query

        returns:
          {
            'MeanProcessedJobs': X'
            'LastProcessedJobs': X'
          }
    """
    
    if commandIn is not None:
      command = commandIn
    else:
      # use standard command
      from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import JobsStats_Command
      command = JobsStats_Command()
      
    clientsInvoker = ClientsInvoker()
    clientsInvoker.setCommand(command)
    status = clientsInvoker.doCommand(args + (periods, ))
    
    return status


    
  def _getJobsEff(self, args, periods, commandIn = None):
    """ Returns jobs efficiency invoking jobs client

        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        
        - periods contains the periods to consider in the query

        returns:
          {
            'JobsEff': X (0-1)'
          }
    """
    
    if commandIn is not None:
      command = commandIn
    else:
      # use standard command
      from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import JobsEff_Command
      command = JobsEff_Command()
      
    clientsInvoker = ClientsInvoker()
    clientsInvoker.setCommand(command)
    jobsEff = clientsInvoker.doCommand(args + (periods, ))

    return jobsEff

#############################################################################

  def _getSystemCharge(self, args, commandIn=None):
    """ Returns last hour system charge, and the system charge of an hour before

        returns:
          {
            'LastHour': n_lastHour
            'anHourBefore': n_anHourBefore
          }
    """

    if commandIn is not None:
      command = commandIn
    else:
      # use standard command
      from DIRAC.ResourceStatusSystem.Client.Command.Jobs_Command import SystemCharge_Command
      command = SystemCharge_Command()
      
    clientsInvoker = ClientsInvoker()
    clientsInvoker.setCommand(command)
    SystemCharge = clientsInvoker.doCommand(args)

    return SystemCharge
