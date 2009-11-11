""" The PilotsEfficiency_Policy class is a policy class 
    that checks the efficiency of the pilots
"""

from DIRAC.ResourceStatusSystem.Policy.PolicyBase import PolicyBase
from DIRAC.ResourceStatusSystem.Client.Command.ClientsInvoker import ClientsInvoker
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

class PilotsEfficiency_Policy(PolicyBase):
  
  def evaluate(self, args, knownInfo=None, commandPeriods=None, commandStats=None, commandEff=None):
    """ evaluate policy on pilots stats, using args (tuple). 
        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        - args[2] should be the present status
        
        returns:
            { 
              'SAT':True|False, 
              'Status':Active|Probing|Banned, 
              'Reason':'PilotsEff:low|PilotsEff:med|PilotsEff:good',
            }
    """ 

    if not isinstance(args, tuple):
      raise TypeError, where(self, self.evaluate)
    
    if args[0] not in ValidRes:
      raise InvalidRes, where(self, self.evaluate)
    
    if args[2] not in ValidStatus:
      raise InvalidStatus, where(self, self.evaluate)

    if knownInfo is not None:
      if 'PilotsEff' in knownInfo.keys():
        status = knownInfo
    else:
      
      if args[2] == 'Banned':
        return {'SAT':None}
      
      periods = self._getPeriods(args, commandIn=commandPeriods)
      pilotsStats = self._getPilotsStats((args[0], args[1]), periods, commandIn=commandStats)
      periodsForPilotsEff = self._getPeriods(args, pilotsStats['MeanProcessedPilots'], commandIn=commandPeriods)
      if len(periodsForPilotsEff) != 1:
        return {'SAT':None}
      status = self._getPilotsEff((args[0], args[1]), periodsForPilotsEff, commandIn=commandEff)
      
      result = {}
      
      if pilotsStats['MeanProcessedPilots'] < ( 3 * pilotsStats['LastProcessedPilots'] ):
        # unusual amount of pilots
        if status['PilotsEff'] < Configurations.GOOD_PILOTS_EFFICIENCY:
          if args[2] == 'Active':
            result['SAT'] = True
            result['Status'] = 'Probing'
            result['Reason'] = 'PilotsSaturation'
          elif args[2] == 'Probing':
            result['SAT'] = False
            result['Status'] = 'Probing'
            result['Reason'] = 'PilotsSaturation'
        else:
          if args[2] == 'Active':
            result['SAT'] = False
            result['Status'] = 'Active'
            result['Reason'] = 'PilotsEff:good'
          elif args[2] == 'Probing':
            result['SAT'] = True
            result['Status'] = 'Active'
            result['Reason'] = 'PilotsEff:good'
        return result
      
    result = {}
    #standard situation
    if args[2] == 'Active':
      if status['PilotsEff'] > Configurations.GOOD_PILOTS_EFFICIENCY:
        result['SAT'] = False
        result['Status'] = 'Active'
        result['Reason'] = 'PilotsEff:good'
      else:
        if status['PilotsEff'] < Configurations.MEDIUM_PILOTS_EFFICIENCY:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'PilotsEff:low'
        elif status['PilotsEff'] > Configurations.MEDIUM_PILOTS_EFFICIENCY and status['PilotsEff'] < Configurations.GOOD_PILOTS_EFFICIENCY:
          result['SAT'] = True
          result['Status'] = 'Probing'
          result['Reason'] = 'PilotsEff:med'
    
    elif args[2] == 'Probing':
      if status['PilotsEff'] > Configurations.GOOD_PILOTS_EFFICIENCY: 
        result['SAT'] = True
        result['Status'] = 'Active'
        result['Reason'] = 'PilotsEff:good'
      else:
        if status['PilotsEff'] < MEDIUM_PILOTS_EFFICIENCY:
          result['SAT'] = True
          result['Status'] = 'Banned'
          result['Reason'] = 'PilotsEff:low'
        elif status['PilotsEff'] > Configurations.MEDIUM_PILOTS_EFFICIENCY and status['PilotsEff'] < Configurations.GOOD_PILOTS_EFFICIENCY:
          result['SAT'] = False
          result['Status'] = 'Probing'
          result['Reason'] = 'PilotsEff:med'
    
    return result




  def _getPeriods(self, args, meanProcessedPilots=None, commandIn=None):
    """ Returns a list of periods of time where args[1] was in status args[2]

        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        - args[2] should be the present status

        - meanProcessedPilots determines the periods window

        returns:
          {
            'Periods': [list of periods]
          }
    """

    if meanProcessedPilots is not None:
      if meanProcessedPilots > Configurations.HIGH_PILOTS_NUMBER:
        hours = Configurations.SHORT_PILOTS_PERIOD_WINDOW
      elif  meanProcessedPilots < Configurations.HIGH_PILOTS_NUMBER and meanProcessedPilots > Configurations.MEDIUM_PILOTS_NUMBER:
        hours = Configurations.MEDIUM_PILOTS_PERIOD_WINDOW
      else:
        hours = Configurations.LARGE_PILOTS_PERIOD_WINDOW
    else:
      hours = Configurations.MAX_PILOTS_PERIOD_WINDOW

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
    

      
  def _getPilotsStats(self, args, periods, commandIn=None):
    """ Returns pilots stats invoking pilots client

        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes

        - periods contains the periods to consider in the query

        returns:
          {
            'MeanProcessedPilots': X'
            'LastProcessedPilots': X'
          }
    """
    
    if commandIn is not None:
      command = commandIn
    else:
      # use standard command
      from DIRAC.ResourceStatusSystem.Client.Command.Pilots_Command import PilotsStats_Command
      command = PilotsStats_Command()
      
    clientsInvoker = ClientsInvoker()
    clientsInvoker.setCommand(command)
    status = clientsInvoker.doCommand(args + (periods, ))
    
    return status


    
  def _getPilotsEff(self, args, periods, commandIn = None):
    """ Returns pilots efficiency invoking pilots client

        - args[0] should be a ValidRes
        - args[1] should be the name of the ValidRes
        
        - periods contains the periods to consider in the query

        returns:
          {
            'PilotsEff': X (0-1)'
          }
    """
    
    if commandIn is not None:
      command = commandIn
    else:
      # use standard command
      from DIRAC.ResourceStatusSystem.Client.Command.Pilots_Command import PilotsEff_Command
      command = PilotsEff_Command()
      
    clientsInvoker = ClientsInvoker()
    clientsInvoker.setCommand(command)
    pilotsEff = clientsInvoker.doCommand(args + (periods, ))

    return pilotsEff
