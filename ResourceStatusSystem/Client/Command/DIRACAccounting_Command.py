""" The DIRACAccounting_Command class is a command class to 
    interrogate the DIRAC Accounting.
"""

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

from datetime import datetime, timedelta

#############################################################################

class DIRACAccounting_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Returns jobs accounting info for sites in the last 24h
        
       :params:
         :attr:`args`: 
           - args[1]: string - should be a ValidRes
           
           - args[1]: string - should be the name of the ValidRes
           
           - args[2]: string - should be 'Job' or 'Pilot' or 'DataOperation'
             or 'WMSHistory' (??) or 'SRM' (??)
           
           - args[3]: string - should be the plot to generate (e.g. CPUEfficiency) 
           
           - args[4]: dictionary - e.g. {'Format': 'LastHours', 'hours': 24}
           
           - args[5]: string - should be the grouping
           
           - args[6]: dictionary - optional conditions
    """
    
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)

    if args[0] not in ValidRes:
       raise InvalidRes, where(self, self.doCommand)

    if clientIn is not None:
      rc = clientIn
    else:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      rc = ReportsClient()

    print args

    granularity = args[0]
    name = args[1]
    accounting = args[2]
    plot = args[3]
    period = args[4]
    if period['Format'] == 'LastHours':
      fromT = datetime.utcnow()-timedelta(hours = period['hours'])
      toT = datetime.utcnow()
    elif period['Format'] == 'Periods':
      #TODO
      pass
    grouping = args[5]
    try:
      if args[6] is not None:
        conditions = args[6]
      else:
        raise Exception
    except:
      conditions = {}
    if accounting == 'Job' or accounting == 'Pilot':
      if granularity == 'Resource':
        conditions['GridCE'] = [name]
      elif granularity == 'Service':
        conditions['Site'] = [name.split('@').pop()]
      elif granularity == 'Site':
        conditions['Site'] = [name]
      else:
        raise InvalidRes, where(self, self.doCommand)
    elif accounting == 'DataOperation':
      conditions['Destination'] = [name]
          
    res = rc.getReport(accounting, plot, fromT, toT, conditions, grouping)
    
    print res
    
    
    if res['OK']:
      return res['Value']
    else:
      raise RSSException, where(self, self.doCommand) 

#############################################################################
