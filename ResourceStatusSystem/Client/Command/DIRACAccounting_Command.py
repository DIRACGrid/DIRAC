""" The DIRACAccounting_Command class is a command class to 
    interrogate the DIRAC Accounting.
"""

from datetime import datetime, timedelta

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Client.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *

#############################################################################

class DIRACAccounting_Command(Command):
  
  def doCommand(self, args, clientIn=None):
    """ Returns jobs accounting info for sites in the last 24h
        
       :params:
         :attr:`args`: 
           - args[0]: string - should be a ValidRes
           
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
          
    try:

      res = rc.getReport(accounting, plot, fromT, toT, conditions, grouping)
          
      if res['OK']:
        return {plot:res['Value']}
      else:
        raise RSSException, where(self, self.doCommand) + ' ' + res['Message'] 

    except:
      gLogger.exception("Exception when calling ReportsClient for " + granularity + " " + name )
      return {plot:'Unknown'}


#############################################################################

class TransferQuality_Command(Command):

  def doCommand(self, args, clientIn=None):
    """ Return getQuality from DIRAC's accounting ReportsClient
    
        :params:
          :attr:`args`: a tuple
            - args[0]: string: should be a ValidRes
      
            - args[1]: string should be the name of the ValidRes

            - args[2]: optional dateTime object: a "from" date
          
            - args[3]: optional dateTime object: a "to" date
          
        :returns:
          {'TransferQuality': None | a number between 0 and 1}
    """
    
    if not isinstance(args, tuple):
      raise TypeError, where(self, self.doCommand)
    
    if args[0] not in ('StorageElement', 'StorageElements'):
       raise InvalidRes, where(self, self.doCommand)

    if clientIn is not None:
      rc = clientIn
    else:
      # use standard Client
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      rc = ReportsClient()

    try:
      if args[2] is None:
        fromD = datetime.utcnow()-timedelta(hours = 2)
      else:
        fromD = args[2]
    except:
      fromD = datetime.utcnow()-timedelta(hours = 2)
    try:
      if args[3] is None:
        toD = datetime.utcnow()
      else:
        toD = args[3]
    except:
      toD = datetime.utcnow()

    try:
      pr_quality = rc.getReport('DataOperation', 'Quality', fromD, toD, 
                                {'OperationType':'putAndRegister', 'Destination':[args[1]]}, 'Channel')
      if not pr_quality['OK']:
        raise RSSException, where(self, self.getQualityStats) + " " + pr_quality['Message'] 

    except:
      gLogger.exception("Exception when calling ReportsClient for %s %s" %(args[0], args[1]))
      return {'TransferQuality':'Unknown'}
    
    pr_q_d = pr_quality['Value']['data']
    
    if pr_q_d == {}:
      return {'TransferQuality':None}
    else:
      if len(pr_q_d) == 1:
        values = []
        for k in pr_q_d.keys():
          for n in pr_q_d[k].values():
            values.append(n)
        return {'TransferQuality':sum(values)/len(values)}
      else:
        values = []
        for n in pr_q_d['Total'].values():
          values.append(n)
        return {'TransferQuality':sum(values)/len(values)} 
  
#############################################################################
