""" The DIRACAccounting_Command class is a command class to 
    interrogate the DIRAC Accounting.
"""

from datetime import datetime, timedelta

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

#############################################################################

class DIRACAccounting_Command(Command):
  
  def doCommand(self):
    """ 
    Returns jobs accounting info for sites in the last 24h
    `args`: 
       - args[0]: string - should be a ValidRes
       
       - args[1]: string - should be the name of the ValidRes
       
       - args[2]: string - should be 'Job' or 'Pilot' or 'DataOperation'
         or 'WMSHistory' (??) or 'SRM' (??)
       
       - args[3]: string - should be the plot to generate (e.g. CPUEfficiency) 
       
       - args[4]: dictionary - e.g. {'Format': 'LastHours', 'hours': 24}
       
       - args[5]: string - should be the grouping
       
       - args[6]: dictionary - optional conditions
    """
    super(DIRACAccounting_Command, self).doCommand()
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    granularity = self.args[0]
    name = self.args[1]
    accounting = self.args[2]
    plot = self.args[3]
    period = self.args[4]
    if period['Format'] == 'LastHours':
      fromT = datetime.utcnow()-timedelta(hours = period['hours'])
      toT = datetime.utcnow()
    elif period['Format'] == 'Periods':
      #TODO
      pass
    grouping = self.args[5]
    try:
      if self.args[6] is not None:
        conditions = self.args[6]
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

      res = self.client.getReport(accounting, plot, fromT, toT, conditions, grouping)
          
      if res['OK']:
        return {'Result':res['Value']}
      else:
        raise RSSException, where(self, self.doCommand) + ' ' + res['Message'] 

    except:
      gLogger.exception("Exception when calling ReportsClient for " + granularity + " " + name )
      return {'Result':'Unknown'}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class TransferQuality_Command(Command):

  def doCommand(self):
    """ 
    Return getQuality from DIRAC's accounting ReportsClient
    
    `args`: a tuple
      - args[0]: string: should be a ValidRes

      - args[1]: string should be the name of the ValidRes

      - args[2]: optional dateTime object: a "from" date
    
      - args[3]: optional dateTime object: a "to" date
      
    :returns:
      {'Result': None | a float between 0.0 and 100.0}
    """
    super(TransferQuality_Command, self).doCommand()
   
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    try:
      if self.args[2] is None:
        fromD = datetime.utcnow()-timedelta(hours = 2)
      else:
        fromD = self.args[2]
    except:
      fromD = datetime.utcnow()-timedelta(hours = 2)
    try:
      if self.args[3] is None:
        toD = datetime.utcnow()
      else:
        toD = self.args[3]
    except:
      toD = datetime.utcnow()

    try:
      pr_quality = self.client.getReport('DataOperation', 'Quality', fromD, toD, 
                                         {'OperationType':'putAndRegister', 
                                          'Destination':[self.args[1]]}, 'Channel')
      
      if not pr_quality['OK']:
        raise RSSException, where(self, self.doCommand) + " " + pr_quality['Message'] 

    except:
      gLogger.exception("Exception when calling ReportsClient for %s %s" %(self.args[0], self.args[1]))
      return {'Result':'Unknown'}
    
    pr_q_d = pr_quality['Value']['data']
    
    if pr_q_d == {}:
      return {'Result':None}
    else:
      if len(pr_q_d) == 1:
        values = []
        for k in pr_q_d.keys():
          for n in pr_q_d[k].values():
            values.append(n)
        return {'Result':sum(values)/len(values)}
      else:
        values = []
        for n in pr_q_d['Total'].values():
          values.append(n)
        return {'Result':sum(values)/len(values)} 
  
  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################

class TransferQualityCached_Command(Command):
  
  def doCommand(self):
    """ 
    Returns transfer quality as it is cached

    :attr:`args`: 
       - args[0]: string: should be a ValidRes
  
       - args[1]: string should be the name of the ValidRes

    :returns:
      {'Result': None | a float between 0.0 and 100.0}
    """
    super(TransferQualityCached_Command, self).doCommand()

    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.client = ResourceStatusClient(timeout = self.timeout)
      
    name = self.args[1]
    
    try:
      res = self.client.getCachedResult(name, 'TransferQualityEverySEs', 'TQ', 'NULL')
      if res == []:
        return {'Result':None}
    except:
      gLogger.exception("Exception when calling ResourceStatusClient for %s" %(name))
      return {'Result':'Unknown'}
    
    return {'Result':float(res[0])}

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__
    
#############################################################################
