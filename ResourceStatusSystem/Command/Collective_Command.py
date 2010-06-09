""" The Collective_Command class is a command class to know about collective results 
    (to be cached)
"""

from datetime import datetime, timedelta

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

#############################################################################

class JobsEffSimpleEveryOne_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns simple jobs efficiency for all the sites in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}
    """
#    super(JobsEffSimpleEveryOne_Command, self).doCommand()
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.JobsClient import JobsClient   
      self.client = JobsClient()

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient("ResourceStatus/ResourceStatus")
      sites = RPC.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    
    try:
      res = self.client.getJobsSimpleEff(sites, self.RPC)
    except:
      gLogger.exception("Exception when calling JobsClient.")
      return {}
    
    return res

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__


#############################################################################

class PilotsEffSimpleEverySites_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns simple pilots efficiency for all the sites and resources in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}
    """
#    super(PilotsEffSimpleEverySites_Command, self).doCommand()
    
    if self.client is None:
      from DIRAC.ResourceStatusSystem.Client.PilotsClient import PilotsClient   
      self.client = PilotsClient()

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient("ResourceStatus/ResourceStatus")
      sites = RPC.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + res['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("WorkloadManagement/WMSAdministrator")
    
    try:
      res = self.client.getPilotsSimpleEff('Site', sites, None, self.RPC)
    except:
      gLogger.exception("Exception when calling PilotsClient.")
      return {}
    
    return res

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__


#############################################################################


class TransferQualityEverySEs_Command(Command):
  
  def doCommand(self, SEs = None):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE 
        
    :params:
      :attr:`SEs`: list of storage elements (when not given, take every SE)
    
      :attr:`RPCWMSAdmin`: optional RPCClient to RPCWMSAdmin

    :returns:
      {'SiteName': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}
    """
#    super(TransferQualityEverySEs_Command, self).doCommand()

    if SEs is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      SEs = RPC_RSS.getStorageElementsList()
      if not SEs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + SEs['Message'] 
      else:
        SEs = SEs['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.utcnow()-timedelta(hours = 2)
    toD = datetime.utcnow()

    try:
      qualityAll = self.client.getReport('DataOperation', 'Quality', fromD, toD, 
                                         {'OperationType':'putAndRegister', 
                                          'Destination':SEs}, 'Channel')
      if not qualityAll['OK']:
        raise RSSException, where(self, self.doCommand) + " " + qualityAll['Message'] 
      else:
        qualityAll = qualityAll['Value']['data']

    except:
      gLogger.exception("Exception when calling TransferQualityEverySEs_Command")
      return {}
    
    listOfDestSEs = []
    
    for k in qualityAll.keys():
      try:
        key = k.split(' -> ')[1]
        if key not in listOfDestSEs:
          listOfDestSEs.append(key)
      except:
        continue

    meanQuality = {}

    for destSE in listOfDestSEs:
      s = 0
      n = 0
      for k in qualityAll.keys():
        try:
          if k.split(' -> ')[1] == destSE:
            n = n + len(qualityAll[k])
            s = s + sum(qualityAll[k].values())
        except:
          continue
      meanQuality[destSE] = s/n
      
    return meanQuality

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################
