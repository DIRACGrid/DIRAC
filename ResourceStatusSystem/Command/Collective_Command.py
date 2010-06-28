""" The Collective_Command class is a command class to know about collective results 
    (to be cached)
"""

from datetime import datetime, timedelta

from DIRAC import gLogger
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName, getDIRACSiteName

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
      {'SiteName': {'JE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}, ...}
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
    
    resToReturn = {}
    
    for site in res:
      resToReturn[site] = {'JE_S': res[site]}
    
    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__


#############################################################################

class PilotsEffSimpleEverySites_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns simple pilots efficiency for all the sites and resources in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName':  {'PE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
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
    
    resToReturn = {}
    
    for site in res:
      resToReturn[site] = {'PE_S': res[site]}
    
    return resToReturn


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
      {'SiteName': {TQ : 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
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
      
    resToReturn = {}
    
    for se in meanQuality:
      resToReturn[se] = {'TQ': meanQuality[se]}
    
    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################


class DTEverySites_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns downtimes information for all the sites in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName': {'SEVERITY': 'OUTAGE'|'AT_RISK', 
                    'StartDate': 'aDate', ...} ... }
    """
#    super(PilotsEffSimpleEverySites_Command, self).doCommand()
    
    if self.client is None:
      from DIRAC.Core.LCG.GOCDBClient import GOCDBClient   
      self.client = GOCDBClient()

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient("ResourceStatus/ResourceStatus")
      sites = RPC.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = sites['Value']
    
    GOC_sites = []
    for site in sites:
      GOC_site = getGOCSiteName(site)
      if GOC_site['OK']:
        GOC_sites.append(GOC_site['Value'])
    
    try:
      res = self.client.getStatus('Site', GOC_sites, None, 120)
    except:
      gLogger.exception("Exception when calling GOCDBClient.")
      return {}
    
    if not res['OK']:
      raise RSSException, where(self, self.doCommand) + " " + res['Message']
    else:
      res = res['Value']
    
    if res == None:
      return {}
    
    resToReturn = {}
    
    for dt_ID in res:
      dt = {}
      dt['ID'] = dt_ID
      dt['StartDate'] = res[dt_ID]['FORMATED_START_DATE']
      dt['EndDate'] = res[dt_ID]['FORMATED_END_DATE']
      dt['Severity'] = res[dt_ID]['SEVERITY']
      dt['Description'] = res[dt_ID]['DESCRIPTION'].replace('\'', '')
      DIRACname = getDIRACSiteName(res[dt_ID]['SITENAME'])['Value']
      resToReturn[DIRACname] = dt
    
    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################


class DTEveryResources_Command(Command):
  
  def doCommand(self, resources = None):
    """ 
    Returns downtimes information for all the resources in input.
        
    :params:
      :attr:`sites`: list of resource names (when not given, take every resource)
    
    :returns:
      {'ResourceName': {'SEVERITY': 'OUTAGE'|'AT_RISK', 
                    'StartDate': 'aDate', ...} ... }
    """
#    super(PilotsEffSimpleEverySites_Command, self).doCommand()
    
    if self.client is None:
      from DIRAC.Core.LCG.GOCDBClient import GOCDBClient   
      self.client = GOCDBClient()

    if resources is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC = RPCClient("ResourceStatus/ResourceStatus")
      resources = RPC.getResourcesList()
      if not resources['OK']:
        raise RSSException, where(self, self.doCommand) + " " + resources['Message'] 
      else:
        resources = resources['Value']
    
    try:
      res = self.client.getStatus('Resource', resources, None, 120)
    except:
      gLogger.exception("Exception when calling GOCDBClient.")
      return {}
    
    if not res['OK']:
      raise RSSException, where(self, self.doCommand) + " " + res['Message']
    else:
      res = res['Value']
    
    if res == None:
      return {}
    
    resToReturn = {}
    
    for dt_ID in res:
      dt = {}
      dt['ID'] = dt_ID
      dt['StartDate'] = res[dt_ID]['FORMATED_START_DATE']
      dt['EndDate'] = res[dt_ID]['FORMATED_END_DATE']
      dt['Severity'] = res[dt_ID]['SEVERITY']
      dt['Description'] = res[dt_ID]['DESCRIPTION'].replace('\'', '')
      resToReturn[res[dt_ID]['HOSTNAME']] = dt
    
    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################
