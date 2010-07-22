""" The AccountingCache_Command class is a command module that collects command classes to store
    accounting results in the accounting cache.
"""

from datetime import datetime, timedelta

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Command.Command import Command
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *

#############################################################################

class TransferQualityByDestSplitted_Command(Command):
  
  def doCommand(self, sources = None, SEs = None):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE 
    for the last 2 hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """

    if SEs is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      SEs = RPC_RSS.getStorageElementsList()
      if not SEs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + SEs['Message'] 
      else:
        SEs = SEs['Value']
    
    if sources is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sources = RPC_RSS.getSitesList()
      if not sources['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sources['Message'] 
      else:
        sources = sources['Value']
    
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
                                          'Source':sources, 'Destination':SEs}, 
                                          'Destination')
      if not qualityAll['OK']:
        raise RSSException, where(self, self.doCommand) + " " + qualityAll['Message'] 
      else:
        qualityAll = qualityAll['Value']

    except:
      gLogger.exception("Exception when calling TransferQualityByDestSplitted_Command")
      return {}
    
    listOfDestSEs = qualityAll['data'].keys()
    
    plotGran = qualityAll['granularity']
    
    singlePlots = {}
    
    for SE in listOfDestSEs:
      plot = {}
      plot['data'] = {SE: qualityAll['data'][SE]}
      plot['granularity'] = plotGran
      singlePlots[SE] = plot
    
    resToReturn = {'DataOperation': singlePlots}

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class FailedTransfersBySourceSplitted_Command(Command):
  
  def doCommand(self, sources = None, SEs = None):
    """ 
    Returns failed transfer using the DIRAC accounting system for every SE 
    for the last 2 hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """

    if SEs is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      SEs = RPC_RSS.getStorageElementsList()
      if not SEs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + SEs['Message'] 
      else:
        SEs = SEs['Value']
    
    if sources is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sources = RPC_RSS.getSitesList()
      if not sources['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sources['Message'] 
      else:
        sources = sources['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.utcnow()-timedelta(hours = 2)
    toD = datetime.utcnow()

    try:
      ft_source = self.client.getReport('DataOperation', 'FailedTransfers', 
                                         fromD, toD, 
                                         {'OperationType':'putAndRegister', 
                                          'Source':sources, 'Destination':SEs,
                                          'FinalStatus':['Failed']}, 
                                         'Source')
      if not ft_source['OK']:
        raise RSSException, where(self, self.doCommand) + " " + ft_source['Message'] 
      else:
        ft_source = ft_source['Value']

    except:
      gLogger.exception("Exception when calling FailedTransfersBySourceSplitted_Command")
      return {}
    
    listOfSources = ft_source['data'].keys()
    
    plotGran = ft_source['granularity']
    
    singlePlots = {}
    
    for source in listOfSources:
      if source in sources:
        plot = {}
        plot['data'] = {source: ft_source['data'][source]}
        plot['granularity'] = plotGran
        singlePlots[source] = plot
    
    resToReturn = {'DataOperation': singlePlots}

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class SuccessfullJobsBySiteSplitted_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns successfull jobs using the DIRAC accounting system for every site 
    for the last 24 hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sources = RPC_RSS.getSitesList()
      if not sources['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sources['Message'] 
      else:
        sources = sources['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.utcnow()-timedelta(hours = 24)
    toD = datetime.utcnow()

    try:
      succ_jobs = self.client.getReport('Job', 'NumberOfJobs', fromD, toD, 
                                        {'FinalStatus':['Done']}, 'Site')
      if not succ_jobs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + succ_jobs['Message'] 
      else:
        succ_jobs = succ_jobs['Value']

    except:
      gLogger.exception("Exception when calling SuccessfullJobsBySiteSplitted_Command")
      return {}
    
    listOfSites = succ_jobs['data'].keys()
    
    plotGran = succ_jobs['granularity']
    
    singlePlots = {}
    
    for site in listOfSites:
      plot = {}
      plot['data'] = {site: succ_jobs['data'][site]}
      plot['granularity'] = plotGran
      singlePlots[site] = plot
    
    resToReturn = {'Job': singlePlots}

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class FailedJobsBySiteSplitted_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns failed jobs using the DIRAC accounting system for every site 
    for the last 24 hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sources = RPC_RSS.getSitesList()
      if not sources['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sources['Message'] 
      else:
        sources = sources['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.utcnow()-timedelta(hours = 24)
    toD = datetime.utcnow()

    try:
      failed_jobs = self.client.getReport('Job', 'NumberOfJobs', fromD, toD, 
                                          {'FinalStatus':['Failed']}, 'Site')
      if not failed_jobs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + failed_jobs['Message'] 
      else:
        failed_jobs = failed_jobs['Value']

    except:
      gLogger.exception("Exception when calling FailedJobsBySiteSplitted_Command")
      return {}
    
    listOfSites = failed_jobs['data'].keys()
    
    plotGran = failed_jobs['granularity']
    
    singlePlots = {}
    
    for site in listOfSites:
      plot = {}
      plot['data'] = {site: failed_jobs['data'][site]}
      plot['granularity'] = plotGran
      singlePlots[site] = plot
    
    resToReturn = {'Job': singlePlots}

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

