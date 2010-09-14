""" The AccountingCache_Command class is a command module that collects command classes to store
    accounting results in the accounting cache.
"""

import datetime

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

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 2)
    toD = datetime.datetime.utcnow()

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

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 2)
    toD = datetime.datetime.utcnow()

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
      sites = RPC_RSS.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 24)
    toD = datetime.datetime.utcnow()

    try:
      succ_jobs = self.client.getReport('Job', 'NumberOfJobs', fromD, toD, 
                                        {'FinalStatus':['Done'], 'Site':sites}, 'Site')
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
      if site in sites:
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
      sites = RPC_RSS.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 24)
    toD = datetime.datetime.utcnow()

    try:
      failed_jobs = self.client.getReport('Job', 'NumberOfJobs', fromD, toD, 
                                          {'FinalStatus':['Failed'], 'Site':sites}, 'Site')
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
      if site in sites:
        plot = {}
        plot['data'] = {site: failed_jobs['data'][site]}
        plot['granularity'] = plotGran
        singlePlots[site] = plot
    
    resToReturn = {'Job': singlePlots}

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class SuccessfullPilotsBySiteSplitted_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every site 
    for the last 24 hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if sites is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sites = RPC_RSS.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 24)
    toD = datetime.datetime.utcnow()

    try:
      succ_pilots = self.client.getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                        {'GridStatus':['Done'], 'Site':sites}, 'Site')
      if not succ_pilots['OK']:
        raise RSSException, where(self, self.doCommand) + " " + succ_pilots['Message'] 
      else:
        succ_pilots = succ_pilots['Value']

    except:
      gLogger.exception("Exception when calling SuccessfullPilotsBySiteSplitted_Command")
      return {}
    
    listOfSites = succ_pilots['data'].keys()
    
    plotGran = succ_pilots['granularity']
    
    singlePlots = {}
    
    for site in listOfSites:
      if site in sites:
        plot = {}
        plot['data'] = {site: succ_pilots['data'][site]}
        plot['granularity'] = plotGran
        singlePlots[site] = plot
    
    resToReturn = {'Pilot': singlePlots}

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class FailedPilotsBySiteSplitted_Command(Command):
  
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
      sites = RPC_RSS.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = sites['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 24)
    toD = datetime.datetime.utcnow()

    try:
      failed_pilots = self.client.getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                          {'GridStatus':['Aborted'], 'Site':sites}, 'Site')
      if not failed_pilots['OK']:
        raise RSSException, where(self, self.doCommand) + " " + failed_pilots['Message'] 
      else:
        failed_pilots = failed_pilots['Value']

    except:
      gLogger.exception("Exception when calling FailedPilotsBySiteSplitted_Command")
      return {}
    
    listOfSites = failed_pilots['data'].keys()
    
    plotGran = failed_pilots['granularity']
    
    singlePlots = {}

    for site in listOfSites:
      if site in sites:
        plot = {}
        plot['data'] = {site: failed_pilots['data'][site]}
        plot['granularity'] = plotGran
        singlePlots[site] = plot
    
    resToReturn = {'Pilot': singlePlots}

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class SuccessfullPilotsByCESplitted_Command(Command):
  
  def doCommand(self, CEs = None):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every CE 
    for the last 24 hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """

    if CEs is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      CEs = RPC_RSS.getCEsList()
      if not CEs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + CEs['Message'] 
      else:
        CEs = CEs['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 24)
    toD = datetime.datetime.utcnow()

    try:
      succ_pilots = self.client.getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                          {'GridStatus':['Done'], 'GridCE':CEs}, 'GridCE')
      if not succ_pilots['OK']:
        raise RSSException, where(self, self.doCommand) + " " + succ_pilots['Message'] 
      else:
        succ_pilots = succ_pilots['Value']

    except:
      gLogger.exception("Exception when calling SuccessfullPilotsByCESplitted_Command")
      return {}
    
    listOfCEs = succ_pilots['data'].keys()
    
    plotGran = succ_pilots['granularity']
    
    singlePlots = {}
    
    for CE in listOfCEs:
      if CE in CEs:
        plot = {}
        plot['data'] = {CE: succ_pilots['data'][CE]}
        plot['granularity'] = plotGran
        singlePlots[CE] = plot
    
    resToReturn = {'Pilot': singlePlots}

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

class FailedPilotsByCESplitted_Command(Command):
  
  def doCommand(self, CEs = None):
    """ 
    Returns failed pilots using the DIRAC accounting system for every CE 
    for the last 24 hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """

    if CEs is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      CEs = RPC_RSS.getCEsList()
      if not CEs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + CEs['Message'] 
      else:
        CEs = CEs['Value']
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = 24)
    toD = datetime.datetime.utcnow()

    try:
      failed_pilots = self.client.getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                            {'GridStatus':['Aborted'], 'GridCE':CEs}, 'GridCE')
      if not failed_pilots['OK']:
        raise RSSException, where(self, self.doCommand) + " " + failed_pilots['Message'] 
      else:
        failed_pilots = failed_pilots['Value']

    except:
      gLogger.exception("Exception when calling FailedPilotsByCESplitted_Command")
      return {}
    
    listOfCEs = failed_pilots['data'].keys()
    
    plotGran = failed_pilots['granularity']
    
    singlePlots = {}

    for CE in listOfCEs:
      if CE in CEs:
        plot = {}
        plot['data'] = {CE: failed_pilots['data'][CE]}
        plot['granularity'] = plotGran
        singlePlots[CE] = plot
    
    resToReturn = {'Pilot': singlePlots}

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

#############################################################################

