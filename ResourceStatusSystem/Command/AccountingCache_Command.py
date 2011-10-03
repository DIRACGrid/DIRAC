""" The AccountingCache_Command class is a command module that collects command classes to store
    accounting results in the accounting cache.
"""

import datetime

from DIRAC import gLogger

from DIRAC.ResourceStatusSystem.Command.Command import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

from DIRAC.Core.DISET.RPCClient import RPCClient

################################################################################

class TransferQualityByDestSplitted_Command( Command ):
  
  def doCommand( self, sources = None, SEs = None ):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if SEs is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      SEs = self.rsClient.getStorageElementsList()
      if not SEs[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + SEs[ 'Message' ] 
      else:
        SEs = [ se[0] for se in SEs[ 'Value' ] ]
    
    if sources is None:
#      from DIRAC.Core.DISET.RPCClient import RPCClient
#      RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      sources = self.rsClient.getSitesList()
      if not sources[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + sources[ 'Message' ] 
      else:
        sources = [ s[0] for s in sources[ 'Value' ] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient( "Accounting/ReportGenerator", timeout = self.timeout )
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient( rpcClient = self.RPC )

    fromD = datetime.datetime.utcnow()-datetime.timedelta( hours = self.args[ 0 ] )
    toD   = datetime.datetime.utcnow()

    try:
      qualityAll = self.client.getReport( 'DataOperation', 'Quality', fromD, toD, 
                                          { 'OperationType':'putAndRegister', 
                                          'Source': sources + SEs, 'Destination': sources + SEs }, 
                                          'Destination' )
      
      if not qualityAll[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + qualityAll[ 'Message' ] 
      else:
        qualityAll = qualityAll[ 'Value' ]

    except:
      gLogger.exception( "Exception when calling TransferQualityByDestSplitted_Command" ) 
      return {}
    
    listOfDestSEs = qualityAll[ 'data' ].keys()
    
    plotGran = qualityAll[ 'granularity' ]
    
    singlePlots = {}
    
    for SE in listOfDestSEs:
      plot                  = {}
      plot[ 'data' ]        = { SE: qualityAll[ 'data' ][ SE ] }
      plot[ 'granularity' ] = plotGran
      singlePlots[ SE ]     = plot
    
    resToReturn = { 'DataOperation': singlePlots }

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################

class TransferQualityByDestSplittedSite_Command( Command ):
  
  def doCommand( self, sources = None, SEs = None ):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE
    of a single site for the last self.args[0] hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if SEs is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      SEs = self.rsClient.getStorageElementsList()
      if not SEs[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + SEs[ 'Message' ] 
      else:
        SEs = [ se[0] for se in SEs[ 'Value' ] ]
    
    if sources is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      sources = self.rsClient.getSitesList()
      if not sources[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + sources[ 'Message' ] 
      else:
        sources = [ si[0] for si in sources[ 'Value' ] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient( "Accounting/ReportGenerator", timeout = self.timeout )
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient( rpcClient = self.RPC )

    fromD = datetime.datetime.utcnow()-datetime.timedelta( hours = self.args[ 0 ] )
    toD = datetime.datetime.utcnow()

    try:
      qualityAll = self.client.getReport( 'DataOperation', 'Quality', fromD, toD, 
                                          {'OperationType':'putAndRegister', 
                                        'Source':sources + SEs, 'Destination':sources + SEs }, 
                                        'Destination' )
      if not qualityAll[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + qualityAll[ 'Message' ] 
      else:
        qualityAll = qualityAll[ 'Value' ]

    except:
      gLogger.exception( "Exception when calling TransferQualityByDestSplittedSite_Command" )
      return {}
    
    listOfDest = qualityAll[ 'data' ].keys()
    
    try:
      storSitesWeb = self.rsClient.getStorageElementsStatusWeb( { 'StorageElementName': listOfDest }, 0, 300 )
    except NameError:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      storSitesWeb = self.rsClient.getStorageElementsStatusWeb( { 'StorageElementName': listOfDest }, 0, 300 )
    
    if not storSitesWeb[ 'OK' ]:
      raise RSSException, where( self, self.doCommand ) + " " + storSitesWeb[ 'Message' ] 
    else:
      storSitesWeb = storSitesWeb[ 'Value' ][ 'Records' ]
    
    SESiteMapping = {}
    siteSEMapping = {}
    
    for r in storSitesWeb:
      sites               = r[ 2 ].split( ' ' )[ :-1 ]
      SESiteMapping[ r[ 0 ] ] = sites
      
    for SE in SESiteMapping.keys():
      for site in SESiteMapping[ SE ]:
        try:
          l = siteSEMapping[ site ]
          l.append( SE )
          siteSEMapping[ site ] = l
        except KeyError:
          siteSEMapping[ site ] = [ SE ]
   
    
    plotGran = qualityAll[ 'granularity' ]
    
    singlePlots = {}
    
    for site in siteSEMapping.keys():
      plot           = {}
      plot[ 'data' ] = {}
      for SE in siteSEMapping[site]:
        plot[ 'data' ][ SE ] = qualityAll[ 'data' ][ SE ]
      plot[ 'granularity' ] = plotGran
    
      singlePlots[ site ] = plot
    
    resToReturn = { 'DataOperation': singlePlots }

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################

class TransferQualityBySourceSplittedSite_Command( Command ):
  
  def doCommand( self, sources = None, SEs = None ):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE
    of a single site and for the site itself for the last self.args[0] hours 
        
    :params:
      :attr:`dests`: list of destinations (when not given, take everything)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if SEs is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      SEs = self.rsClient.getStorageElementsList( 'Read' )
      if not SEs[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + SEs[ 'Message' ] 
      else:
        SEs = SEs[ 'Value' ]
    
    if sources is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      sources = self.rsClient.getSitesList()
      if not sources[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + sources[ 'Message' ] 
      else:
        sources = sources[ 'Value' ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient( "Accounting/ReportGenerator", timeout = self.timeout )
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient( rpcClient = self.RPC )

    fromD = datetime.datetime.utcnow()-datetime.timedelta( hours = self.args[ 0 ] )
    toD = datetime.datetime.utcnow()

    try:
      qualityAll = self.client.getReport( 'DataOperation', 'Quality', fromD, toD, 
                                          { 'OperationType':'putAndRegister', 
                                            'Source': sources + SEs, 'Destination': sources + SEs }, 
                                          'Destination')
      if not qualityAll[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + qualityAll[ 'Message' ] 
      else:
        qualityAll = qualityAll[ 'Value' ]

    except:
      gLogger.exception( "Exception when calling TransferQualityByDestSplittedSite_Command" )
      return {}
    
    listOfDest = qualityAll[ 'data' ].keys()
    
    try:
      storSitesWeb = self.rsClient.getStorageElementsStatusWeb( { 'StorageElementName': listOfDest },
                                                             [], 0, 300, 'Read' )
    except NameError:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      storSitesWeb = self.rsClient.getStorageElementsStatusWeb( { 'StorageElementName': listOfDest },
                                                             [], 0, 300, 'Read' )
    
    if not storSitesWeb[ 'OK' ]:
      raise RSSException, where( self, self.doCommand ) + " " + storSitesWeb[ 'Message' ] 
    else:
      storSitesWeb = storSitesWeb[ 'Value' ][ 'Records' ]
    
    SESiteMapping = {}
    siteSEMapping = {}
    
    for r in storSitesWeb:
      sites                   = r[ 2 ].split( ' ' )[ :-1 ]
      SESiteMapping[ r[ 0 ] ] = sites
      
    for SE in SESiteMapping.keys():
      for site in SESiteMapping[ SE ]:
        try:
          l = siteSEMapping[ site ]
          l.append( SE )
          siteSEMapping[ site ] = l
        except KeyError:
          siteSEMapping[ site ] = [ SE ]
   
    
    plotGran = qualityAll[ 'granularity' ]
    
    singlePlots = {}
    
    for site in siteSEMapping.keys():
      plot           = {}
      plot[ 'data' ] = {}
      for SE in siteSEMapping[ site ]:
        plot[ 'data' ][ SE ] = qualityAll[ 'data' ][ SE ]
      plot[ 'granularity' ] = plotGran
    
      singlePlots[ site ] = plot
    
    resToReturn = { 'DataOperation': singlePlots }

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################

class FailedTransfersBySourceSplitted_Command( Command ):
  
  def doCommand( self, sources = None, SEs = None ):
    """ 
    Returns failed transfer using the DIRAC accounting system for every SE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if SEs is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      SEs = self.rsClient.getStorageElementsList()
      if not SEs[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + SEs[ 'Message' ] 
      else:
        SEs = [ se[0] for se in SEs[ 'Value' ] ]
    
    if sources is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient( "ResourceStatus/ResourceStatus" )
      sources = self.rsClient.getSitesList()
      if not sources[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + sources[ 'Message' ] 
      else:
        sources = [ si[0] for si in sources[ 'Value' ] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient( "Accounting/ReportGenerator", timeout = self.timeout )
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient( rpcClient = self.RPC )

    fromD = datetime.datetime.utcnow()-datetime.timedelta( hours = self.args[ 0 ] )
    toD = datetime.datetime.utcnow()

    try:
      ft_source = self.client.getReport( 'DataOperation', 'FailedTransfers', 
                                         fromD, toD, 
                                         { 'OperationType':'putAndRegister', 
                                           'Source': sources + SEs, 'Destination': sources + SEs,
                                         'FinalStatus':[ 'Failed' ] }, 
                                         'Source' )
      if not ft_source[ 'OK' ]:
        raise RSSException, where( self, self.doCommand ) + " " + ft_source[ 'Message' ] 
      else:
        ft_source = ft_source[ 'Value' ]

    except:
      gLogger.exception( "Exception when calling FailedTransfersBySourceSplitted_Command" )
      return {}
    
    listOfSources = ft_source[ 'data' ].keys()
    
    plotGran = ft_source[ 'granularity' ]
    
    singlePlots = {}
    
    for source in listOfSources:
      if source in sources:
        plot                  = {}
        plot[ 'data' ]        = { source: ft_source[ 'data' ][ source ] }
        plot[ 'granularity' ] = plotGran
        singlePlots[ source ] = plot
    
    resToReturn = { 'DataOperation': singlePlots }

    return resToReturn

  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################

class SuccessfullJobsBySiteSplitted_Command( Command ):
  
  def doCommand(self, sites = None):
    """ 
    Returns successfull jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if sites is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sites = self.rsClient.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = [ si[0] for si in sites['Value'] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = self.args[0])
    toD   = datetime.datetime.utcnow()

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

################################################################################

class FailedJobsBySiteSplitted_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns failed jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if sites is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sites = self.rsClient.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = [ si[0] for si in sites['Value'] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = self.args[0])
    toD   = datetime.datetime.utcnow()

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

################################################################################

class SuccessfullPilotsBySiteSplitted_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if sites is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sites = self.rsClient.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = [ si[0] for si in sites['Value'] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = self.args[0])
    toD   = datetime.datetime.utcnow()

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

################################################################################

class FailedPilotsBySiteSplitted_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns failed jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if sites is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sites = self.rsClient.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = [ si[0] for si in sites['Value'] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = self.args[0])
    toD   = datetime.datetime.utcnow()

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

################################################################################

class SuccessfullPilotsByCESplitted_Command(Command):
  
  def doCommand(self, CEs = None):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every CE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if CEs is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      CEs = self.rsClient.getCEsList()
      if not CEs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + CEs['Message'] 
      else:
        CEs = [ ce[0] for ce in CEs['Value'] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = self.args[0])
    toD   = datetime.datetime.utcnow()

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

################################################################################

class FailedPilotsByCESplitted_Command(Command):
  
  def doCommand(self, CEs = None):
    """ 
    Returns failed pilots using the DIRAC accounting system for every CE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if CEs is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      CEs = self.rsClient.getCEsList()
      if not CEs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + CEs['Message'] 
      else:
        CEs = [ ce[0] for ce in CEs['Value'] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = self.args[0])
    toD   = datetime.datetime.utcnow()

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

################################################################################

class RunningJobsBySiteSplitted_Command(Command):
  
  def doCommand(self, sites = None):
    """ 
    Returns running and runned jobs, querying the WMSHistory  
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every sites)

    :returns:
      
    """

    if self.rsClient is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsClient = ResourceStatusClient()

    if sites is None:
      #from DIRAC.Core.DISET.RPCClient import RPCClient
      #RPC_RSS = RPCClient("ResourceStatus/ResourceStatus")
      sites = self.rsClient.getSitesList()
      if not sites['OK']:
        raise RSSException, where(self, self.doCommand) + " " + sites['Message'] 
      else:
        sites = [ si[0] for si in sites['Value'] ]
    
    if self.RPC is None:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.RPC = RPCClient("Accounting/ReportGenerator", timeout = self.timeout)
      
    if self.client is None:
      from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
      self.client = ReportsClient(rpcClient = self.RPC)

    fromD = datetime.datetime.utcnow()-datetime.timedelta(hours = self.args[0])
    toD   = datetime.datetime.utcnow()

    try:
      run_jobs = self.client.getReport('WMSHistory', 'NumberOfJobs', fromD, toD, 
                                       {}, 'Site')
      if not run_jobs['OK']:
        raise RSSException, where(self, self.doCommand) + " " + run_jobs['Message'] 
      else:
        run_jobs = run_jobs['Value']

    except:
      gLogger.exception("Exception when calling RunningJobsBySiteSplitted_Command")
      return {}
    
    listOfSites = run_jobs['data'].keys()
    
    plotGran = run_jobs['granularity']
    
    singlePlots = {}
    
    for site in listOfSites:
      if site in sites:
        plot = {}
        plot['data'] = {site: run_jobs['data'][site]}
        plot['granularity'] = plotGran
        singlePlots[site] = plot
    
    resToReturn = {'WMSHistory': singlePlots}

    return resToReturn


  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF