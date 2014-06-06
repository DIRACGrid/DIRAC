# $HeadURL:  $
''' AccountingCacheCommand
 
  The AccountingCacheCommand class is a command module that collects command 
  classes to store accounting results in the accounting cache.
  
'''

from datetime import datetime, timedelta

from DIRAC                                                  import S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.ReportsClient            import ReportsClient
from DIRAC.ConfigurationSystem.Client.Helpers               import Resources
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command             import Command
#from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities                   import CSHelpers

__RCSID__ = '$Id:  $'

################################################################################
################################################################################

#class TransferQualityByDestSplittedCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( TransferQualityByDestSplittedCommand, self ).__init__( args, clients )
#    
##    if 'ResourceStatusClient' in self.apis:
##      self.rsClient = self.apis[ 'ResourceStatusClient' ]
##    else:
##      self.rsClient = ResourceStatusClient() 
#
#    if 'ReportsClient' in self.apis:
#      self.rClient = self.apis[ 'ReportsClient' ]
#    else:
#      self.rClient = ReportsClient() 
#
#    if 'ReportGenerator' in self.apis:
#      self.rgClient = self.apis[ 'ReportGenerator' ]
#    else:
#      self.rgClient = RPCClient( 'Accounting/ReportGenerator' ) 
#  
#    self.rClient.rpcClient = self.rgClient
#  
#  def doCommand( self ):
#    """ 
#    Returns transfer quality using the DIRAC accounting system for every SE 
#    for the last self.args[0] hours 
#        
#    :params:
#      :attr:`sources`: list of source sites (when not given, take every site)
#    
#      :attr:`SEs`: list of storage elements (when not given, take every SE)
#
#    :returns:
#      
#    """
#
#    if not 'hours' in self.args:
#      return S_ERROR( 'Number of hours not specified' )
#    hours = self.args[ 'hours' ]
#
#    sites = None
#    if 'sites' in self.args:
#      sites = self.args[ 'sites' ] 
#    if sites is None:      
##FIXME: pointing to the CSHelper instead     
##      meta = { 'columns' : 'SiteName' }
##      sources = self.rsClient.getSite( meta = meta )      
##      if not sources[ 'OK' ]:
##        return sources
##      sources = [ s[0] for s in sources[ 'Value' ] ]
#      sites = CSHelpers.getSites()      
#      if not sites['OK']:
#        return sites
#      
#      sites = sites[ 'Value' ]
#      #sites = [ site[ 0 ] for site in sites[ 'Value' ] ]  
#      
#    ses = None
#    if 'ses' in self.args:
#      ses = self.args[ 'ses' ]
#    if ses is None:
##FIXME: pointing to the CSHelper instead      
##      meta = { 'columns' : 'StorageElementName' }
##      ses = self.rsClient.getStorageElement( meta = meta )
##      if not ses[ 'OK' ]:
##        return ses 
##      ses = [ se[0] for se in ses[ 'Value' ] ]
#      ses = CSHelpers.getStorageElements()      
#      if not ses['OK']:
#        return ses
#      
#      ses = ses[ 'Value' ]
#      #ses = [ se[ 0 ] for se in ses[ 'Value' ] ]  
##    if sources is None:
##      meta = { 'columns' : 'SiteName' }
##      sources = self.rsClient.getSite( meta = meta )      
##      if not sources[ 'OK' ]:
##        return sources
##      sources = [ s[0] for s in sources[ 'Value' ] ]
#  
#    if not sites + ses:
#      return S_ERROR( 'Sites + SEs is empty' )
#
#    toD   = datetime.utcnow()
#    fromD = toD - timedelta( hours = hours )
#
#    qualityAll = self.rClient.getReport( 'DataOperation', 'Quality', fromD, toD, 
#                                          { 'OperationType' : 'putAndRegister', 
#                                            'Source'        : sites + ses, 
#                                            'Destination'   : sites + ses 
#                                           }, 'Destination' )
#      
#    if not qualityAll[ 'OK' ]:
#      return qualityAll
#    qualityAll = qualityAll[ 'Value' ]
#    
#    if not 'data' in qualityAll:
#      return S_ERROR( 'Missing data key' )
#    if not 'granularity' in qualityAll:
#      return S_ERROR( 'Missing granularity key' )
#    
#    singlePlots = {}
#    for se, value in qualityAll[ 'data' ].items():
#      plot                  = {}
#      plot[ 'data' ]        = { se: value }
#      plot[ 'granularity' ] = qualityAll[ 'granularity' ]
#      singlePlots[ se ]     = plot
#    
#    return S_OK( singlePlots )
#
#################################################################################
#################################################################################
#
#class TransferQualityByDestSplittedSiteCommand( Command ):
#  
#  def __init__( self, args = None, clients = None ):
#    
#    super( TransferQualityByDestSplittedSiteCommand, self ).__init__( args, clients )
#    
#    if 'ResourceStatusClient' in self.apis:
#      self.rsClient = self.apis[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient() 
#
#    if 'ReportsClient' in self.apis:
#      self.rClient = self.apis[ 'ReportsClient' ]
#    else:
#      self.rClient = ReportsClient() 
#
#    if 'ReportGenerator' in self.apis:
#      self.rgClient = self.apis[ 'ReportGenerator' ]
#    else:
#      self.rgClient = RPCClient( 'Accounting/ReportGenerator' ) 
#  
#    self.rClient.rpcClient = self.rgClient
#  
#  def doCommand( self ):
#    """ 
#    Returns transfer quality using the DIRAC accounting system for every SE
#    of a single site for the last self.args[0] hours 
#        
#    :params:
#      :attr:`sources`: list of source sites (when not given, take every site)
#    
#      :attr:`SEs`: list of storage elements (when not given, take every SE)
#
#    :returns:
#      
#    """
#
#    if not 'hours' in self.args:
#      return S_ERROR( 'Number of hours not specified' )
#    hours = self.args[ 'hours' ]
#      
#    sites = None
#    if 'sites' in self.args:
#      sites = self.args[ 'sites' ] 
#    if sites is None:      
##FIXME: pointing to the CSHelper instead     
##      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
##      if not sources[ 'OK' ]:
##        return sources 
##      sources = [ si[0] for si in sources[ 'Value' ] ]
#      sites = CSHelpers.getSites()      
#      if not sites['OK']:
#        return sites
#      sites = sites[ 'Value' ]      
#      
#    ses = None
#    if 'ses' in self.args:
#      ses = self.args[ 'ses' ]
#    if ses is None:
##FIXME: pointing to the CSHelper instead      
##      meta = { 'columns' : 'StorageElementName' }
##      ses = self.rsClient.getStorageElement( meta = meta )
##      if not ses[ 'OK' ]:
##        return ses 
##      ses = [ se[0] for se in ses[ 'Value' ] ]
#      ses = CSHelpers.getStorageElements()      
#      if not ses['OK']:
#        return ses
#      
#      ses = ses[ 'Value' ]      
#          
#    if not sites + ses:
#      return S_ERROR( 'Sites + SEs is empty' )
# 
#    return S_ERROR( 'This guy is buggy, missing method on rsClient' )
# 
#    fromD = datetime.utcnow() - timedelta( hours = hours )
#    toD   = datetime.utcnow()
#
#    qualityAll = self.rClient.getReport( 'DataOperation', 'Quality', fromD, toD, 
#                                         { 'OperationType' : 'putAndRegister', 
#                                           'Source'        : sites + ses, 
#                                           'Destination'   : sites + ses 
#                                          }, 'Destination' )
#    if not qualityAll[ 'OK' ]:
#      return qualityAll 
#    qualityAll = qualityAll[ 'Value' ]
#    
#    if not 'data' in qualityAll:
#      return S_ERROR( 'Missing data key' )
#    listOfDest = qualityAll[ 'data' ].keys()
#    
#    if not 'granularity' in qualityAll:
#      return S_ERROR( 'Missing granularity key' )
#    plotGran = qualityAll[ 'granularity' ]
#    
#    storSitesWeb = self.rsClient.getMonitoredsStatusWeb( 'StorageElement', 
#                                                         { 'StorageElementName': listOfDest }, 0, 300 )
#    if not storSitesWeb[ 'OK' ]:
#      return storSitesWeb 
#    storSitesWeb = storSitesWeb[ 'Value' ]   
#    
#    if not 'Records' in storSitesWeb:
#      return S_ERROR( 'Missing Records key' )  
#    storSitesWeb  = storSitesWeb[ 'Records' ]
#    
#    SESiteMapping = {}
#    siteSEMapping = {}
#    
#    #FIXME: this is very likely going to explode sooner or later...
#    for r in storSitesWeb:
#      sites                   = r[ 2 ].split( ' ' )[ :-1 ]
#      SESiteMapping[ r[ 0 ] ] = sites
#      
#    for se in SESiteMapping.keys():
#      for site in SESiteMapping[ se ]:
#        try:
#          l = siteSEMapping[ site ]
#          l.append( se )
#          siteSEMapping[ site ] = l
#        except KeyError:
#          siteSEMapping[ site ] = [ se ]
#   
#    singlePlots = {}
#    
#    #FIXME: refactor it
#    for site in siteSEMapping.keys():
#      plot           = {}
#      plot[ 'data' ] = {}
#      for SE in siteSEMapping[site]:
#        plot[ 'data' ][ se ] = qualityAll[ 'data' ][ se ]
#      plot[ 'granularity' ] = plotGran
#    
#      singlePlots[ site ] = plot
#    
#    return S_OK( singlePlots )
  
################################################################################
################################################################################

#class TransferQualityBySourceSplittedSite_Command( Command ):
#  
#  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ] 
#  
#  def doCommand( self, sources = None, SEs = None ):
#    """ 
#    Returns transfer quality using the DIRAC accounting system for every SE
#    of a single site and for the site itself for the last self.args[0] hours 
#        
#    :params:
#      :attr:`dests`: list of destinations (when not given, take everything)
#    
#      :attr:`SEs`: list of storage elements (when not given, take every SE)
#
#    :returns:
#      
#    """
#  
#    super( TransferQualityBySourceSplittedSite_Command, self ).doCommand()
#    self.apis = initAPIs( self.__APIs__, self.apis )
#
#    if SEs is None:
#      SEs = self.apis[ 'ResourceStatusClient' ].getStorageElement( columns = 'StorageElementName' )
#      if not SEs[ 'OK' ]:
#      else:
#        SEs = SEs[ 'Value' ]
#    
#    if sources is None:
#      sources = self.apis[ 'ResourceStatusClient' ].getSitesList()
#      if not sources[ 'OK' ]:
#      else:
#        sources = sources[ 'Value' ]
#    
#    self.apis[ 'ReportsClient' ].rpcClient = self.apis[ 'ReportGenerator' ]
#
#    fromD = datetime.utcnow()-timedelta( hours = self.args[ 0 ] )
#    toD = datetime.utcnow()
#
#    try:
#      qualityAll = self.apis[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', fromD, toD, 
#                                          { 'OperationType':'putAndRegister', 
#                                            'Source': sources + SEs, 'Destination': sources + SEs }, 
#                                          'Destination')
#      if not qualityAll[ 'OK' ]:
#      else:
#        qualityAll = qualityAll[ 'Value' ]
#
#    except:
#      gLogger.exception( "Exception when calling TransferQualityByDestSplittedSite_Command" )
#      return {}
#    
#    listOfDest = qualityAll[ 'data' ].keys()
#    
#    try:
#      storSitesWeb = self.apis[ 'ResourceStatusClient' ].getMonitoredsStatusWeb( 'StorageElement', { 'StorageElementName': listOfDest }, 0, 300)
#    except:
#      gLogger.exception( "Exception when calling TransferQualityByDestSplittedSite_Command" )
#      return {}
#    
#    if not storSitesWeb[ 'OK' ]:
#    else:
#      storSitesWeb = storSitesWeb[ 'Value' ][ 'Records' ]
#    
#    SESiteMapping = {}
#    siteSEMapping = {}
#    
#    for r in storSitesWeb:
#      sites                   = r[ 2 ].split( ' ' )[ :-1 ]
#      SESiteMapping[ r[ 0 ] ] = sites
#      
#    for SE in SESiteMapping.keys():
#      for site in SESiteMapping[ SE ]:
#        try:
#          l = siteSEMapping[ site ]
#          l.append( SE )
#          siteSEMapping[ site ] = l
#        except KeyError:
#          siteSEMapping[ site ] = [ SE ]
#   
#    
#    plotGran = qualityAll[ 'granularity' ]
#    
#    singlePlots = {}
#    
#    for site in siteSEMapping.keys():
#      plot           = {}
#      plot[ 'data' ] = {}
#      for SE in siteSEMapping[ site ]:
#        plot[ 'data' ][ SE ] = qualityAll[ 'data' ][ SE ]
#      plot[ 'granularity' ] = plotGran
#    
#      singlePlots[ site ] = plot
#    
#    resToReturn = { 'DataOperation': singlePlots }
#
#    return resToReturn
#
#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

#class FailedTransfersBySourceSplittedCommand( Command ):
#
#  def __init__( self, args = None, clients = None ):
#    
#    super( FailedTransfersBySourceSplittedCommand, self ).__init__( args, clients )
#
#    if 'ReportsClient' in self.apis:
#      self.rClient = self.apis[ 'ReportsClient' ]
#    else:
#      self.rClient = ReportsClient() 
#
#    if 'ReportGenerator' in self.apis:
#      self.rgClient = self.apis[ 'ReportGenerator' ]
#    else:
#      self.rgClient = RPCClient( 'Accounting/ReportGenerator' ) 
#  
#    self.rClient.rpcClient = self.rgClient
#  
#  def doCommand( self):
#    """ 
#    Returns failed transfer using the DIRAC accounting system for every SE 
#    for the last self.args[0] hours 
#        
#    :params:
#      :attr:`sources`: list of source sites (when not given, take every site)
#    
#      :attr:`SEs`: list of storage elements (when not given, take every SE)
#
#    :returns:
#      
#    """
#
#    if not 'hours' in self.args:
#      return S_ERROR( 'Number of hours not specified' )
#    hours = self.args[ 'hours' ]
#      
#    sites = None
#    if 'sites' in self.args:
#      sites = self.args[ 'sites' ] 
#    if sites is None:      
##FIXME: pointing to the CSHelper instead     
##      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
##      if not sources[ 'OK' ]:
##        return sources 
##      sources = [ si[0] for si in sources[ 'Value' ] ]
#      sites = CSHelpers.getSites()      
#      if not sites['OK']:
#        return sites
#      sites = sites[ 'Value' ]      
#      
#    ses = None
#    if 'ses' in self.args:
#      ses = self.args[ 'ses' ]
#    if ses is None:
##FIXME: pointing to the CSHelper instead      
##      meta = { 'columns' : 'StorageElementName' }
##      ses = self.rsClient.getStorageElement( meta = meta )
##      if not ses[ 'OK' ]:
##        return ses 
##      ses = [ se[0] for se in ses[ 'Value' ] ]
#      ses = CSHelpers.getStorageElements()      
#      if not ses['OK']:
#        return ses
#      
#      ses = ses[ 'Value' ]      
#          
#    if not sites + ses:
#      return S_ERROR( 'Sites + SEs is empty' )
#
#    fromD = datetime.utcnow()-timedelta( hours = hours )
#    toD   = datetime.utcnow()
#
#    failedTransfers = self.rClient.getReport( 'DataOperation', 'FailedTransfers', fromD, toD, 
#                                              { 'OperationType' : 'putAndRegister', 
#                                                'Source'        : sites + ses, 
#                                                'Destination'   : sites + ses,
#                                                'FinalStatus'   : [ 'Failed' ] 
#                                               }, 'Source' )
#    if not failedTransfers[ 'OK' ]:
#      return failedTransfers
#    failedTransfers = failedTransfers[ 'Value' ]
#    
#    if not 'data' in failedTransfers:
#      return S_ERROR( 'Missing data key' )
#    if not 'granularity' in failedTransfers:
#      return S_ERROR( 'Missing granularity key' )
#    
#    singlePlots = {}
#    
#    for source, value in failedTransfers[ 'data' ].items():
#      if source in sites:
#        plot                  = {}
#        plot[ 'data' ]        = { source: value }
#        plot[ 'granularity' ] = failedTransfers[ 'granularity' ]
#        singlePlots[ source ] = plot
#    
#    return S_OK( singlePlots )

################################################################################
################################################################################

class SuccessfullJobsBySiteSplittedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( SuccessfullJobsBySiteSplittedCommand, self ).__init__( args, clients )

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' ) 
    
    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns successfull jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    sites = None
    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 
    if sites is None:      
#FIXME: pointing to the CSHelper instead     
#      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
#      if not sources[ 'OK' ]:
#        return sources 
#      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = Resources.getSites()      
      if not sites['OK']:
        return sites
      sites = sites[ 'Value' ]
    
    if not sites:
      return S_ERROR( 'Sites is empty' )

    fromD = datetime.utcnow()-timedelta( hours = hours )
    toD   = datetime.utcnow()

    successfulJobs = self.rClient.getReport( 'Job', 'NumberOfJobs', fromD, toD, 
                                             { 'FinalStatus' : [ 'Done' ], 
                                               'Site'        : sites
                                             }, 'Site' )
    if not successfulJobs[ 'OK' ]:
      return successfulJobs 
    successfulJobs = successfulJobs[ 'Value' ]
    
    if not 'data' in successfulJobs:
      return S_ERROR( 'Missing data key' ) 
    if not 'granularity' in successfulJobs:
      return S_ERROR( 'Missing granularity key' )   
    
    singlePlots = {}
    
    for site, value in successfulJobs[ 'data' ].items():
      if site in sites:
        plot                  = {}
        plot[ 'data' ]        = { site: value }
        plot[ 'granularity' ] = successfulJobs[ 'granularity' ]
        singlePlots[ site ]   = plot
    
    return S_OK( singlePlots )

################################################################################
################################################################################

class FailedJobsBySiteSplittedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( FailedJobsBySiteSplittedCommand, self ).__init__( args, clients )

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' ) 

    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns failed jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    sites = None
    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 
    if sites is None:      
#FIXME: pointing to the CSHelper instead     
#      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
#      if not sources[ 'OK' ]:
#        return sources 
#      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = Resources.getSites()      
      if not sites[ 'OK' ]:
        return sites
      sites = sites[ 'Value' ]
    
    if not sites:
      return S_ERROR( 'Sites is empty' )

    fromD = datetime.utcnow() - timedelta( hours = hours )
    toD   = datetime.utcnow()

    failedJobs = self.rClient.getReport( 'Job', 'NumberOfJobs', fromD, toD, 
                                         { 'FinalStatus' : [ 'Failed' ], 
                                            'Site'        : sites
                                         }, 'Site' )
    if not failedJobs[ 'OK' ]:
      return failedJobs 
    failedJobs = failedJobs[ 'Value' ]
    
    if not 'data' in failedJobs:
      return S_ERROR( 'Missing data key' )   
    if not 'granularity' in failedJobs:
      return S_ERROR( 'Missing granularity key' )   
    
    singlePlots = {}
    
    for site, value in failedJobs[ 'data' ].items():
      if site in sites:
        plot                  = {}
        plot[ 'data' ]        = { site: value }
        plot[ 'granularity' ] = failedJobs[ 'granularity' ]
        singlePlots[ site ]   = plot
    
    return S_OK( singlePlots )

################################################################################
################################################################################

class SuccessfullPilotsBySiteSplittedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( SuccessfullPilotsBySiteSplittedCommand, self ).__init__( args, clients )

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )

    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    sites = None
    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 
    if sites is None:      
#FIXME: pointing to the CSHelper instead     
#      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
#      if not sources[ 'OK' ]:
#        return sources 
#      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = Resources.getSites()      
      if not sites[ 'OK' ]:
        return sites
      sites = sites[ 'Value' ]
    
    if not sites:
      return S_ERROR( 'Sites is empty' )

    fromD = datetime.utcnow()-timedelta( hours = hours )
    toD   = datetime.utcnow()

    succesfulPilots = self.rClient.getReport( 'Pilot', 'NumberOfPilots', fromD, toD, 
                                              { 'GridStatus' : [ 'Done' ], 
                                                'Site'       : sites 
                                              }, 'Site' )
    if not succesfulPilots[ 'OK' ]:
      return succesfulPilots 
    succesfulPilots = succesfulPilots[ 'Value' ]
    
    if not 'data' in succesfulPilots:
      return S_ERROR( 'Missing data key' )
    if not 'granularity' in succesfulPilots:
      return S_ERROR( 'Missing granularity key' )   
    
    singlePlots = {}
    
    for site, value in succesfulPilots[ 'data' ].items():
      if site in sites:
        plot                    = {}
        plot[ 'data' ]          = { site: value }
        plot[ 'granularity' ]   = succesfulPilots[ 'granularity' ]
        singlePlots[ site ]     = plot
    
    return S_OK( singlePlots )

################################################################################
################################################################################

class FailedPilotsBySiteSplittedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( FailedPilotsBySiteSplittedCommand, self ).__init__( args, clients )

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )
 
    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns failed jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    sites = None
    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 
    if sites is None:      
#FIXME: pointing to the CSHelper instead     
#      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
#      if not sources[ 'OK' ]:
#        return sources 
#      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = Resources.getSites()      
      if not sites[ 'OK' ]:
        return sites
      sites = sites[ 'Value' ]
    
    if not sites:
      return S_ERROR( 'Sites is empty' )

    fromD = datetime.utcnow() - timedelta( hours = hours )
    toD   = datetime.utcnow()

    failedPilots = self.rClient.getReport( 'Pilot', 'NumberOfPilots', fromD, toD, 
                                            { 'GridStatus' : [ 'Aborted' ], 
                                             'Site'       : sites
                                            }, 'Site' )
    if not failedPilots[ 'OK' ]:
      return failedPilots 
    failedPilots = failedPilots[ 'Value' ]
       
    if not 'data' in failedPilots:
      return S_ERROR( 'Missing data key' )
    if not 'granularity' in failedPilots:
      return S_ERROR( 'Missing granularity key' ) 
    
    singlePlots = {}

    for site, value in failedPilots[ 'data' ].items():
      if site in sites:
        plot                  = {}
        plot[ 'data' ]        = { site: value }
        plot[ 'granularity' ] = failedPilots[ 'granularity' ] 
        singlePlots[ site ]   = plot
    
    return S_OK( singlePlots )

################################################################################
################################################################################

class SuccessfullPilotsByCESplittedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( SuccessfullPilotsByCESplittedCommand, self ).__init__( args, clients )
    
    self.resources = Resources.Resources()
    
    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )

    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every CE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    ces = None
    if 'ces' in self.args:
      ces = self.args[ 'ces' ] 
    if ces is None:      
#FIXME: pointing to the CSHelper instead     
#      meta = {'columns':'ResourceName'}
#      CEs = self.rsClient.getResource( resourceType = [ 'CE','CREAMCE' ], meta = meta )
#      if not CEs['OK']:
#        return CEs 
#      CEs = [ ce[0] for ce in CEs['Value'] ]
     
      ces = self.resources.getEligibleResources( 'Computing' )
      if not ces[ 'OK' ]:
        return ces
      ces = ces[ 'Value' ]
    
    if not ces:
      return S_ERROR( 'CEs is empty' )

    fromD = datetime.utcnow() - timedelta( hours = hours )
    toD   = datetime.utcnow()

    successfulPilots = self.rClient.getReport( 'Pilot', 'NumberOfPilots', fromD, toD, 
                                          { 'GridStatus' : [ 'Done' ], 
                                            'GridCE'     : ces
                                          }, 'GridCE' )
    if not successfulPilots[ 'OK' ]:
      return successfulPilots 
    successfulPilots = successfulPilots[ 'Value' ]
    
    if not 'data' in successfulPilots:
      return S_ERROR( 'Missing data key' )
    if not 'granularity' in successfulPilots:
      return S_ERROR( 'Missing granularity key' )    
    
    singlePlots = {}
    
    for ce, value in successfulPilots[ 'data' ].items():
      if ce in ces:
        plot                  = {}
        plot[ 'data' ]        = { ce : value }
        plot[ 'granularity' ] = successfulPilots[ 'granularity' ]
        singlePlots[ ce ]     = plot
    
    return S_OK( singlePlots )

################################################################################
################################################################################

class FailedPilotsByCESplittedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( FailedPilotsByCESplittedCommand, self ).__init__( args, clients )

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )

    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns failed pilots using the DIRAC accounting system for every CE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    ces = None
    if 'ces' in self.args:
      ces = self.args[ 'ces' ] 
    if ces is None:      
#FIXME: pointing to the CSHelper instead     
#      meta = {'columns':'ResourceName'}
#      CEs = self.rsClient.getResource( resourceType = [ 'CE','CREAMCE' ], meta = meta )
#      if not CEs['OK']:
#        return CEs 
#      CEs = [ ce[0] for ce in CEs['Value'] ]
     
      ces = CSHelpers.getComputingElements()      
      if not ces[ 'OK' ]:
        return ces
      ces = ces[ 'Value' ]
    
    if not ces:
      return S_ERROR( 'CEs is empty' )

    fromD = datetime.utcnow() - timedelta( hours = hours )
    toD   = datetime.utcnow()

    failedPilots = self.rClient.getReport( 'Pilot', 'NumberOfPilots', fromD, toD, 
                                            { 'GridStatus' : [ 'Aborted' ], 
                                              'GridCE'     : ces 
                                            }, 'GridCE' )
    if not failedPilots[ 'OK' ]:
      return failedPilots
    failedPilots = failedPilots[ 'Value' ]
    
    if not 'data' in failedPilots:
      return S_ERROR( 'Missing data key' )
    if not 'granularity' in failedPilots:
      return S_ERROR( 'Missing granularity key' )
    
    singlePlots   = {}

    for ce, value in failedPilots[ 'data' ].items():
      if ce in ces:
        plot                  = {}
        plot[ 'data' ]        = { ce : value } 
        plot[ 'granularity' ] = failedPilots[ 'granularity' ]
        singlePlots[ ce ]     = plot
    
    return S_OK( singlePlots )

################################################################################
################################################################################

class RunningJobsBySiteSplittedCommand( Command ):

  def __init__( self, args = None, clients = None ):
    
    super( RunningJobsBySiteSplittedCommand, self ).__init__( args, clients )

    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )

    self.rClient.rpcClient = self.rgClient
  
  def doCommand( self ):
    """ 
    Returns running and runned jobs, querying the WMSHistory  
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every sites)

    :returns:
      
    """

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    sites = None
    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 
    if sites is None:      
#FIXME: pointing to the CSHelper instead     
#      sources = self.rsClient.getSite( meta = {'columns': 'SiteName'} )
#      if not sources[ 'OK' ]:
#        return sources 
#      sources = [ si[0] for si in sources[ 'Value' ] ]
      sites = Resources.getSites()      
      if not sites[ 'OK' ]:
        return sites
      sites = sites[ 'Value' ]
    
    if not sites:
      return S_ERROR( 'Sites is empty' )   

    fromD = datetime.utcnow() - timedelta( hours = hours )
    toD   = datetime.utcnow()

    runJobs = self.rClient.getReport( 'WMSHistory', 'NumberOfJobs', fromD, toD, 
                                       {}, 'Site')
    if not runJobs[ 'OK' ]:
      return runJobs 
    runJobs    = runJobs[ 'Value' ]
    
    if not 'data' in runJobs:
      return S_ERROR( 'Missing data key' )
    if not 'granularity' in runJobs:
      return S_ERROR( 'Missing granularity key' )
    
    singlePlots = {}
    
    for site, value in runJobs[ 'data' ].items():
      if site in sites:
        plot                  = {}
        plot[ 'data' ]        = { site: value }
        plot[ 'granularity' ] = runJobs[ 'granularity' ]
        singlePlots[ site ]   = plot
    
    return S_OK( singlePlots )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF