# $HeadURL $
''' AccountingCacheCommand
 
  The AccountingCacheCommand class is a command module that collects command 
  classes to store accounting results in the accounting cache.
  
'''

from datetime                                     import datetime, timedelta

from DIRAC                                        import gLogger, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command   import *
from DIRAC.ResourceStatusSystem.Command.knownAPIs import initAPIs
#from DIRAC.ResourceStatusSystem.Utilities.Utils   import where

__RCSID__ = '$Id: $'

################################################################################
################################################################################

class TransferQualityByDestSplittedCommand( Command ):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]  
  
  def doCommand( self, sources = None, SEs = None ):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """

#    super( TransferQualityByDestSplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if SEs is None:
        meta = { 'columns' : 'StorageElementName' }
        SEs = self.APIs[ 'ResourceStatusClient' ].getStorageElement( meta = meta )
        if not SEs[ 'OK' ]:
          return SEs 
        SEs = [ se[0] for se in SEs[ 'Value' ] ]
    
      if sources is None:
        meta = { 'columns' : 'SiteName' }
        sources = self.APIs[ 'ResourceStatusClient' ].getSite( meta = meta )      
        if not sources[ 'OK' ]:
          return sources
        sources = [ s[0] for s in sources[ 'Value' ] ]
    
      if not sources + SEs:
        return S_ERROR( 'Sources + SEs is empty' )
    
      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      toD   = datetime.utcnow()
      fromD = toD - timedelta( hours = self.args[ 0 ] )

      qualityAll = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', 
                                                    fromD, toD, 
                                                    { 'OperationType':'putAndRegister', 
                                                      'Source': sources + SEs, 
                                                      'Destination': sources + SEs 
                                                    }, 
                                                    'Destination' )
      
      if not qualityAll[ 'OK' ]:
        return qualityAll
      
      qualityAll    = qualityAll[ 'Value' ]
      listOfDestSEs = qualityAll[ 'data' ].keys()
      plotGran      = qualityAll[ 'granularity' ]
      singlePlots   = {}
    
      for SE in listOfDestSEs:
        plot                  = {}
        plot[ 'data' ]        = { SE: qualityAll[ 'data' ][ SE ] }
        plot[ 'granularity' ] = plotGran
        singlePlots[ SE ]     = plot
    
      res = S_OK( { 'DataOperation' : singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class TransferQualityByDestSplittedSiteCommand( Command ):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]   
  
  def doCommand( self, sources = None, SEs = None ):
    """ 
    Returns transfer quality using the DIRAC accounting system for every SE
    of a single site for the last self.args[0] hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """
   
#    super( TransferQualityByDestSplittedSite_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      
      if SEs is None:
        SEs = self.APIs[ 'ResourceStatusClient' ].getStorageElement( meta = {'columns': 'StorageElementName' })
        if not SEs[ 'OK' ]:
          return SEs 
        SEs = [ se[0] for se in SEs[ 'Value' ] ]
    
      if sources is None:
        sources = self.APIs[ 'ResourceStatusClient' ].getSite( meta = {'columns': 'SiteName'} )
        if not sources[ 'OK' ]:
          return sources 
        sources = [ si[0] for si in sources[ 'Value' ] ]
    
      if not sources + SEs:
        return S_ERROR( 'Sources + SEs is empty' )
    
      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]
 
      fromD = datetime.utcnow() - timedelta( hours = self.args[ 0 ] )
      toD   = datetime.utcnow()

      qualityAll = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', 
                                                    fromD, toD, 
                                                    {'OperationType':'putAndRegister', 
                                                     'Source':sources + SEs, 
                                                     'Destination':sources + SEs 
                                                     }, 
                                                     'Destination' )
      if not qualityAll[ 'OK' ]:
        return qualityAll 
      
      qualityAll = qualityAll[ 'Value' ]
      listOfDest = qualityAll[ 'data' ].keys()
    
      storSitesWeb = self.APIs[ 'ResourceStatusClient' ].getMonitoredsStatusWeb( 'StorageElement', 
                                                                { 'StorageElementName': listOfDest }, 0, 300 )
    
      if not storSitesWeb[ 'OK' ]:
        return storSitesWeb 
      
      storSitesWeb  = storSitesWeb[ 'Value' ][ 'Records' ]
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
    
      res = S_OK( { 'DataOperation': singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

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
#    self.APIs = initAPIs( self.__APIs__, self.APIs )
#
#    if SEs is None:
#      SEs = self.APIs[ 'ResourceStatusClient' ].getStorageElement( columns = 'StorageElementName' )
#      if not SEs[ 'OK' ]:
#      else:
#        SEs = SEs[ 'Value' ]
#    
#    if sources is None:
#      sources = self.APIs[ 'ResourceStatusClient' ].getSitesList()
#      if not sources[ 'OK' ]:
#      else:
#        sources = sources[ 'Value' ]
#    
#    self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]
#
#    fromD = datetime.utcnow()-timedelta( hours = self.args[ 0 ] )
#    toD = datetime.utcnow()
#
#    try:
#      qualityAll = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', fromD, toD, 
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
#      storSitesWeb = self.APIs[ 'ResourceStatusClient' ].getMonitoredsStatusWeb( 'StorageElement', { 'StorageElementName': listOfDest }, 0, 300)
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

class FailedTransfersBySourceSplittedCommand( Command ):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ] 
  
  def doCommand( self, sources = None, SEs = None ):
    """ 
    Returns failed transfer using the DIRAC accounting system for every SE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sources`: list of source sites (when not given, take every site)
    
      :attr:`SEs`: list of storage elements (when not given, take every SE)

    :returns:
      
    """
  
#    super( FailedTransfersBySourceSplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:
      
      if SEs is None:
        meta = { 'columns' : 'StorageElementName' }
        SEs = self.APIs[ 'ResourceStatusClient' ].getStorageElement( meta = meta)
        if not SEs[ 'OK' ]:
          return SEs 
        SEs = [ se[0] for se in SEs[ 'Value' ] ]
    
      if sources is None:
        meta = { 'columns' : 'SiteName' }
        sources = self.APIs[ 'ResourceStatusClient' ].getSite( meta = meta )      
        if not sources[ 'OK' ]:
          return sources 
        sources = [ si[0] for si in sources[ 'Value' ] ]

      if not sources + SEs:
        return S_ERROR( 'Sources + SEs is empty' )

      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow()-timedelta( hours = self.args[ 0 ] )
      toD   = datetime.utcnow()

      ft_source = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'FailedTransfers', 
                                                   fromD, toD, 
                                                   { 'OperationType':'putAndRegister', 
                                                     'Source': sources + SEs, 
                                                     'Destination': sources + SEs,
                                                     'FinalStatus':[ 'Failed' ] 
                                                    }, 
                                                    'Source' )
      if not ft_source[ 'OK' ]:
        return ft_source
      
      ft_source = ft_source[ 'Value' ]
      listOfSources = ft_source[ 'data' ].keys()   
      plotGran      = ft_source[ 'granularity' ]
      singlePlots   = {}
    
      for source in listOfSources:
        if source in sources:
          plot                  = {}
          plot[ 'data' ]        = { source: ft_source[ 'data' ][ source ] }
          plot[ 'granularity' ] = plotGran
          singlePlots[ source ] = plot
    
      res = S_OK( { 'DataOperation': singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class SuccessfullJobsBySiteSplittedCommand( Command ):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
  
  def doCommand(self, sites = None):
    """ 
    Returns successfull jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

#    super( SuccessfullJobsBySiteSplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if sites is None:
        sites = self.APIs[ 'ResourceStatusClient' ].getSite( meta = {'columns': 'SiteName' })
        if not sites['OK']:
          return sites 
        sites = [ si[0] for si in sites['Value'] ]
    
      if not sites:
        return S_ERROR( 'Sites is empty' )
    
      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow()-timedelta( hours = self.args[0] )
      toD   = datetime.utcnow()

      succ_jobs = self.APIs[ 'ReportsClient' ].getReport('Job', 'NumberOfJobs', fromD, toD, 
                                        {'FinalStatus':['Done'], 'Site':sites}, 'Site')
      if not succ_jobs['OK']:
        return succ_jobs 

      succ_jobs   = succ_jobs['Value']
      listOfSites = succ_jobs['data'].keys()   
      plotGran    = succ_jobs['granularity']
      singlePlots = {}
    
      for site in listOfSites:
        if site in sites:
          plot = {}
          plot['data'] = {site: succ_jobs['data'][site]}
          plot['granularity'] = plotGran
          singlePlots[site] = plot
    
      res = S_OK( { 'Job' : singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class FailedJobsBySiteSplittedCommand(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
  
  def doCommand(self, sites = None):
    """ 
    Returns failed jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """
    
#    super( FailedJobsBySiteSplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if sites is None:
        sites = self.APIs[ 'ResourceStatusClient' ].getSite( meta = {'columns' : 'SiteName'} )      
        if not sites['OK']:
          return sites 
        sites = [ si[0] for si in sites['Value'] ]

      if not sites:
        return S_ERROR( 'Sites is empty' )

      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow()-timedelta( hours = self.args[0] )
      toD   = datetime.utcnow()

      failed_jobs = self.APIs[ 'ReportsClient' ].getReport('Job', 'NumberOfJobs', fromD, toD, 
                                          {'FinalStatus':['Failed'], 'Site':sites}, 'Site')
      if not failed_jobs['OK']:
        return failed_jobs 
      
      failed_jobs = failed_jobs['Value']   
      listOfSites = failed_jobs['data'].keys()   
      plotGran    = failed_jobs['granularity']
      singlePlots = {}
    
      for site in listOfSites:
        if site in sites:
          plot = {}
          plot['data'] = {site: failed_jobs['data'][site]}
          plot['granularity'] = plotGran
          singlePlots[site] = plot
    
      res = S_OK( { 'Job': singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class SuccessfullPilotsBySiteSplittedCommand(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]  
  
  def doCommand(self, sites = None):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """
    
#    super( SuccessfullPilotsBySiteSplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if sites is None:
        sites = self.APIs[ 'ResourceStatusClient' ].getSite( meta = {'columns':'SiteName'} )      
        if not sites['OK']:
          return sites 
        sites = [ si[0] for si in sites['Value'] ]

      if not sites:
        return S_ERROR( 'Sites is empty' )

      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow()-timedelta( hours = self.args[0] )
      toD   = datetime.utcnow()

      succ_pilots = self.APIs[ 'ReportsClient' ].getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                        {'GridStatus':['Done'], 'Site':sites}, 'Site')
      if not succ_pilots['OK']:
        return succ_pilots 
      
      succ_pilots = succ_pilots['Value']
      listOfSites = succ_pilots['data'].keys()   
      plotGran    = succ_pilots['granularity']
      singlePlots = {}
    
      for site in listOfSites:
        if site in sites:
          plot = {}
          plot['data']        = { site: succ_pilots['data'][site] }
          plot['granularity'] = plotGran
          singlePlots[site]   = plot
    
      res = S_OK( { 'Pilot' : singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class FailedPilotsBySiteSplittedCommand(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
  
  def doCommand(self, sites = None):
    """ 
    Returns failed jobs using the DIRAC accounting system for every site 
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every site)

    :returns:
      
    """

#    super( FailedPilotsBySiteSplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:    

      if sites is None:
        sites = self.APIs[ 'ResourceStatusClient' ].getSite( meta = {'columns': 'SiteName'} )      
        if not sites['OK']:
          return sites 
        sites = [ si[0] for si in sites['Value'] ]
    
      if not sites:
        return S_ERROR( 'Sites is empty' )    
    
      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow()-timedelta(hours = self.args[0])
      toD   = datetime.utcnow()

      failed_pilots = self.APIs[ 'ReportsClient' ].getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                          {'GridStatus':['Aborted'], 'Site':sites}, 'Site')
      if not failed_pilots['OK']:
        return failed_pilots 
      
      failed_pilots = failed_pilots['Value']   
      listOfSites   = failed_pilots['data'].keys() 
      plotGran      = failed_pilots['granularity']    
      singlePlots   = {}

      for site in listOfSites:
        if site in sites:
          plot = {}
          plot['data']        = { site: failed_pilots['data'][site] }
          plot['granularity'] = plotGran
          singlePlots[site]   = plot
    
      res = S_OK( { 'Pilot': singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class SuccessfullPilotsByCESplittedCommand(Command):

  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]  
  
  def doCommand(self, CEs = None):
    """ 
    Returns successfull pilots using the DIRAC accounting system for every CE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """
    
#    super( SuccessfullPilotsByCESplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if CEs is None:
        meta = {'columns':'ResourceName'}
        CEs = self.APIs[ 'ResourceStatusClient' ].getResource( resourceType = [ 'CE','CREAMCE' ], meta = meta )
        if not CEs['OK']:
          return CEs 
        CEs = [ ce[0] for ce in CEs['Value'] ]

      if not CEs:
        return S_ERROR( 'CEs is empty' )

      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow() - timedelta(hours = self.args[0])
      toD   = datetime.utcnow()

      succ_pilots = self.APIs[ 'ReportsClient' ].getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                          {'GridStatus':['Done'], 'GridCE':CEs}, 'GridCE')
      if not succ_pilots['OK']:
        return succ_pilots 
      
      succ_pilots = succ_pilots['Value']
      listOfCEs   = succ_pilots['data'].keys()    
      plotGran    = succ_pilots['granularity']
      singlePlots = {}
    
      for CE in listOfCEs:
        if CE in CEs:
          plot = {}
          plot['data'] = {CE: succ_pilots['data'][CE]}
          plot['granularity'] = plotGran
          singlePlots[CE] = plot
    
      res = S_OK( { 'Pilot': singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class FailedPilotsByCESplittedCommand(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ] 
  
  def doCommand(self, CEs = None):
    """ 
    Returns failed pilots using the DIRAC accounting system for every CE 
    for the last self.args[0] hours 
        
    :params:
      :attr:`CEs`: list of CEs (when not given, take every CE)

    :returns:
      
    """

#    super( FailedPilotsByCESplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if CEs is None:
        CEs = self.APIs[ 'ResourceStatusClient' ].getResource( resourceType = [ 'CE','CREAMCE' ], meta = {'columns': 'ResourceName'})     
        if not CEs['OK']:
          return CEs 
        CEs = [ ce[0] for ce in CEs['Value'] ]
    
      if not CEs:
        return S_ERROR( 'CEs is empty' )    
    
      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow()-timedelta(hours = self.args[0])
      toD   = datetime.utcnow()

      failed_pilots = self.APIs[ 'ReportsClient' ].getReport('Pilot', 'NumberOfPilots', fromD, toD, 
                                            {'GridStatus':['Aborted'], 'GridCE':CEs}, 'GridCE')
      if not failed_pilots['OK']:
        return failed_pilots

      failed_pilots = failed_pilots['Value']
      listOfCEs     = failed_pilots['data'].keys()
      plotGran      = failed_pilots['granularity']
      singlePlots   = {}

      for CE in listOfCEs:
        if CE in CEs:
          plot = {}
          plot['data']        = { CE : failed_pilots['data'][CE] } 
          plot['granularity'] = plotGran
          singlePlots[CE]     = plot
    
      res = S_OK( { 'Pilot': singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class RunningJobsBySiteSplittedCommand(Command):
  
  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient', 'ReportGenerator' ]
  
  def doCommand(self, sites = None):
    """ 
    Returns running and runned jobs, querying the WMSHistory  
    for the last self.args[0] hours 
        
    :params:
      :attr:`sites`: list of sites (when not given, take every sites)

    :returns:
      
    """

#    super( RunningJobsBySiteSplitted_Command, self ).doCommand()
    self.APIs = initAPIs( self.__APIs__, self.APIs )

    try:

      if sites is None:
        sites = self.APIs[ 'ResourceStatusClient' ].getSite( meta = {'columns': 'SiteName'} )
        if not sites['OK']:
          return sites 
        sites = [ si[0] for si in sites['Value'] ]
    
      if not sites:
        return S_ERROR( 'Sites is empty' )
    
      self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]

      fromD = datetime.utcnow()-timedelta(hours = self.args[0])
      toD   = datetime.utcnow()

      run_jobs = self.APIs[ 'ReportsClient' ].getReport('WMSHistory', 'NumberOfJobs', fromD, toD, 
                                       {}, 'Site')
      if not run_jobs['OK']:
        return run_jobs 

      run_jobs    = run_jobs['Value']
      listOfSites = run_jobs['data'].keys()
      plotGran    = run_jobs['granularity']
      singlePlots = {}
    
      for site in listOfSites:
        if site in sites:
          plot = {}
          plot['data'] = {site: run_jobs['data'][site]}
          plot['granularity'] = plotGran
          singlePlots[site] = plot
    
      res = S_OK( { 'WMSHistory' : singlePlots } )

    except Exception, e:
      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
      gLogger.exception( _msg )
      return S_ERROR( _msg )

    return res 

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF