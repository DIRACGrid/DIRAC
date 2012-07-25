# $HeadURL:  $
''' ClientsCacheCommand module

  The ClientsCacheCommand class is a command module to know about collective 
  clients results (to be cached).
  
'''

from DIRAC                                                  import S_OK
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping            import getGOCSiteName, getDIRACSiteName
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Core.LCG.GOCDBClient                             import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.JobsClient           import JobsClient
from DIRAC.ResourceStatusSystem.Client.PilotsClient         import PilotsClient
from DIRAC.ResourceStatusSystem.Command.Command             import Command
from DIRAC.ResourceStatusSystem.Utilities                   import CSHelpers

__RCSID__ = '$Id:  $'

class JobsEffSimpleEveryOneCommand( Command ):

  #FIXME: write propper docstrings

  def __init__( self, args = None, clients = None ):
    
    super( JobsEffSimpleEveryOneCommand, self ).__init__( args, clients )
    
    if 'ResourceStatusClient' in self.APIs:
      self.rsClient = self.APIs[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient() 

    if 'JobsClient' in self.APIs:
      self.jClient = self.APIs[ 'JobsClient' ]
    else:
      self.jClient = JobsClient() 
    
    if 'WMSAdministrator' in self.APIs:
      self.wClient = self.APIs[ 'WMSAdministrator' ]
    else:
      self.wClient = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    
  def doCommand( self ):
    """ 
    Returns simple jobs efficiency for all the sites in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName': {'JE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'}, ...}
    """

    sites = None

    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 

    if sites is None:
      #FIXME: we do not get them from RSS DB anymore, from CS now.
      #sites = self.rsClient.selectSite( meta = { 'columns' : 'SiteName' } )
      sites = CSHelpers.getSites()
        
      if not sites['OK']:
        return sites
         
      sites = [ site[ 0 ] for site in sites[ 'Value' ] ]

    results = self.jClient.getJobsSimpleEff( sites, self.wClient )
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]
        
    if results is None:
      results = []

    resToReturn = {}

    for site in results:
      resToReturn[ site ] = { 'JE_S' : results[ site ] }

    return S_OK( resToReturn ) 

################################################################################
################################################################################

class PilotsEffSimpleEverySitesCommand( Command ):

  #FIXME: write propper docstrings

  def __init__( self, args = None, clients = None ):
    
    super( PilotsEffSimpleEverySitesCommand, self ).__init__( args, clients )
    
    if 'ResourceStatusClient' in self.APIs:
      self.rsClient = self.APIs[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient() 

    if 'PilotsClient' in self.APIs:
      self.pClient = self.APIs[ 'PilotsClient' ]
    else:
      self.pClient = PilotsClient() 
    
    if 'WMSAdministrator' in self.APIs:
      self.wClient = self.APIs[ 'WMSAdministrator' ]
    else:
      self.wClient = RPCClient( 'WorkloadManagement/WMSAdministrator' )

  def doCommand( self ):
    """ 
    Returns simple pilots efficiency for all the sites and resources in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName':  {'PE_S': 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
    """

    sites = None

    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 

    if sites is None:
      #FIXME: we do not get them from RSS DB anymore, from CS now.
      #sites = self.rsClient.selectSite( meta = { 'columns' : 'SiteName' } )
      sites = CSHelpers.getSites()      
      if not sites[ 'OK' ]:
        return sites
      
      sites = [ site[ 0 ] for site in sites[ 'Value' ] ]

    results = self.pClient.getPilotsSimpleEff( 'Site', sites, None, self.wClient )
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]
    
    if results is None:
      results = []

    resToReturn = {}

    for site in results:
      resToReturn[ site ] = { 'PE_S' : results[ site ] }

    return S_OK( resToReturn )

################################################################################
################################################################################
#
#class TransferQualityEverySEs_Command( Command ):
#
#  __APIs__ = [ 'ResourceStatusClient', 'ReportsClient' ]
#
#  def doCommand( self, SEs = None ):
#    """ 
#    Returns transfer quality using the DIRAC accounting system for every SE 
#        
#    :params:
#      :attr:`SEs`: list of storage elements (when not given, take every SE)
#    
#    :returns:
#      {'SiteName': {TQ : 'Good'|'Fair'|'Poor'|'Idle'|'Bad'} ...}
#    """
#
#    self.APIs = initAPIs( self.__APIs__, self.APIs )
#
#    if SEs is None:
#      SEs = self.APIs[ 'ResourceStatusClient' ].getStorageElement( meta = {'columns' : 'StorageElementName' })
#      if not SEs['OK']:
#      else:
#        SEs = SEs['Value']
#
#    self.APIs[ 'ReportsClient' ].rpcClient = self.APIs[ 'ReportGenerator' ]
#
#    fromD = datetime.datetime.utcnow() - datetime.timedelta( hours = 2 )
#    toD = datetime.datetime.utcnow()
#
#    try:
#      qualityAll = self.APIs[ 'ReportsClient' ].getReport( 'DataOperation', 'Quality', fromD, toD,
#                                         {'OperationType':'putAndRegister',
#                                          'Destination':SEs}, 'Channel' )
#      if not qualityAll['OK']:
#      else:
#        qualityAll = qualityAll['Value']['data']
#
#    except:
#      gLogger.exception( "Exception when calling TransferQualityEverySEs_Command" )
#      return {}
#
#    listOfDestSEs = []
#
#    for k in qualityAll.keys():
#      try:
#        key = k.split( ' -> ' )[1]
#        if key not in listOfDestSEs:
#          listOfDestSEs.append( key )
#      except:
#        continue
#
#    meanQuality = {}
#
#    for destSE in listOfDestSEs:
#      s = 0
#      n = 0
#      for k in qualityAll.keys():
#        try:
#          if k.split( ' -> ' )[1] == destSE:
#            n = n + len( qualityAll[k] )
#            s = s + sum( qualityAll[k].values() )
#        except:
#          continue
#      meanQuality[destSE] = s / n
#
#    resToReturn = {}
#
#    for se in meanQuality:
#      resToReturn[se] = {'TQ': meanQuality[se]}
#
#    return resToReturn
#
#
#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
################################################################################

class DTEverySitesCommand( Command ):

  #FIXME: write propper docstrings

  def __init__( self, args = None, clients = None ):
    
    super( DTEverySitesCommand, self ).__init__( args, clients )
    
    if 'ResourceStatusClient' in self.APIs:
      self.rsClient = self.APIs[ 'ResourceStatusClient' ]
    else:
      self.rsClient = ResourceStatusClient() 

    if 'GOCDBClient' in self.APIs:
      self.gClient = self.APIs[ 'GOCDBClient' ]
    else:
      self.gClient = GOCDBClient() 

  def doCommand( self ):
    """ 
    Returns downtimes information for all the sites in input.
        
    :params:
      :attr:`sites`: list of site names (when not given, take every site)
    
    :returns:
      {'SiteName': {'SEVERITY': 'OUTAGE'|'AT_RISK', 
                    'StartDate': 'aDate', ...} ... }
    """

    sites = None

    if 'sites' in self.args:
      sites = self.args[ 'sites' ] 
    
    if sites is None:
      #FIXME: we do not get them from RSS DB anymore, from CS now.
      #sites = self.rsClient.selectSite( meta = { 'columns' : 'SiteName' } )
      sites = CSHelpers.getSites()      
      if not sites['OK']:
        return sites
      
      sites = [ site[ 0 ] for site in sites[ 'Value' ] ]  
      
#    if sites is None:
#      GOC_sites = self.rsClient.getGridSite( meta = { 'columns' : 'GridSiteName' })
#      if not GOC_sites['OK']:
#        return GOC_sites
#      GOC_sites = [ gs[0] for gs in GOC_sites['Value'] ]
#    else:
#      GOC_sites = [ getGOCSiteName( x )[ 'Value' ] for x in sites ]

    gocSites = [ getGOCSiteName( x )[ 'Value' ] for x in sites ]

    resGOC = self.gClient.getStatus( 'Site', gocSites, None, 120 )
    if not resGOC[ 'OK' ]:
      return resGOC
      
    resGOC = resGOC[ 'Value' ]

    if resGOC == None:
      resGOC = []

    results = {}

    for dt_ID in resGOC:
        
#      try:
          
      dt                  = {}
      dt[ 'ID' ]          = dt_ID
      dt[ 'StartDate' ]   = resGOC[ dt_ID ][ 'FORMATED_START_DATE' ]
      dt[ 'EndDate' ]     = resGOC[ dt_ID ][ 'FORMATED_END_DATE' ]
      dt[ 'Severity' ]    = resGOC[ dt_ID ][ 'SEVERITY' ]
      dt[ 'Description' ] = resGOC[ dt_ID ][ 'DESCRIPTION' ].replace( '\'', '' )
      dt[ 'Link' ]        = resGOC[ dt_ID ][ 'GOCDB_PORTAL_URL' ]
        
      diracNames = getDIRACSiteName( resGOC[ dt_ID ][ 'SITENAME' ] )
          
      if not diracNames[ 'OK' ]:
        return diracNames
          
      for diracName in diracNames[ 'Value' ]:
        results[ '%s %s' % ( dt_ID.split()[0], diracName ) ] = dt

# FIXME: why does it fail ?            
#      except KeyError:
#        continue

    return S_OK( results )        

################################################################################
################################################################################

class DTEveryResourcesCommand( Command ):

  #FIXME: write propper docstrings

  def __init__( self, args = None, clients = None ):
    
    super( DTEveryResourcesCommand, self ).__init__( args, clients )
    
#    if 'ResourceStatusClient' in self.APIs:
#      self.rsClient = self.APIs[ 'ResourceStatusClient' ]
#    else:
#      self.rsClient = ResourceStatusClient() 

    if 'GOCDBClient' in self.APIs:
      self.gClient = self.APIs[ 'GOCDBClient' ]
    else:
      self.gClient = GOCDBClient() 

  def doCommand( self ):
    """ 
    Returns downtimes information for all the resources in input.
        
    :params:
      :attr:`sites`: list of resource names (when not given, take every resource)
    
    :returns:
      {'ResourceName': {'SEVERITY': 'OUTAGE'|'AT_RISK', 
                    'StartDate': 'aDate', ...} ... }
    """

#    self.APIs = initAPIs( self.__APIs__, self.APIs )

#    try:

    resources = None

    if 'resources' in self.args:
      resources = self.args[ 'resources' ] 
    
    if resources is None:

      #FIXME: we do not get them from RSS DB anymore, from CS now.
#      meta = { 'columns' : 'ResourceName' }
#      resources = self.rsClient.getResource( meta = meta )
#      if not resources['OK']:
#        return resources
#      resources = [ re[0] for re in resources['Value'] ]
      resources = CSHelpers.getResources()      
      if not resources[ 'OK' ]:
        return resources
      
      resources = [ resource[ 0 ] for resource in resources[ 'Value' ] ]  
      

    resGOC = self.gClient.getStatus( 'Resource', resources, None, 120 )
    
    if not resGOC['OK']:
      return resGOC
    resGOC = resGOC['Value']

    if resGOC == None:
      resGOC = []

    res = {}

    for dt_ID in resGOC:
      dt                   = {}
      dt[ 'ID' ]           = dt_ID
      dt[ 'StartDate' ]    = resGOC[ dt_ID ][ 'FORMATED_START_DATE' ]
      dt[ 'EndDate' ]      = resGOC[ dt_ID ][ 'FORMATED_END_DATE' ]
      dt[ 'Severity' ]     = resGOC[ dt_ID ][ 'SEVERITY' ]
      dt[ 'Description' ]  = resGOC[ dt_ID ][ 'DESCRIPTION' ].replace( '\'', '' )
      dt[ 'Link' ]         = resGOC[ dt_ID ][ 'GOCDB_PORTAL_URL' ]
      res[ dt_ID ] = dt

    return S_OK( res )

#    except Exception, e:
#      _msg = '%s (%s): %s' % ( self.__class__.__name__, self.args, e )
#      gLogger.exception( _msg )
#      return S_ERROR( _msg )

#  doCommand.__doc__ = Command.doCommand.__doc__ + doCommand.__doc__

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF