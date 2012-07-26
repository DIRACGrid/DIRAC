# $HeadURL:  $
''' DowntimeCommand module

'''

from DIRAC                                       import S_OK
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getGOCSiteName, getDIRACSiteName
from DIRAC.Core.LCG.GOCDBClient                  import GOCDBClient
from DIRAC.ResourceStatusSystem.Command.Command  import Command
from DIRAC.ResourceStatusSystem.Utilities        import CSHelpers

__RCSID__ = '$Id:  $'

class DowntimeSitesCommand( Command ):

  #FIXME: write propper docstrings

  def __init__( self, args = None, clients = None ):
    
    super( DowntimeSitesCommand, self ).__init__( args, clients )

    if 'GOCDBClient' in self.apis:
      self.gClient = self.apis[ 'GOCDBClient' ]
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
      if not sites[ 'OK' ]:
        return sites
      sites = sites[ 'Value' ]
      
    gocSites = []
    for site in sites:
      
      gocSite = getGOCSiteName( site )      
      if not gocSite[ 'OK' ]:
        #FIXME: not all sites are in GOC, only LCG sites. We have to filter them
        # somehow.
        continue
        #return gocSite
      
      gocSites.append( gocSite[ 'Value' ] )   

    results = self.gClient.getStatus( 'Site', gocSites, None, 120 )
    if not results[ 'OK' ]:
      return results     
    results = results[ 'Value' ]
    
    if results == None:
      return S_OK( results )

    downtimes = []

    for downtime, downDic in results.items():
        
      dt                  = {}
      dt[ 'ID' ]          = downtime
      dt[ 'StartDate' ]   = downDic[ 'FORMATED_START_DATE' ]
      dt[ 'EndDate' ]     = downDic[ 'FORMATED_END_DATE' ]
      dt[ 'Severity' ]    = downDic[ 'SEVERITY' ]
      dt[ 'Description' ] = downDic[ 'DESCRIPTION' ].replace( '\'', '' )
      dt[ 'Link' ]        = downDic[ 'GOCDB_PORTAL_URL' ]
        
      diracNames = getDIRACSiteName( downDic[ 'SITENAME' ] )
      if not diracNames[ 'OK' ]:
        return diracNames
          
      for diracName in diracNames[ 'Value' ]:
        dt[ 'Name' ] = diracName
        downtimes.append( dt )        

    return S_OK( downtimes )        

################################################################################
################################################################################

class DowntimeResourcesCommand( Command ):

  #FIXME: write propper docstrings

  def __init__( self, args = None, clients = None ):
    
    super( DowntimeResourcesCommand, self ).__init__( args, clients )
    
    if 'GOCDBClient' in self.apis:
      self.gClient = self.apis[ 'GOCDBClient' ]
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

    resources = None
    if 'resources' in self.args:
      resources = self.args[ 'resources' ] 
    
    if resources is None:

      #FIXME: we do not get them from RSS DB anymore, from CS now.
#      resources = self.rsClient.getResource( meta = { 'columns' : 'ResourceName' } )

      resources = CSHelpers.getResources()      
      if not resources[ 'OK' ]:
        return resources
      resources = resources[ 'Value' ]    

    results = self.gClient.getStatus( 'Resource', resources, None, 120 )
    
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]

    if results == None:
      return S_OK( results )

    downtimes = []

    for downtime, downDic in results.items():

      dt                  = {}
      dt[ 'ID' ]          = downtime
      dt[ 'StartDate' ]   = downDic[ 'FORMATED_START_DATE' ]
      dt[ 'EndDate' ]     = downDic[ 'FORMATED_END_DATE' ]
      dt[ 'Severity' ]    = downDic[ 'SEVERITY' ]
      dt[ 'Description' ] = downDic[ 'DESCRIPTION' ].replace( '\'', '' )
      dt[ 'Link' ]        = downDic[ 'GOCDB_PORTAL_URL' ]
      dt[ 'Name' ]        = downDic[ 'HOSTNAME' ]
        
      downtimes.append( dt )

    return S_OK( downtimes )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF