# $HeadURL:  $
''' DowntimeCommand module

'''

from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping                import getGOCSiteName, getDIRACSiteName
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'

class DowntimeCommand( Command ):
  '''
    As the API provided by GOCDB is incomplete, this command will look little
    bit terrible
  '''

  def __init__( self, args = None, clients = None ):
    
    super( DowntimeCommand, self ).__init__( args, clients )

    if 'GOCDBClient' in self.apis:
      self.gClient = self.apis[ 'GOCDBClient' ]
    else:
      self.gClient = GOCDBClient() 

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()
      
  def _storeCommand( self, result ):

    for dt in result:
      
      resQuery = self.rmClient.addOrModifyDowntimeCache( dt[ 'DowntimeID' ], 
                                                         dt[ 'Element' ], 
                                                         dt[ 'Name' ], 
                                                         dt[ 'StartDate' ], 
                                                         dt[ 'EndDate' ], 
                                                         dt[ 'Severity' ], 
                                                         dt[ 'Description' ], 
                                                         dt[ 'Link' ] )  
      if not resQuery[ 'OK' ]:
        return resQuery
    return S_OK()
  
  def _prepareCommand( self ):
    
    if 'name' not in self.args:
      return S_ERROR( '"name" not found in self.args' )
    elementName = self.args[ 'name' ]      
    
    if not isinstance( elementName, list ):
      elementName = [ elementName ]
    
    if 'element' not in self.args:
      return S_ERROR( '"element" not found in self.args' )
    element = self.args[ 'element' ]
    
    if not element in [ 'Site', 'Resource' ]:
      return S_ERROR( 'element is not Site nor Resource' )   
    
    if element == 'Site':
      
      gocSites = []
        
      for siteName in elementName:
        gocSite = getGOCSiteName( siteName )      
        if not gocSite[ 'OK' ]:
          #FIXME: not all sites are in GOC, only LCG sites. 
          #We have to filter them somehow.
          continue
          #return gocSite
        gocSites.append( gocSite[ 'Value' ] ) 
       
      elementName = gocSite
       
    return S_OK( ( element, elementName ) )

  def doNew( self, masterParams = None ):
    
    if masterParams is not None:
      element, elementNames = masterParams
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      element, elementNames = params[ 'Value' ]       
    
    uniformResult = []
    
    for elementName in elementNames:    
      
      results = self.gClient.getStatus( element, elementName, None, 120 )        
      if not results[ 'OK' ]:
        return results
      results = results[ 'Value' ]

      if results is None:
        continue

      for downtime, downDic in results.items():

        dt                  = {}
        dt[ 'DowntimeID' ]  = downtime
        dt[ 'Element' ]     = element
        dt[ 'StartDate' ]   = downDic[ 'FORMATED_START_DATE' ]
        dt[ 'EndDate' ]     = downDic[ 'FORMATED_END_DATE' ]
        dt[ 'Severity' ]    = downDic[ 'SEVERITY' ]
        dt[ 'Description' ] = downDic[ 'DESCRIPTION' ].replace( '\'', '' )
        dt[ 'Link' ]        = downDic[ 'GOCDB_PORTAL_URL' ]
        if element == 'Resource':
          dt[ 'Name' ]        = downDic[ 'HOSTNAME' ]
        else:
          dt[ 'Name' ] = downDic[ 'SITENAME' ]
      
        uniformResult.append( dt )  
          
    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes
           
    return S_OK( uniformResult )            

  def doCache( self ):

    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    element, elementNames = params[ 'Value' ]
    
    result = self.rmClient.selectDowntimeCache( element, elementNames )  
    if result[ 'OK' ]:
      result = S_OK( dict( zip( result[ 'Columns' ], result[ 'Value' ] ) ) )
           
    return result          

  def doMaster( self ):
    
    sites = CSHelpers.getSites()
    if not sites[ 'OK' ]:
      return sites
    sites = sites[ 'Value' ]

    gocSites = []
        
    for siteName in sites:
      gocSite = getGOCSiteName( siteName )      
      if not gocSite[ 'OK' ]:
        continue
      gocSites.append( gocSite[ 'Value' ] ) 
    sites = gocSite
    
    resources = CSHelpers.getResources()
    if not resources[ 'OK' ]:
      return resources
    resources = resources[ 'Value' ]
    
    siteRes = self.doNew( ( 'Site', sites ) )
    if not siteRes[ 'OK' ]:
      self.metrics[ 'failed' ].append( siteRes[ 'Message' ] )

    resourceRes = self.doNew( ( 'Resources', resources ) ) 
    if not resourceRes[ 'OK' ]:
      self.metrics[ 'failed' ].append( resourceRes[ 'Message' ] )
    
    return S_OK( self.metrics )

################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################

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
        return self.returnERROR( sites )
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
      return self.returnERROR( results )     
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
        return self.returnERROR( diracNames )
          
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
        return self.returnERROR( resources )
      resources = resources[ 'Value' ]    

    results = self.gClient.getStatus( 'Resource', resources, None, 120 )
    
    if not results[ 'OK' ]:
      return self.returnERROR( results )
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