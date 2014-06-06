# $HeadURL:  $
''' DowntimeCommand module

'''

import urllib2

from datetime import datetime, timedelta

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
from DIRAC.ConfigurationSystem.Client.Helpers.Resources         import getGOCSiteName, Resources
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

__RCSID__ = '$Id:  $'

class DowntimeCommand( Command ):
  '''
    Downtime "master" Command.    
  '''

  def __init__( self, args = None, clients = None ):
    
    super( DowntimeCommand, self ).__init__( args, clients )

    self.resources = Resources()

    if 'GOCDBClient' in self.apis:
      self.gClient = self.apis[ 'GOCDBClient' ]
    else:
      self.gClient = GOCDBClient() 

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()
      
  def _storeCommand( self, result ):
    '''
      Stores the results of doNew method on the database.
    '''

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
    '''
      DowntimeCommand requires three arguments:
      - name : <str>
      - element : Site / Resource
      - elementType: <str>  
      
      If the elements are Site(s), we need to get their GOCDB names. They may
      not have, so we ignore them if they do not have.
    '''
    
    if 'name' not in self.args:
      return S_ERROR( '"name" not found in self.args' )
    elementName = self.args[ 'name' ]      
    
    if 'element' not in self.args:
      return S_ERROR( '"element" not found in self.args' )
    element = self.args[ 'element' ]
    
    if 'elementType' not in self.args:
      return S_ERROR( '"elementType" not found in self.args' )
    elementType = self.args[ 'elementType' ]
    
    if not element in [ 'Site', 'Resource' ]:
      return S_ERROR( 'element is not Site nor Resource' )   

    hours = None
    if 'hours' in self.args:
      hours = self.args[ 'hours' ]
    
    # Transform DIRAC site names into GOCDB topics
    if element == 'Site':

      gocSite = getGOCSiteName( elementName )
      if not gocSite[ 'OK' ]:
        return gocSite
      elementName = gocSite[ 'Value' ]
          
    # The DIRAC se names mean nothing on the grid, but their hosts do mean.
    elif elementType == 'StorageElement':
      
      result = CSHelpers.getSEProtocolOption( elementName, 'Host' )
      if not result['OK']:
        return S_ERROR( 'No seHost for %s' % elementName )
      elementName = result['Value']
             
    return S_OK( ( element, elementName, hours ) )

  def doNew( self, masterParams = None ):
    '''
      Gets the parameters to run, either from the master method or from its
      own arguments.
      
      For every elementName, unless it is given a list, in which case it contacts 
      the gocdb client. The server is not very stable, so in case of failure tries
      a second time.
      
      If there are downtimes, are recorded and then returned.
    '''
    
    if masterParams is not None:
      element, elementNames = masterParams
      hours       = None
      elementName = None
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      element, elementName, hours = params[ 'Value' ]  
      elementNames = [ elementName ]     

    startDate = datetime.utcnow() - timedelta( days = 14 )
          
    try:
      results = self.gClient.getStatus( element, elementName, startDate, 120 )
    except urllib2.URLError:
      try:
        #Let's give it a second chance..
        results = self.gClient.getStatus( element, elementName, startDate, 120 )
      except urllib2.URLError, e:
        return S_ERROR( e )
                  
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]

    if results is None:
      return S_OK( None )

    uniformResult = []
      
    # Humanize the results into a dictionary, not the most optimal, but readable
    for downtime, downDic in results.items():

      dt                  = {}
      if element == 'Resource':
        dt[ 'Name' ]        = downDic[ 'HOSTNAME' ]
      else:
        dt[ 'Name' ] = downDic[ 'SITENAME' ]
      
      if not dt[ 'Name' ] in elementNames:
        continue
      
      dt[ 'DowntimeID' ]  = downtime
      dt[ 'Element' ]     = element
      dt[ 'StartDate' ]   = downDic[ 'FORMATED_START_DATE' ]
      dt[ 'EndDate' ]     = downDic[ 'FORMATED_END_DATE' ]
      dt[ 'Severity' ]    = downDic[ 'SEVERITY' ]
      dt[ 'Description' ] = downDic[ 'DESCRIPTION' ].replace( '\'', '' )
      dt[ 'Link' ]        = downDic[ 'GOCDB_PORTAL_URL' ]
     
      uniformResult.append( dt )  
      
    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes
    
    # We return only one downtime, if its ongoind at dtDate
    startDate = datetime.utcnow()
    endDate   = startDate     
    if hours:
      startDate = startDate + timedelta( hours = hours )

    result = None           
    for dt in uniformResult:
      
      if ( dt[ 'StartDate' ] < str( startDate ) ) and ( dt[ 'EndDate' ] > str( endDate ) ):
        result = dt
        #We want to take the latest one ( they are sorted by insertion time )
        #break
           
    return S_OK( result )            

  def doCache( self ):
    '''
      Method that reads the cache table and tries to read from it. It will 
      return a list of dictionaries if there are results.
    '''
    
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    element, elementName, hours = params[ 'Value' ]
    
    result = self.rmClient.selectDowntimeCache( element = element, name = elementName )  
    if not result[ 'OK' ]:
      return result
    
    uniformResult = [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ]

    # We return only one downtime, if its ongoind at dtDate
    dtDate = datetime.utcnow()
    result = None
       
    if not hours:
      # If not hours defined, we want the downtimes running now, which means,
      # the ones that already started and will finish later. In case many
      # overlapping downtimes have been declared, the first one in severity and 
      # then time order will be selected. 
  
      dtOutages = []
      dtWarnings = []
      for dt in uniformResult:
        if ( dt[ 'StartDate' ] < dtDate ) and ( dt[ 'EndDate' ] > dtDate ):
          if dt[ 'Severity' ] == 'Outage':
            dtOutages.append( dt )
          else:
            dtWarnings.append( dt )

      if len( dtOutages ) > 0:
        result = dtOutages[0]
      elif len( dtWarnings ) > 0:
        result = dtWarnings[0]



    else:
      # If hours are defined, we want the downtimes starting in the next <hours>
      dtDateFuture = dtDate + timedelta( hours = hours )       
      for dt in uniformResult:
        if ( dt[ 'StartDate' ] > dtDate ) and ( dt[ 'StartDate' ] < dtDateFuture ):
          result = dt
          #We want to take the latest one ( they are sorted by insertion time )
          #break
           
    return S_OK( result )       

  def doMaster( self ):
    '''
      Master method, which looks little bit spaguetti code, sorry !
      - It gets all sites and transforms them into gocSites.
      - It gets all the storage elements and transforms them into their hosts
      - It gets the fts, the ces and file catalogs.
    '''
        
    gocSites = CSHelpers.getGOCSites()
    if not gocSites[ 'OK' ]:
      return gocSites
    gocSites = gocSites[ 'Value' ]
  
    sesHosts = CSHelpers.getStorageElementsHosts()
    if not sesHosts[ 'OK' ]:
      return sesHosts      
    sesHosts = sesHosts[ 'Value' ]  
    
    resources = sesHosts      
              
    #
    #
    #FIXME: file catalogs need also to use their hosts
    # something similar applies to FTS Channels
    #
    #fts = CSHelpers.getFTS()
    #if fts[ 'OK' ]:
    #  resources = resources + fts[ 'Value' ]
    #fc = CSHelpers.getFileCatalogs()
    #if fc[ 'OK' ]:
    #  resources = resources + fc[ 'Value' ]
    
    ce = self.resources.getEligibleResources( 'Computing' )
    if ce[ 'OK' ]:
      resources = resources + ce[ 'Value' ]
    
    gLogger.verbose( 'Processing Sites: %s' % ', '.join( gocSites ) )
    
    siteRes = self.doNew( ( 'Site', gocSites ) )
    if not siteRes[ 'OK' ]:
      self.metrics[ 'failed' ].append( siteRes[ 'Message' ] )

    gLogger.verbose( 'Processing Resources: %s' % ', '.join( resources ) )

    resourceRes = self.doNew( ( 'Resource', resources ) ) 
    if not resourceRes[ 'OK' ]:
      self.metrics[ 'failed' ].append( resourceRes[ 'Message' ] )
    
    return S_OK( self.metrics )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
