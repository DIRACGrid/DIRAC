''' DowntimeCommand module
'''

import urllib2

from datetime import datetime, timedelta

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.Core.LCG.GOCDBClient                                 import GOCDBClient
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping                import getGOCSiteName, getGOCFTSName
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers
from DIRAC.ConfigurationSystem.Client.Helpers.Resources         import getStorageElementOptions, getFTS3Servers
from operator                                                   import itemgetter

__RCSID__ = '$Id:  $'


class DowntimeCommand( Command ):
  '''
    Downtime "master" Command.
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
    '''
      Stores the results of doNew method on the database.
    '''

    for dt in result:
      resQuery = self.rmClient.addOrModifyDowntimeCache( 
                               downtimeID = dt[ 'DowntimeID' ],
                               element = dt[ 'Element' ],
                               name = dt[ 'Name' ],
                               startDate = dt[ 'StartDate' ],
                               endDate = dt[ 'EndDate' ],
                               severity = dt[ 'Severity' ],
                               description = dt[ 'Description' ],
                               link = dt[ 'Link' ],
                               gocdbServiceType = dt[ 'GOCDBServiceType' ] )
    return resQuery
  
  
  def _cleanCommand( self ):
    '''
      Clear Cache from expired DT.
    '''
    
    #reading all the cache entries
    result = self.rmClient.selectDowntimeCache()

    if not result[ 'OK' ]:
      return result

    uniformResult = [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ]
    
    currentDate = datetime.utcnow()
    
    for dt in uniformResult:
      if dt[ 'EndDate' ] < currentDate:
        resQuery = self.rmClient.deleteDowntimeCache ( 
                               downtimeID = dt[ 'DowntimeID' ]
                               )
    return resQuery


  def _prepareCommand( self ):
    '''
      DowntimeCommand requires four arguments:
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

    gocdbServiceType = None

    # Transform DIRAC site names into GOCDB topics
    if element == 'Site':

      gocSite = getGOCSiteName( elementName )
      if not gocSite[ 'OK' ]:
        return gocSite
      elementName = gocSite[ 'Value' ]

    # The DIRAC se names mean nothing on the grid, but their hosts do mean.
    elif elementType == 'StorageElement':
      # We need to distinguish if it's tape or disk
      if getStorageElementOptions( elementName )['Value']['TapeSE']:
        gocdbServiceType = "srm.nearline"
      elif getStorageElementOptions( elementName )['Value']['DiskSE']:
        gocdbServiceType = "srm"

      seHost = CSHelpers.getSEHost( elementName )
      if not seHost:
        return S_ERROR( 'No seHost for %s' % elementName )
      elementName = seHost
      
    elif elementType == 'FTS':
      gocdbServiceType = elementType
      try:
        #WARNING: this method presupposes that the server is an FTS3 type
        elementName  = getGOCFTSName(elementName)
      except:
        return S_ERROR( 'No FTS3 server specified in dirac.cfg (see Resources/FTSEndpoints)' )

    return S_OK( ( element, elementName, hours, gocdbServiceType ) )


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
      hours = None
      elementName = None
      gocdbServiceType = None
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      element, elementName, hours, gocdbServiceType = params[ 'Value' ]
      elementNames = [ elementName ]

    #WARNING: checking all the DT that are ongoing or starting in given <hours> from now
    startDate = None 
    if hours is not None:
      startDate = datetime.utcnow() + timedelta( hours = hours )

    try:
      results = self.gClient.getStatus( element, elementName, startDate )
    except urllib2.URLError:
      try:
        #Let's give it a second chance..
        results = self.gClient.getStatus( element, elementName, startDate )
      except urllib2.URLError, e:
        return S_ERROR( e )

    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]

    if results is None:
      return S_OK( None )
    
    
    #cleaning the Cache
    cleanRes = self._cleanCommand()
    if not cleanRes[ 'OK' ]:
      return cleanRes
    

    uniformResult = []

    # Humanize the results into a dictionary, not the most optimal, but readable
    for downtime, downDic in results.items():

      dt = {}
      
      if 'HOSTNAME' in downDic.keys():
        dt[ 'Name' ] = downDic[ 'HOSTNAME' ]
      elif 'SITENAME' in downDic.keys():
        dt[ 'Name' ] = downDic[ 'SITENAME' ]
      else:
        return S_ERROR( "SITENAME or HOSTNAME are missing" )
      
      if not dt[ 'Name' ] in elementNames:
        #it is not a site/resource we are interested to monitor
        continue      
      
      if gocdbServiceType and 'SERVICE_TYPE' in downDic.keys():
        gocdbST = gocdbServiceType.lower()
        csST = downDic[ 'SERVICE_TYPE' ].lower()
        if gocdbST != csST:
          return S_ERROR( "SERVICE_TYPE mismatch between GOCDB (%s) and CS (%s) for %s" % (gocdbST, csST, dt[ 'Name' ]) )
        else:
          dt[ 'GOCDBServiceType' ] = downDic[ 'SERVICE_TYPE' ]
      else:
        #WARNING: do we want None as default value?
        dt[ 'GOCDBServiceType' ] = None

      dt[ 'DowntimeID' ] = downtime
      dt[ 'Element' ] = element
      dt[ 'StartDate' ] = downDic[ 'FORMATED_START_DATE' ]
      dt[ 'EndDate' ] = downDic[ 'FORMATED_END_DATE' ]
      dt[ 'Severity' ] = downDic[ 'SEVERITY' ]
      dt[ 'Description' ] = downDic[ 'DESCRIPTION' ].replace( '\'', '' )
      dt[ 'Link' ] = downDic[ 'GOCDB_PORTAL_URL' ]

      uniformResult.append( dt )

    storeRes = self._storeCommand( uniformResult )
    if not storeRes[ 'OK' ]:
      return storeRes

    return S_OK()


  def doCache( self ):
    '''
      Method that reads the cache table and tries to read from it. It will
      return a list with one dictionary describing the DT if there are results.
    '''

    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    element, elementName, hours, gocdbServiceType = params[ 'Value' ]

    result = self.rmClient.selectDowntimeCache( element = element, name = elementName,
                                                gocdbServiceType = gocdbServiceType )

    if not result[ 'OK' ]:
      return result

    uniformResult = [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ]
    #sorting for convenient manipulation
    uniformResult.sort(key=itemgetter('Name','Severity','StartDate'))

    # We return only one downtime, if its ongoing at targetDate
    currentDate = datetime.utcnow()
    if hours is not None:
      futureDate = currentDate + timedelta( hours = hours )
      
    result = []
    #dtOverlapping is a buffer to assure only one dt is returned 
    #when there are overlapping outage/warning dt for same resource/site
    #fon top of the buffer we put the most recent outages 
    #while at the bottom the most recent warnings,
    #assumption: uniformResult list is already ordered by resource/site name, severity, startdate
    dtOverlapping = [] 
    countdown = len(uniformResult)
    
    for dt in uniformResult:
      countdown = countdown - 1
      # If hours defined, we want ongoing dt and dt starting before futureDate
      if hours is not None:
        if (dt[ 'StartDate' ] > currentDate) and (dt[ 'StartDate' ] < futureDate):
          result.append( dt )
      else:
        # if overlapping DTs for the same resource/site, then get just the most recent Outage
        if ( dt[ 'StartDate' ] < currentDate ) and ( dt[ 'EndDate' ] > currentDate ):
          if len(dtOverlapping) == 0:
            dtOverlapping.append(dt)
            #unless last iteration
            if countdown == 0:
              result.append( dt )
          else:
            dtTop = dtOverlapping[0]
            dtBottom = dtOverlapping[-1]
            if dtTop['Name'] != dt['Name']:
              if dtTop['Severity'] == 'OUTAGE':
                result.append( dtTop )
              else:
                # there are just warning dts
                result.append( dtBottom )
              #resetting the overlapping buffer
              dtOverlapping = [dt]
            else:
              #if outage we put it on top of the overlapping buffer
              #i.e. the most recent outage is top the most
              if dt['Severity'] == 'OUTAGE':
                dtOverlapping = [dt] + dtOverlapping
                #if last iteration, then dt must be the most recent outage
                if countdown == 0:
                  result.append( dt )
              #if warning we put it at the bottom of the overlapping buffer
              #i.e. the most recent warning is bottom the most
              elif  dt['Severity'] == 'WARNING':
                dtOverlapping.append(dt)
                #if last iteration, then look for overlapping outages
                if countdown == 0:
                  dtTop = dtOverlapping[0]
                  dtBottom = dtOverlapping[-1]
                  if dtTop['Severity'] == 'OUTAGE':
                    result.append( dtTop )
                  else:
                    result.append( dtBottom )


    return S_OK( result )


  def doMaster( self ):
    ''' Master method, which looks little bit spaghetti code, sorry !
        - It gets all sites and transforms them into gocSites.
        - It gets all the storage elements and transforms them into their hosts
        - It gets the the CEs (FTS and file catalogs will come).
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
    
    ftsServer = getFTS3Servers()
    if ftsServer[ 'OK' ]:
      resources.extend( ftsServer[ 'Value' ] )
      
    #TODO: file catalogs need also to use their hosts
   
    #fc = CSHelpers.getFileCatalogs()
    #if fc[ 'OK' ]:
    #  resources = resources + fc[ 'Value' ]

    ce = CSHelpers.getComputingElements()
    if ce[ 'OK' ]:
      resources.extend( ce[ 'Value' ] )
      
    print resources
    return  

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