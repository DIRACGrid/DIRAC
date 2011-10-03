"""
ResourceStatusBooster class comprises methods that are horrible or not popular
enough to be added to a generic API, but still quite convenient. 

Note that all interaction is done though the Client with its generic API !!
"""

from DIRAC import S_OK
from DIRAC.ResourceStatusSystem.Utilities.Validator import ResourceStatusValidator

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException


from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import getDIRACSiteName

from DIRAC.ResourceStatusSystem import ValidRes

from DIRAC.ResourceStatusSystem.Utilities.Decorators import CheckExecution

from datetime import datetime, timedelta

class ResourceStatusBooster:
  
  def __init__( self, rsClient ):
    self.rsClient = rsClient
    self.rsVal      = ResourceStatusValidator( ) 

################################################################################    

  @CheckExecution      
  def getGeneralName( self, from_element, name, to_element ):

    self.rsVal.validateRes( from_element )
    self.rsVal.validateRes( to_element )

    if from_element == 'Service':
      kwargs = { 'columns' : [ 'SiteName' ] }
      resQuery = self.rsClient.getServices( serviceName = name, **kwargs )  

    elif from_element == 'Resource':
      kwargs = { 'columns' : [ 'ServiceType' ] }
      resQuery = self.rsClient.getResources( resourceName = name, **kwargs )    
      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]

      if serviceType == 'Computing':
        kwargs = { 'columns' : [ 'SiteName' ] }  
        resQuery = self.rsClient.getResources( resourceName = name, **kwargs )
      else:
        kwargs = { 'columns' : [ 'GridSiteName' ] }    
        gridSiteNames = self.rsClient.getResources( resourceName = name, **kwargs )
        kwargs = { 'columns' : [ 'SiteName' ] }  
        resQuery = self.rsClient.getSites( gridSiteName = list( gridSiteNames[ 'Value' ] ), **kwargs )
        
    elif from_element == 'StorageElement':

      if to_element == 'Resource':
        kwargs = { 'columns' : [ 'ResourceName' ] }   
        resQuery = self.rsClient.getStorageElements( storageElementName = name, **kwargs )
      else:
        kwargs = { 'columns' : [ 'GridSiteName' ] }  
        gridSiteNames = self.rsClient.getStorageElements( storageElementName = name, **kwargs )
        kwargs = { 'columns' : [ 'SiteName' ] }
        resQuery = self.rsClient.getSites( gridSiteName = list( gridSiteNames[ 'Value' ] ), **kwargs )

        if to_element == 'Service':
          serviceType = 'Storage'

    else:
      raise ValueError

    if not resQuery[ 'Value' ]:
      return resQuery

    newNames = [ x[0] for x in resQuery[ 'Value' ] ]

    if to_element == 'Service':
      return S_OK( [ serviceType + '@' + x for x in newNames ] )
    else:
      return S_OK( newNames )
    
################################################################################    
    
  @CheckExecution  
  def getGridSiteName( self, granularity, name ):

    self.rsVal.validateRes( granularity )

    rDict = {
             '%sName' % granularity : name
             }

    kwargs = { 
              'columns' : [ 'GridSiteName' ] 
             }
    
    kwargs.update( rDict )
    

    getter = getattr( self.rsClient, 'get%ss' % granularity )
    return getter( **kwargs )

################################################################################    
        
  @CheckExecution      
  def getTokens( self, granularity, name, tokenExpiration, statusType, **kwargs ):

    self.rsVal.validateRes( granularity )  

    rDict = {}
    if name is not None:
      rDict[ '%sName' % granularity ] = name
      
    if statusType is not None:
      self.rsVal.validateElementStatusTypes( granularity, statusType )
      rDict[ 'StatusType' ] = statusType

    kw = {}
    kw[ 'columns' ] = kwargs.pop( 'columns', None )
    if tokenExpiration is not None:
      kw[ 'minor' ]   = { 'TokenExpiration' : tokenExpiration }
     
    getter = getattr( self.rsClient, 'get%sStatus' % granularity )
    
    kw.update( rDict )
    
    return getter( **kw ) 

################################################################################    
    
  @CheckExecution  
  def setToken( self, granularity, name, statusType, reason, tokenOwner, tokenExpiration ):

    self.rsVal.validateRes( granularity )
    self.rsVal.validateElementStatusTypes( granularity, statusType )
    
    updatter = getattr( self.rsClient, 'update%sStatus' % granularity )
    
    rDict = { 
             'statusType'          : statusType,
             'reason'              : reason,
             'tokenOwner'          : tokenOwner,
             'tokenExpiration'     : tokenExpiration
             }
    
    return updatter( name, **rDict )
    

################################################################################    
    
  @CheckExecution  
  def setReason( self, granularity, name, statusType, reason ):
        
    self.rsVal.validateRes( granularity )
    
    updatter = getattr( self.rsClient, 'update%sStatus' % granularity )
    
    rDict = { 
             'statusType'          : statusType,
             'reason'              : reason,
             }
    
    return updatter( name, **rDict )

################################################################################    
    
  @CheckExecution  
  def setDateEnd( self, granularity, name, statusType, dateEffective ):
    
    self.rsVal.validateRes( granularity )

    updatter = getattr( self.rsClient, 'update%sStatus' % granularity )
    
    rDict = { 
             'statusType'          : statusType,
             'dateEffective'       : dateEffective,
             }
    
    return updatter( name, **rDict )
    
################################################################################    
     
  @CheckExecution   
  def whatIs( self, name ):
    """
    Find which is the granularity of name.
    """

    for g in ValidRes:
      
      getter = getattr( self.rsClient, 'get%ss' % g )

      rDict  = { '%sName' % g : name }
      resQuery = getter( **rDict )
      
      if not resQuery[ 'Value' ]:
        continue
      else:
        return S_OK( g )

    return S_OK( 'Unknown' )  

################################################################################    
     
  @CheckExecution   
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    """
    Get Sites, Services, Resources, StorageElements to be checked using Present-x views.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`checkFrequecy': dictonary. Frequency of active sites/resources checking in minutes.

      :attr:`maxN`: integer - maximum number of lines in output
    """

    self.rsVal.validateRes( granularity )

    toCheck = {}

    now = datetime.utcnow().replace( microsecond = 0 )

    for freqName, freq in checkFrequency.items():
      toCheck[ freqName ] = ( now - timedelta( minutes = freq ) ).isoformat(' ')

    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = 'LastCheckTime'

    kwargs[ 'or' ] = []
        
    for k,v in toCheck.items():
          
      siteType, status = k.replace( '_CHECK_FREQUENCY', '' ).split( '_' )
      status = status[0] + status[1:].lower()
        
      dict = { 'Status' : status, 'SiteType' : siteType }
      kw   = { 'minor' : { 'LastCheckTime' : v } }
                
      orDict = { 'dict': dict, 'kwargs' : kw }          
                
      kwargs[ 'or' ].append( orDict )          
                   
    getter = getattr( self.rsClient, 'get%ssPresent' % granularity )
    return getter( **kwargs )      

################################################################################    
  
  @CheckExecution
  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, maxItems ):
    """
    Get present sites status list, for the web.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsList`
    and :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB.getMonitoredsHistory`

    Example of parameters:

    :params:
      :attr:`selectDict`: { 'SiteName':['XX', ...] , 'ExpandSiteHistory': ['XX', ...],
      'Status': ['XX', ...]}
      and equivalents for the other monitoreds

      :attr:`sortList`

      :attr:`startItem`

      :attr:`maxItems`

    :return: {
      :attr:`ParameterNames`: ['SiteName', 'Tier', 'GridType', 'Country',
      'Status', 'DateEffective', 'FormerStatus', 'Reason', 'StatusInTheMask'],

      :attr:'Records': [[], [], ...],

      :attr:'TotalRecords': X,

      :attr:'Extras': {}

      }
    """

    self.rsVal.validateRes( granularity )

    if granularity == 'Site':
      paramNames = [ 'SiteName', 'Tier', 'GridType', 'Country',
                     'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'SiteName', 'SiteType', 'StatusType','Status', 'DateEffective',
                     'FormerStatus', 'Reason' ]
    elif granularity == 'Service':
      paramNames = [ 'ServiceName', 'ServiceType', 'Site', 'Country', 'StatusType','Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'ServiceName', 'ServiceType', 'SiteName', 'StatusType','Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity == 'Resource':
      paramNames = [ 'ResourceName', 'ServiceType', 'SiteName', 'ResourceType',
                     'Country', 'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'ResourceName', 'ServiceType', 'SiteName', 'GridSiteName', 'ResourceType',
                     'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity == 'StorageElement':
      paramNames = [ 'StorageElementName', 'ResourceName', 'SiteName',
                     'Country', 'StatusType','Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'StorageElementName', 'ResourceName', 'GridSiteName', 'StatusType','Status',
                     'DateEffective', 'FormerStatus', 'Reason' ]

    records                = []

    rDict = { 'SiteName'                    : None,
              'ServiceName'                 : None,
              'ResourceName'                : None,
              'StorageElementName'          : None,
              'StatusType'                  : None,
              'Status'                      : None,
              'SiteType'                    : None,
              'ServiceType'                 : None,
              'ResourceType'                : None,
#              'Countries'                   : None,
              'ExpandSiteHistory'           : None,
              'ExpandServiceHistory'        : None,
              'ExpandResourceHistory'       : None,
              'ExpandStorageElementHistory' : None }


    for k in rDict.keys():
      if selectDict.has_key( k ):
        rDict[ k ] = selectDict[ k ]
        if not isinstance( rDict, list ):
          rDict[ k ] = [ rDict[ k ] ]

    if selectDict.has_key( 'Expanded%sHistory' % granularity ):
      paramsList = [ '%sName', 'StatusType', 'Status', 'Reason', 'DateEffective' ]
      elements   = rDict[ 'Expanded%sHistory' % granularity ]
      hgetter    = getattr( self.rsClient, 'get%ssHhistory' )
      kwargs     = { '%sName' % granularity : elements, 'columns' : paramsList }  
      elementsH  = hgetter( **kwargs )
      #elementsH  = self.getMonitoredsHistory( granularity, paramsList = paramsList,
      #                                        name = elements )

      for elementH in elementsH[ 'Value' ]:
        record = []
        record.append( elementH[ 0 ] )  #%sName % granularity
        record.append( None )           #Tier
        record.append( None )           #GridType
        record.append( None )           #Country
        record.append( elementH[ 1 ] )  #StatusType 
        record.append( elementH[ 2 ] )  #Status
        record.append( elementH[ 4 ].isoformat(' ') ) #DateEffective
        record.append( None )           #FormerStatus
        record.append( elementH[ 3 ] )  #Reason
        records.append( record )        

    else:
      kwargs = { 'columns' : paramsList }  
      if granularity == 'Site':
          
        sitesList = self.rsClient.getSitesPresent( siteName = rDict['SiteName'], 
                                          status   = rDict['Status'],
                                          siteType   = rDict['SiteType'],
                                          **kwargs )  
        #sitesList = self.getMonitoredsList(granularity,
        #                                   paramsList = paramsList,
        #                                   siteName   = rDict['SiteName'], #sites_select,
        #                                   status     = rDict['Status'],   #status_select,
        #                                   siteType   = rDict['SiteType'])#, #siteType_select,
        #                                   #countries  = rDict['Countries'])#countries_select)
        for site in sitesList[ 'Value' ]:
          record   = []
          gridType = ( site[ 0 ] ).split( '.' ).pop(0)
          country  = ( site[ 0 ] ).split( '.' ).pop()

          record.append( site[ 0 ] ) #SiteName
          record.append( site[ 1 ] ) #Tier
          record.append( gridType ) #GridType
          record.append( country ) #Country
          record.append( site[ 2 ] ) #StatusType
          record.append( site[ 3 ] ) #Status
          record.append( site[ 4 ].isoformat(' ') ) #DateEffective
          record.append( site[ 5 ] ) #FormerStatus
          record.append( site[ 6 ] ) #Reason
          records.append( record )

      elif granularity == 'Service':
        
        servicesList = self.rsClient.getServicesPresent( serviceName = rDict['ServiceName'],
                                                siteName    = rDict['SiteName'],
                                                status      = rDict['Status'],
                                                siteType    = rDict['SiteType'],
                                                serviceType = rDict['ServiceType'],
                                                **kwargs )         
        
        #servicesList = self.getMonitoredsList( granularity,
        #                                       paramsList  = paramsList,
        #                                       serviceName = rDict['ServiceName'], #services_select,
        #                                       siteName    = rDict['SiteName'], #sites_select,
        #                                       status      = rDict['Status'], #status_select,
        #                                       siteType    = rDict['SiteType'], #siteType_select,
        #                                       serviceType = rDict['ServiceType'])#, #serviceType_select,
        #                                     #  countries   = rDict['Countries']) #countries_select )
        for service in servicesList[ 'Value' ]:
          record  = []
          country = ( service[ 0 ] ).split( '.' ).pop()

          record.append( service[ 0 ] ) #ServiceName
          record.append( service[ 1 ] ) #ServiceType
          record.append( service[ 2 ] ) #Site
          record.append( country ) #Country
          record.append( service[ 3 ] ) #StatusType
          record.append( service[ 4 ] ) #Status
          record.append( service[ 5 ].isoformat(' ') ) #DateEffective
          record.append( service[ 6 ] ) #FormerStatus
          record.append( service[ 7 ] ) #Reason
          records.append( record )

      elif granularity == 'Resource':
        if rDict[ 'SiteName' ] == None:
          kw = { 'columns' : [ 'SiteName' ] }
          sites_select = self.rsClient.getSitesPresent( **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                       paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ] 
          
        kw = { 'columns' : [ 'GridSiteName' ] }
        gridSites_select = self.rsClient.getSitesPresent( siteName = rDict[ 'SiteName'], **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        resourcesList = self.rsClient.getResourcesPresent( resourceName = rDict['ResourceName'],
                                                  status       = rDict['Status'],
                                                  siteType     = rDict['SiteType'],
                                                  resourceType = rDict['ResourceType'],
                                                  gridSiteName = gridSites_select,
                                                  **kwargs )

        #resourcesList = self.getMonitoredsList( granularity,
        #                                        paramsList   = paramsList,
        #                                        resourceName = rDict['ResourceName'],#resources_select,
        #                                        status       = rDict['Status'],#status_select,
        #                                        siteType     = rDict['SiteType'],#siteType_select,
        #                                        resourceType = rDict['ResourceType'],#resourceType_select,
        #                                        #countries    = rDict['Countries'],#countries_select,
        #                                        gridSiteName = gridSites_select )

        for resource in resourcesList[ 'Value' ]:
          DIRACsite = resource[ 2 ]

          if DIRACsite == 'NULL':
            GridSiteName = resource[ 3 ]  #self.getGridSiteName(granularity, resource[0])
            DIRACsites = getDIRACSiteName( GridSiteName )
            if not DIRACsites[ 'OK' ]:
              raise RSSException, 'Error executing getDIRACSiteName'
            DIRACsites = DIRACsites[ 'Value' ]
            DIRACsite_comp = ''
            for DIRACsite in DIRACsites:
              if DIRACsite not in rDict[ 'SiteName' ]:#sites_select:
                continue
              DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp

            record  = []
            country = ( resource[ 0 ] ).split( '.' ).pop()

            record.append( resource[ 0 ] ) #ResourceName
            record.append( resource[ 1 ] ) #ServiceType
            record.append( DIRACsite_comp ) #SiteName
            record.append( resource[ 4 ] ) #ResourceType
            record.append( country ) #Country
            record.append( resource[ 5 ] ) #StatusType
            record.append( resource[ 6 ] ) #Status
            record.append( resource[ 7 ].isoformat(' ') ) #DateEffective
            record.append( resource[ 8 ] ) #FormerStatus
            record.append( resource[ 9 ] ) #Reason
            records.append( record )

          else:
            if DIRACsite not in rDict[ 'SiteName' ]: #sites_select:
              continue
            record  = []
            country = ( resource[ 0 ] ).split( '.' ).pop()

            record.append( resource[ 0 ] ) #ResourceName
            record.append( resource[ 1 ] ) #ServiceType
            record.append( DIRACsite ) #SiteName
            record.append( resource[ 4 ] ) #ResourceType
            record.append( country ) #Country
            record.append( resource[ 5 ] ) #StatusType
            record.append( resource[ 6 ] ) #Status
            record.append( resource[ 7 ].isoformat(' ') ) #DateEffective
            record.append( resource[ 8 ] ) #FormerStatus
            record.append( resource[ 9 ] ) #Reason
            records.append( record )


      elif granularity == 'StorageElement':
        if rDict[ 'SiteName' ] == []:#sites_select == []:
          kw = { 'columns' : [ 'SiteName' ] }
          sites_select = self.rsClient.getSitesPresent( **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                      paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ]

        kw = { 'columns' : [ 'GridSiteName' ] }
        gridSites_select = self.rsClient.getSitesPresent( siteName = rDict[ 'SiteName' ], **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        storageElementsList = self.rsClient.getStorageElementsPresent( storageElementName = rDict[ 'StorageElementName' ],
                                                              status             = rDict[ 'Status' ],
                                                              gridSiteName       = gridSites_select,
                                                              **kwargs
                                                              )
        #storageElementsList = self.getMonitoredsList( granularity,
        #                                              paramsList         = paramsList,
        #                                              storageElementName = rDict[ 'StorageElementName' ],#storageElements_select,
        #                                              status             = rDict[ 'Status' ],#status_select,
        #                                         #     countries          = rDict[ 'Countries' ],#countries_select,
        #                                              gridSiteName       = gridSites_select )

        for storageElement in storageElementsList[ 'Value' ]:
          DIRACsites = getDIRACSiteName( storageElement[ 2 ] )
          if not DIRACsites[ 'OK' ]:
            raise RSSException, 'Error executing getDIRACSiteName'
          DIRACsites = DIRACsites[ 'Value' ]
          DIRACsite_comp = ''
          for DIRACsite in DIRACsites:
            if DIRACsite not in rDict[ 'SiteName' ]:
              continue
            DIRACsite_comp = DIRACsite + ' ' + DIRACsite_comp
          record  = []
          country = ( storageElement[ 1 ] ).split( '.' ).pop()

          record.append( storageElement[ 0 ] ) #StorageElementName
          record.append( storageElement[ 1 ] ) #ResourceName
          record.append( DIRACsite_comp ) #SiteName
          record.append( country ) #Country
          record.append( storageElement[ 3 ] ) #StatusType
          record.append( storageElement[ 4 ] ) #Status
          record.append( storageElement[ 5 ].isoformat(' ') ) #DateEffective
          record.append( storageElement[ 6 ] ) #FormerStatus
          record.append( storageElement[ 7 ] ) #Reason
          records.append( record )

    finalDict = {}
    finalDict[ 'TotalRecords' ]   = len( records )
    finalDict[ 'ParameterNames' ] = paramNames

    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      finalDict[ 'Records' ] = records[ startItem:startItem+maxItems ]
    else:
      finalDict[ 'Records' ] = records

    finalDict[ 'Extras' ] = None

    return S_OK( finalDict )    

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF        