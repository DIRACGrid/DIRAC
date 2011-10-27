from DIRAC.ResourceStatusSystem.API.private.ResourceStatusBaseAPI import ResourceStatusBaseAPI
from DIRAC.ResourceStatusSystem                      import ValidStatus, ValidStatusTypes, ValidRes

from datetime import datetime, timedelta

from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping     import getDIRACSiteName
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

from DIRAC import S_OK, S_ERROR

class ResourceStatusExtendedBaseAPI( ResourceStatusBaseAPI ):
    
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    args = ( siteName, siteType, gridSiteName )
    return self.__addOrModifyElement( 'Site', *args )

  def addOrModifyService( self, serviceName, serviceType, siteName ):
    args = ( serviceName, serviceType, siteName )
    return self.__addOrModifyElement( 'Service', *args )

  def addOrModifyResource( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName ):
    args = ( resourceName, resourceType, serviceType, siteName, gridSiteName )
    return self.__addOrModifyElement( 'Resource', *args )

  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    args = (storageElementName, resourceName, gridSiteName)
    return self.__addOrModifyElement( 'StorageElement', *args )

  def addOrModifyGridSite( self, gridSiteName, gridTier ):

    args = ( gridSiteName, gridTier )
    kwargs = { 'onlyUniqueKeys' : True }
      
    sqlQuery = self.getGridSite( *args, **kwargs )
   
    if sqlQuery[ 'Value' ]:
      return self.updateGridSite( *args )      
    else:
      return self.insertGridSite( *args )   

  def modifyElementStatus( self, element, elementName, statusType, status = None, 
                           reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None,
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None ):
    args = ( elementName, statusType, status, reason, dateCreated, dateEffective, 
             dateEnd, lastCheckTime, tokenOwner, tokenExpiration )
    return self.__modifyElementStatus( element, *args )

  def removeElement( self, element, elementName ):
    return self.__removeElement( element, elementName )

  def getServiceStats( self, siteName, statusType = None ):

    presentDict = { 'siteName' : siteName }
    if statusType is not None:
#      self.__validateElementStatusTypes( 'Service', statusType )
      presentDict[ 'StatusType' ] = statusType
    
    kwargs   = { 'columns' : [ 'Status'], 'count' : True, 'group' : 'Status' }
    presentDict.update( kwargs )

    #sqlQuery = self.rsClient.getServicePresent( **presentDict )
    sqlQuery = self.__getElement( 'ServicePresent', **presentDict )
    return self.__getStats( sqlQuery )

  def getResourceStats( self, element, name, statusType = None ):

    # VALIDATION ??
    presentDict = { }

    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( 'Service', statusType )
      presentDict[ 'StatusType'] = statusType    

    rDict = { 'serviceType'  : None, 
              'siteName'     : None, 
              'gridSiteName' : None
            }

    if element == 'Site':
      rDict[ 'siteName' ] = name

    elif element == 'Service':

      serviceType, siteName = name.split( '@' )
      rDict[ 'serviceType' ] = serviceType
      
      if serviceType == 'Computing':
        rDict[ 'siteName' ] = siteName
        
      else:
        kwargs = { 'columns' : [ 'GridSiteName' ], 'siteName' : siteName }
        #gridSiteName = [ gs[0] for gs in self.rsClient.getSite( siteName = siteName, **kwargs )[ 'Value' ] ]
        gridSiteName = [ gs[0] for gs in self.__getElement( 'Site', **kwargs )[ 'Value' ] ]
        
        rDict[ 'gridSiteName' ] = gridSiteName
        
    else:
      message = '%s is non accepted element. Only Site or Service' % element
      return S_ERROR( message )

    #resourceNames = [ re[0] for re in self.rsClient.getResource( **rDict )[ 'Value' ] ]
    resourceNames = [ re[0] for re in self.__getElement( 'Resource', **rDict )[ 'Value' ] ]
    
    kwargs   = { 'columns' : [ 'Status'], 'count' : True, 'group' : 'Status' }
    presentDict[ 'resourceName' ] = resourceNames
    presentDict.update( kwargs )
    
    #sqlQuery = self.rsClient.getResourcePresent( **presentDict )
    sqlQuery = self.__getElement( 'ResourcePresent', **presentDict )
    return self.__getStats( sqlQuery )
 
  def getStorageElementStats( self, element, name, statusType = None ):

    # VALIDATION ??
    presentDict = {}

    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( 'StorageElement', statusType )
      presentDict[ 'StatusType'] = statusType

    rDict = { 'resourceName' : None,
              'gridSiteName' : None }
    
    if element == 'Site':

      kwargs = { 'columns' : [ 'GridSiteName' ], 'siteName' : name  }
      #gridSiteNames = [ gs[0] for gs in self.rsClient.getSite( siteName = name, **kwargs )[ 'Value' ] ]
      gridSiteNames = [ gs[0] for gs in self.__getElement( 'Site', **kwargs )[ 'Value' ] ]
      rDict[ 'gridSiteName' ] = gridSiteNames

    elif element == 'Resource':

      rDict[ 'resourceName' ] = name

    else:
      message = '%s is non accepted element. Only Site or Resource' % element
      return S_ERROR( message )

    #storageElementNames = [ se[0] for se in self.rsClient.getStorageElement( **rDict )[ 'Value' ] ]
    storageElementNames = [ se[0] for se in self.__getElement( 'StorageElement', **rDict )[ 'Value' ] ]

    kwargs   = { 'columns' : [ 'Status'], 'count' : True, 'group' : 'Status' }
    presentDict[ 'storageElementName' ] = storageElementNames
    presentDict.update( kwargs )
    
#    sqlQuery = self.rsClient.getStorageElementPresent( **presentDict )
    sqlQuery = self.__getElement( 'StorageElementPresent', **presentDict )
    return self.__getStats( sqlQuery )  
  
  def getGeneralName( self, from_element, name, to_element ):

#    self.rsVal.validateElement( from_element )
#    self.rsVal.validateElement( to_element )

    if from_element == 'Service':
      kwargs = { 'columns' : [ 'SiteName' ], 'serviceName' : name }
      resQuery = self.__getElement( 'Service', **kwargs )
      #resQuery = self.rsClient.getService( serviceName = name, **kwargs )  

    elif from_element == 'Resource':
      kwargs = { 'columns' : [ 'ServiceType' ], 'resourceName' : name }
      resQuery = self.__getElement( 'Resource', **kwargs )
      #resQuery = self.rsClient.getResource( resourceName = name, **kwargs )    
      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]

      if serviceType == 'Computing':
        kwargs = { 'columns' : [ 'SiteName' ], 'resourceName' : name }
        resQuery = self.__getElement( 'Resource', **kwargs )  
        #resQuery = self.rsClient.getResource( resourceName = name, **kwargs )
      else:
        kwargs = { 'columns' : [ 'GridSiteName' ], 'resourceName' : name }    
        #gridSiteNames = self.rsClient.getResource( resourceName = name, **kwargs )
        gridSiteNames = self.__getElement( 'Resource', **kwargs )
        kwargs = { 'columns' : [ 'SiteName' ], 'gridSiteName' : list( gridSiteNames[ 'Value' ] ) }  
        resQuery = self.__getElement( 'Site', **kwargs )
        #resQuery = self.rsClient.getSite( gridSiteName = list( gridSiteNames[ 'Value' ] ), **kwargs )
        
    elif from_element == 'StorageElement':

      if to_element == 'Resource':
        kwargs = { 'columns' : [ 'ResourceName' ], 'storageElementName' : name }   
        resQuery = self.__getElement( 'StorageElement', **kwargs )
        #resQuery = self.rsClient.getStorageElement( storageElementName = name, **kwargs )
      else:
        kwargs = { 'columns' : [ 'GridSiteName' ], 'storageElementName' : name }  
        #gridSiteNames = self.rsClient.getStorageElement( storageElementName = name, **kwargs )
        gridSiteNames = self.__getElement( 'StorageElement', **kwargs )
        kwargs = { 'columns' : [ 'SiteName' ], 'gridSiteName' : list( gridSiteNames[ 'Value' ] ) }
        resQuery = self.__getElement( 'Site', **kwargs )
        #resQuery = self.rsClient.getSite( gridSiteName = list( gridSiteNames[ 'Value' ] ), **kwargs )

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

  def getGridSiteName( self, granularity, name ):

#    self.rsVal.validateElement( granularity )

    rDict = {
             '%sName' % granularity : name
             }

    kwargs = { 
              'columns' : [ 'GridSiteName' ] 
             }
    
    kwargs.update( rDict )
    
    return self.__getElement( granularity, **kwargs )
 #   getter = getattr( self.rsClient, 'get%s' % granularity )
    #return getter( **kwargs )

  def getTokens( self, granularity, name, tokenExpiration, statusType, **kwargs ):

#    self.rsVal.validateElement( granularity )  

    rDict = {}
    if name is not None:
      rDict[ '%sName' % granularity ] = name
      
    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( granularity, statusType )
      rDict[ 'StatusType' ] = statusType

    kw = {}
    kw[ 'columns' ] = kwargs.pop( 'columns', None )
    if tokenExpiration is not None:
      kw[ 'minor' ]   = { 'TokenExpiration' : tokenExpiration }

    kw.update( rDict )
     
    return self.__getElement( '%sStatus' % granularity, **kw ) 
    #getter = getattr( self.rsClient, 'get%sStatus' % granularity )  
    #return getter( **kw ) 

  def setToken( self, granularity, name, statusType, reason, tokenOwner, tokenExpiration ):

#    self.rsVal.validateElement( granularity )
#    self.rsVal.validateElementStatusTypes( granularity, statusType )
    
    #updatter = getattr( self.rsClient, 'update%sStatus' % granularity )
    
    rDict = { 
             'statusType'          : statusType,
             'reason'              : reason,
             'tokenOwner'          : tokenOwner,
             'tokenExpiration'     : tokenExpiration
             }
    
    return self.__updateElement( 'ElementStatus', granularity, name, **rDict )
    #return updatter( name, **rDict )

  def setReason( self, granularity, name, statusType, reason ):
        
#    self.rsVal.validateElement( granularity )
        
    #modificator = getattr( self, 'modify%sStatus' % granularity )
    #elementName = granularity[0].lower() + granularity[1:]
    
    rDict = { 
             #'%sName' % elementName : name,        
             'elementName': name,
             'statusType' : statusType,
             'reason'     : reason,
             }
     
    return self.modifyElementStatus( granularity, **rDict ) 
    #return modificator( **rDict )

  def setDateEnd( self, granularity, name, statusType, dateEffective ):
    
    self.rsVal.validateElement( granularity )

    #modificator = getattr( self, 'modify%sStatus' % granularity )   
    #elementName = granularity[0].lower() + granularity[1:]
    
    rDict = { 
             #'%sName' % elementName : name,
             'elementName'   : name,
             'statusType'    : statusType,
             'dateEffective' : dateEffective,
             }
    
    return self.modifyElementStatus( granularity, **rDict )
    #return modificator( **rDict )
    #return updatter( name, **rDict )

  def whatIs( self, name ):
    """
    Find which is the granularity of name.
    """

    for g in ValidRes:
      
      #getter = getattr( self.rsClient, 'get%ss' % g )

      rDict  = { '%sName' % g : name }
      #resQuery = getter( **rDict )
      resQuery = self.__getElement( g, **rDict )
      
      if not resQuery[ 'Value' ]:
        continue
      else:
        return S_OK( g )

    return S_OK( 'Unknown' )  

  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    """
    Get Sites, Services, Resources, StorageElements to be checked using Present-x views.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`checkFrequecy': dictonary. Frequency of active sites/resources checking in minutes.

      :attr:`maxN`: integer - maximum number of lines in output
    """

#    self.rsVal.validateElement( granularity )

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
                   
    #getter = getattr( self.rsClient, 'get%sPresent' % granularity )
    #return getter( **kwargs )  
    return self.__getElement( '%sPresent' % granularity, **kwargs )

  def getMonitoredStatus( self, granularity, name ):
 
    #getter = getattr( self.rsClient, 'get%sPresent' % granularity )
    
    elementName = '%sName' % ( granularity[0].lower() + granularity[1:] ) 
    kwargs = { elementName : name, 'columns' : [ 'Status' ] }
    
    return self.__getElement( '%sPresent' % granularity, **kwargs )
    #return getter( **kwargs )

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

#    self.rsVal.validateElement( granularity )

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
      #hgetter    = getattr( self.rsClient, 'get%ssHhistory' )
      kwargs     = { '%sName' % granularity : elements, 'columns' : paramsList }  
      #elementsH  = hgetter( **kwargs )
      elementsH = self.__getElement( 'ElementHistory', granularity, **kwargs )
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
        
        kwargs[ 'siteName' ] = rDict['SiteName']
        kwargs[ 'status' ]   = rDict['Status']
        kwargs[ 'siteType' ] = rDict['SiteType']
        
        sitesList = self.__getElement( 'SitePresent', **kwargs )  
        #sitesList = self.rsClient.getSitePresent( siteName = rDict['SiteName'], 
        #                                  status   = rDict['Status'],
        #                                  siteType   = rDict['SiteType'],
        #                                  **kwargs )  
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
        
        kwargs[ 'serviceName' ] = rDict['ServiceName']
        kwargs[ 'siteName' ]    = rDict['SiteName']
        kwargs[ 'status' ]      = rDict['Status']
        kwargs[ 'siteType' ]    = rDict['SiteType']
        kwargs[ 'serviceType' ] = rDict['ServiceType']
        
        servicesList = self.__getElement( 'ServicePresent', **kwargs )
        #servicesList = self.rsClient.getServicesPresent( serviceName = rDict['ServiceName'],
        #                                        siteName    = rDict['SiteName'],
        #                                        status      = rDict['Status'],
        #                                        siteType    = rDict['SiteType'],
        #                                        serviceType = rDict['ServiceType'],
        #                                        **kwargs )         
        
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
          #sites_select = self.rsClient.getSitePresent( **kw )
          sites_select = self.__getElement( 'SitePresent', **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                       paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ] 
          
        kw = { 'columns' : [ 'GridSiteName' ], 'siteName' : rDict[ 'SiteName'] }
        
        #gridSites_select = self.rsClient.getSitePresent( siteName = rDict[ 'SiteName'], **kw )
        gridSites_select = self.__getElement( 'SitePresent', **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        kwargs[ 'resourceName' ] = rDict['ResourceName']
        kwargs[ 'status' ]       = rDict['Status']
        kwargs[ 'siteType' ]     = rDict['SiteType']
        kwargs[ 'resourceType' ] = rDict['ResourceType']
        kwargs[ 'gridSiteName' ] = gridSites_select

        resourcesList = self.__getElement( 'ResourcePresent', **kwargs )
        #resourcesList = self.rsClient.getResourcePresent( resourceName = rDict['ResourceName'],
        #                                          status       = rDict['Status'],
        #                                          siteType     = rDict['SiteType'],
        #                                          resourceType = rDict['ResourceType'],
        #                                          gridSiteName = gridSites_select,
        #                                          **kwargs )

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
          #sites_select = self.rsClient.getSitePresent( **kw )
          sites_select = self.__getElement( 'SitePresent', **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                      paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ]

        kw = { 'columns' : [ 'GridSiteName' ], 'siteName' : rDict[ 'SiteName' ] }
        #gridSites_select = self.rsClient.getSitePresent( siteName = rDict[ 'SiteName' ], **kw )
        gridSites_select = self.__getElement( 'SitePresent', **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        kwargs[ 'storageElementName' ] = rDict[ 'StorageElementName' ]
        kwargs[ 'status' ]             = rDict[ 'Status' ]
        kwargs[ 'gridSiteName']        = gridSites_select 

        storageElementsList = self.__getElement( 'StorageElementPresent', *kwargs )
        #storageElementsList = self.rsClient.getStorageElementPresent( storageElementName = rDict[ 'StorageElementName' ],
        #                                                      status             = rDict[ 'Status' ],
        #                                                      gridSiteName       = gridSites_select,
        #                                                      **kwargs
        #                                                      )
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

  '''
  ##############################################################################
  # Getter functions
  ##############################################################################
  '''

  def __insertElement( self, element, *args, **kwargs ):
    
    fname = 'insert%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  def __updateElement( self, element, *args, **kwargs ):
    
    fname = 'update%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  def __getElement( self, element, *args, **kwargs ):
    
    fname = 'get%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  def __deleteElement( self, element, *args, **kwargs ):
    
    fname = 'delete%s' % element
    f = getattr( self, fname )
    return f( *args, **kwargs )

  '''
  ##############################################################################
  # addOrModify PRIVATE FUNCTIONS
  ##############################################################################
  '''

  def __addOrModifyElement( self, element, *args ):
    
    kwargs = { 'onlyUniqueKeys' : True }    
    sqlQuery = self.__getElement( element, *args, **kwargs )  
       
    if sqlQuery[ 'Value' ]:      
      return self.__updateElement( element, *args )
    else: 
      sqlQuery = self.__insertElement( element, *args )
      if sqlQuery[ 'OK' ]:       
        return self.__setElementInitStatus( element, *args )
      else:
        return sqlQuery  

  def __setElementInitStatus( self, element, *args ):
    
    defaultStatus  = 'Banned'
    defaultReasons = [ 'Added to DB', 'Init' ]

    # This three lines make not much sense, but sometimes statusToSet is '',
    # and we need it as a list to work properly
    statusToSet = ValidStatusTypes[ element ][ 'StatusType' ]
    
    if not isinstance( statusToSet, list ):
      statusToSet = [ statusToSet ]
    
    for statusType in statusToSet:

      # Trick to populate ElementHistory table with one entry. This allows
      # us to use PresentElement views ( otherwise they do not work ).
      for defaultReason in defaultReasons:

        rList = [ args[0], statusType, defaultStatus, defaultReason ] 
        
        sqlQuery = self.__addOrModifyElementStatus( element, rList  )        
                
        if not sqlQuery[ 'OK' ]:
          return sqlQuery
        
    return S_OK()     

  def __addOrModifyElementStatus( self, element, rList ):

    # VALIDATION ?
   
    rList += self.__setStatusDefaults()

    #elementName = '%sName' % ( element[0].lower() + element[1:] )
    kwargs = { 'elementName' : rList[ 0 ], 'statusType' : rList[ 1 ], 'onlyUniqueKeys' : True }
    #sqlQuery = self.__getElement( '%sStatus' % element, **kwargs )

    sqlQuery = self.__getElement( 'ElementStatus', element, **kwargs )

    if not sqlQuery[ 'Value' ]:
      #return self.__insertElement( '%sStatus' % element, *tuple( rList ) )
      return self.__insertElement( 'ElementStatus', element, *tuple( rList ) )

    #updateSQLQuery = self.__updateElement( '%sStatus' % element, *tuple( rList ))
    updateSQLQuery = self.__updateElement( 'ElementStatus', element, *tuple( rList ))
    if not updateSQLQuery[ 'OK' ]:
      return updateSQLQuery 

    sqlQ      = list( sqlQuery[ 'Value' ][ 0 ] )[1:]
    # EHistory.DateEnd = EStatus.DateEffective
    # This is vital for the views !!!!
    sqlQ[ 6 ] = rList[ 5 ]   
     
    #return self.__insertElement( '%sHistory' % element , *tuple( sqlQ ) )   
    return self.__insertElement( 'ElementHistory', element , *tuple( sqlQ ) )

  def __setStatusDefaults( self ):#, rDict ):
     
    now    = datetime.utcnow().replace( microsecond = 0 )
    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )

    #dateCreated, dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration
    iList = [ now, now, never, now, 'RS_SVC', never ] 
    return iList

  '''
  ##############################################################################
  # Modify PRIVATE FUNCTIONS
  ##############################################################################
  '''
  
  def __modifyElementStatus( self, element, *args ):
      
    args = list(args)

    #elementName = '%sName' % ( element[0].lower() + element[1:] )
    kwargs = { 'elementName' : args[ 0 ], 'statusType' : args[ 1 ] }
    sqlQuery = self.__getElement( 'ElementStatus', element, **kwargs )

    if not sqlQuery[ 'OK' ]:
      return sqlQuery
    if not sqlQuery[ 'Value' ]:
      return S_ERROR( 'Impossible to modify, %s (%s) is not on the DB' % ( args[ 0 ],args[ 1 ] ) )

    #DateEffective
    if args[ 5 ] is None:
      args[ 5 ] = datetime.utcnow().replace( microsecond = 0 )

    #LastCheckTime
    if args[ 7 ] is None:
      args[ 7 ] = datetime.utcnow().replace( microsecond = 0 )
    
    updateSQLQuery = self.__updateElement( 'ElementStatus', element, *tuple( args ))
    if not updateSQLQuery[ 'OK' ]:
      return updateSQLQuery 
    
    sqlQ      = list( sqlQuery[ 'Value' ][ 0 ] )[1:]
    # EHistory.DateEnd = EStatus.DateEffective
    # This is vital for the views !!!!
    sqlQ[ 6 ] = args[ 5 ]   

    return self.__insertElement( 'ElementHistory', element , *tuple( sqlQ ) )  
  
  '''
  ##############################################################################
  # remove PRIVATE FUNCTIONS
  ##############################################################################
  '''
  
  def __removeElement( self, element, elementName ):
  
    tables = [ 'ScheduledStatus', 'Status', 'History' ]
    for table in tables:
      sqlQuery = self.__deleteElement( 'Element%s' % ( table ), element, elementName )
      if not sqlQuery[ 'OK' ]:
        return sqlQuery
    
    sqlQuery = self.__deleteElement( element, elementName )

    return sqlQuery   
  
  '''
  ##############################################################################
  # stats PRIVATE FUNCTIONS
  ##############################################################################
  '''          
     
  def __getStats( self, sqlQuery ):
    
    if not sqlQuery[ 'OK' ]:
      return sqlQuery 

    count = { 'Total' : 0 }
    for validStatus in ValidStatus:
      count[ validStatus ] = 0

    for x in sqlQuery[ 'Value' ]:
      count[ x[0] ] = int( x[1] )

    count['Total'] = sum( count.values() )
    return S_OK( count ) 