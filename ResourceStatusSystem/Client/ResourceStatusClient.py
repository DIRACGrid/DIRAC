################################################################################
# $HeadURL $
################################################################################
__RCSID__ = "$Id:  $"

from DIRAC import S_OK, S_ERROR

from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB 
       
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping     import getDIRACSiteName       
       
from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientFastDec     
from DIRAC.ResourceStatusSystem                             import ValidRes,\
  ValidStatus, ValidStatusTypes, ValidSiteType, ValidServiceType, \
  ValidResourceType       
       
from datetime import datetime, timedelta       
       
class ResourceStatusClient:
  """
  The :class:`ResourceStatusClient` class exposes the :mod:`DIRAC.ResourceStatus` 
  API. All functions you need are on this client.
  
  It has the 'direct-db-access' functions, the ones of the type:
   - insert
   - update
   - get
   - delete 
    
  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.  
  
  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely :class:`ResourceStatusDB` and :class:`ResourceStatusHancler` ).

  You can use this client on this way

   >>> from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
   >>> rsClient = ResourceStatusClient()
   
  All functions calling methods exposed on the database or on the booster are 
  making use of some syntactic sugar, in this case a decorator that simplifies
  the client considerably.    
  """

  def __init__( self , serviceIn = None ):
    '''
      The client tries to connect to :class:ResourceStatusDB by default. If it 
      fails, then tries to connect to the Service :class:ResourceStatusHandler.
    '''
 
    if serviceIn == None:
      try:
        self.gate = ResourceStatusDB()
      except Exception:
        self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
    else:
      self.gate = serviceIn
      
  '''
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''
  
  @ClientFastDec
  def insertSite( self, siteName, siteType, gridSiteName, meta = {} ):
    return locals()
  @ClientFastDec
  def updateSite( self, siteName, siteType, gridSiteName, meta = {} ):
    return locals()
  @ClientFastDec
  def getSite( self, siteName = None, siteType = None, gridSiteName = None, 
               meta = {} ):
    return locals()
  @ClientFastDec
  def deleteSite( self, siteName = None, siteType = None, gridSiteName = None, 
                  meta = {} ):
    return locals()      
  @ClientFastDec
  def getSitePresent( self, siteName = None, siteType = None, 
                      gridSiteName = None, gridTier = None, statusType = None, 
                      status = None, dateEffective = None, reason = None, 
                      lastCheckTime = None, tokenOwner = None, 
                      tokenExpiration = None, formerStatus = None, meta = {} ):
    return locals()

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''
  @ClientFastDec
  def insertService( self, serviceName, serviceType, siteName, meta = {} ):
    return locals()
  @ClientFastDec
  def updateService( self, serviceName, serviceType, siteName, meta = {} ):
    return locals()
  @ClientFastDec
  def getService( self, serviceName = None, serviceType = None, siteName = None, 
                  meta = {} ):
    return locals()
  @ClientFastDec
  def deleteService( self, serviceName = None, serviceType = None, 
                     siteName = None, meta = {} ):
    return locals()
  @ClientFastDec  
  def getServicePresent( self, serviceName = None, siteName = None, 
                         siteType = None, serviceType = None, statusType = None, 
                         status = None, dateEffective = None, reason = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, formerStatus = None, 
                         meta = {} ):
    return locals()

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''
  @ClientFastDec
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, meta = {} ):
    return locals()
  @ClientFastDec
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, meta = {} ):
    return locals()
  @ClientFastDec
  def getResource( self, resourceName = None, resourceType = None, 
                   serviceType = None, siteName = None, gridSiteName = None, 
                   meta = {} ):
    return locals()
  @ClientFastDec
  def deleteResource( self, resourceName = None, resourceType = None, 
                      serviceType = None, siteName = None, gridSiteName = None, 
                      meta = {} ):
    return locals()
  @ClientFastDec      
  def getResourcePresent( self, resourceName = None, siteName = None, 
                          serviceType = None, gridSiteName = None, 
                          siteType = None, resourceType = None, 
                          statusType = None, status = None, 
                          dateEffective = None, reason = None, 
                          lastCheckTime = None, tokenOwner = None, 
                          tokenExpiration = None, formerStatus = None, 
                          meta = {} ):
    return locals()

  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''
  @ClientFastDec
  def insertStorageElement( self, storageElementName, resourceName, 
                            gridSiteName, meta = {} ):
    return locals()
  @ClientFastDec
  def updateStorageElement( self, storageElementName, resourceName, 
                            gridSiteName, meta = {} ):
    return locals()
  @ClientFastDec       
  def getStorageElement( self, storageElementName = None, resourceName = None, 
                         gridSiteName = None, meta = {} ):
    return locals()
  @ClientFastDec       
  def deleteStorageElement( self, storageElementName = None, 
                            resourceName = None, gridSiteName = None, 
                            meta = {} ):
    return locals()    
  @ClientFastDec      
  def getStorageElementPresent( self, storageElementName = None, 
                                resourceName = None, gridSiteName = None, 
                                siteType = None, statusType = None, 
                                status = None, dateEffective = None, 
                                reason = None, lastCheckTime = None, 
                                tokenOwner = None, tokenExpiration = None, 
                                formerStatus = None, meta = {} ):
    return locals()

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''
  @ClientFastDec
  def insertGridSite( self, gridSiteName, gridTier, meta = {} ):
    return locals()
  @ClientFastDec
  def updateGridSite( self, gridSiteName, gridTier, meta = {} ):
    return locals()
  @ClientFastDec    
  def getGridSite( self, gridSiteName = None, gridTier = None, meta = {} ):
    return locals()
  @ClientFastDec    
  def deleteGridSite( self, gridSiteName = None, gridTier = None, meta = {} ):        
    return locals()

  '''
  ##############################################################################
  # ELEMENT STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientFastDec
  def insertElementStatus( self, element, elementName, statusType, status, 
                           reason, dateCreated, dateEffective, dateEnd, 
                           lastCheckTime, tokenOwner, tokenExpiration, 
                           meta = {} ): 
    return locals()
  @ClientFastDec
  def updateElementStatus( self, element, elementName, statusType, status, 
                           reason, dateCreated, dateEffective, dateEnd, 
                           lastCheckTime, tokenOwner, tokenExpiration, 
                           meta = {} ):
    return locals()
  @ClientFastDec
  def getElementStatus( self, element, elementName = None, statusType = None, 
                        status = None, reason = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, 
                        lastCheckTime = None, tokenOwner = None, 
                        tokenExpiration = None, meta = {} ):
    return locals()
  @ClientFastDec
  def deleteElementStatus( self, element, elementName = None, statusType = None, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, meta = {} ):
    return locals()

  '''
  ##############################################################################
  # ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''
  @ClientFastDec
  def insertElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, meta = {} ): 
    return locals()
  @ClientFastDec
  def updateElementScheduledStatus( self, element, elementName, statusType, 
                                    status, reason, dateCreated, dateEffective, 
                                    dateEnd, lastCheckTime, tokenOwner, 
                                    tokenExpiration, meta = {} ):
    return locals()
  @ClientFastDec
  def getElementScheduledStatus( self, element, elementName = None, 
                                 statusType = None, status = None, 
                                 reason = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, tokenOwner = None, 
                                 tokenExpiration = None, meta = {} ):
    return locals()
  @ClientFastDec
  def deleteElementScheduledStatus( self, element, elementName = None, 
                                    statusType = None, status = None, 
                                    reason = None, dateCreated = None,
                                    dateEffective = None, dateEnd = None, 
                                    lastCheckTime = None, tokenOwner = None, 
                                    tokenExpiration = None, meta = {} ):
    return locals()
      
  '''
  ##############################################################################
  # ELEMENT HISTORY FUNCTIONS
  ##############################################################################
  '''
  @ClientFastDec
  def insertElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            meta = {} ): 
    return locals()
  @ClientFastDec
  def updateElementHistory( self, element, elementName, statusType, status, 
                            reason, dateCreated, dateEffective, dateEnd, 
                            lastCheckTime, tokenOwner, tokenExpiration, 
                            meta = {} ):
    return locals()
  @ClientFastDec
  def getElementHistory( self, element, elementName = None, statusType = None, 
                         status = None, reason = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, 
                         lastCheckTime = None, tokenOwner = None, 
                         tokenExpiration = None, meta = {} ):
    return locals()
  @ClientFastDec
  def deleteElementHistory( self, element, elementName = None, 
                            statusType = None, status = None, reason = None, 
                            dateCreated = None, dateEffective = None, 
                            dateEnd = None, lastCheckTime = None, 
                            tokenOwner = None, tokenExpiration = None, 
                            meta = {} ):
    return locals() 

  '''
  ##############################################################################
  # CS VALID ELEMENTS
  ##############################################################################
  '''
  
  def getValidElements( self ):
    return S_OK( ValidRes )
  def getValidStatuses( self ):
    return S_OK( ValidStatus )
  def getValidStatusTypes( self ):  
    return S_OK( ValidStatusTypes )
  def getValidSiteTypes( self ):
    return S_OK( ValidSiteType )
  def getValidServiceTypes( self ):
    return S_OK( ValidServiceType ) 
  def getValidResourceTypes( self ):
    return S_OK( ValidResourceType )

  '''
  ##############################################################################
  # EXTENDED FUNCTIONS
  ##############################################################################
  '''

  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    return self.__addOrModifyElement( 'Site', locals() )

  def addOrModifyService( self, serviceName, serviceType, siteName ):
    return self.__addOrModifyElement( 'Service', locals() )

  def addOrModifyResource( self, resourceName, resourceType, serviceType, 
                           siteName, gridSiteName ):
    return self.__addOrModifyElement( 'Resource', locals() )

  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    return self.__addOrModifyElement( 'StorageElement', locals() )

  def addOrModifyGridSite( self, gridSiteName, gridTier ):

    args = ( gridSiteName, gridTier )
    kwargs = { 'gridSiteName' : gridSiteName, 'gridTier' : gridTier, 
               'meta' : { 'onlyUniqueKeys' : True } }
      
    sqlQuery = self.getGridSite( **kwargs )
   
    if sqlQuery[ 'Value' ]:
      return self.updateGridSite( *args )      
    else:
      return self.insertGridSite( *args )   

  def modifyElementStatus( self, element, elementName, statusType, 
                           status = None, reason = None, dateCreated = None, 
                           dateEffective = None, dateEnd = None,
                           lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None ):
    return self.__modifyElementStatus( locals() )

  def removeElement( self, element, elementName ):
    return self.__removeElement( element, elementName )

  def getServiceStats( self, siteName, statusType = None ):

    presentDict = { 'siteName' : siteName }
    if statusType is not None:
#      self.__validateElementStatusTypes( 'Service', statusType )
      presentDict[ 'statusType' ] = statusType
    
    kwargs   = { 'meta' : { 'columns' : [ 'Status'], 
                            'count' : True, 
                            'group' : 'Status' } }
    presentDict.update( kwargs )

    #sqlQuery = self.rsClient.getServicePresent( **presentDict )
    sqlQuery = self._getElement( 'ServicePresent', **presentDict )
    return self.__getStats( sqlQuery )

  def getResourceStats( self, element, name, statusType = None ):

    # VALIDATION ??
    presentDict = { }

    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( 'Service', statusType )
      presentDict[ 'statusType'] = statusType    

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
        kwargs = { 'meta' : {'columns' : [ 'GridSiteName' ] }, 'siteName' : siteName }
        #gridSiteName = [ gs[0] for gs in self.rsClient.getSite( siteName = siteName, **kwargs )[ 'Value' ] ]
        gridSiteName = [ gs[0] for gs in \
                         self._getElement( 'Site', **kwargs )[ 'Value' ] ]
        
        rDict[ 'gridSiteName' ] = gridSiteName
        
    else:
      message = '%s is non accepted element. Only Site or Service' % element
      return S_ERROR( message )

    #resourceNames = [ re[0] for re in self.rsClient.getResource( **rDict )[ 'Value' ] ]
    resourceNames = [ re[0] for re in \
                          self._getElement( 'Resource', **rDict )[ 'Value' ] ]
    
    kwargs   = { 'meta' : { 'columns' : [ 'Status'], 'count' : True, 'group' : 'Status' } }
    presentDict[ 'resourceName' ] = resourceNames
    presentDict.update( kwargs )
    
    #sqlQuery = self.rsClient.getResourcePresent( **presentDict )
    sqlQuery = self._getElement( 'ResourcePresent', **presentDict )
    return self.__getStats( sqlQuery )
 
  def getStorageElementStats( self, element, name, statusType = None ):

    # VALIDATION ??
    presentDict = {}

    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( 'StorageElement', statusType )
      presentDict[ 'statusType'] = statusType

    rDict = { 'resourceName' : None,
              'gridSiteName' : None }
    
    if element == 'Site':

      kwargs = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'siteName' : name  }
      #gridSiteNames = [ gs[0] for gs in self.rsClient.getSite( siteName = name, **kwargs )[ 'Value' ] ]
      gridSiteNames = [ gs[0] for gs in \
                             self._getElement( 'Site', **kwargs )[ 'Value' ] ]
      rDict[ 'gridSiteName' ] = gridSiteNames

    elif element == 'Resource':

      rDict[ 'resourceName' ] = name

    else:
      message = '%s is non accepted element. Only Site or Resource' % element
      return S_ERROR( message )

    #storageElementNames = [ se[0] for se in self.rsClient.getStorageElement( **rDict )[ 'Value' ] ]
    storageElementNames = [ se[0] for se in \
                    self._getElement( 'StorageElement', **rDict )[ 'Value' ] ]

    kwargs   = { 'meta' : { 'columns' : [ 'Status'], 'count' : True, 'group' : 'Status' } }
    presentDict[ 'storageElementName' ] = storageElementNames
    presentDict.update( kwargs )
    
#    sqlQuery = self.rsClient.getStorageElementPresent( **presentDict )
    sqlQuery = self._getElement( 'StorageElementPresent', **presentDict )
    return self.__getStats( sqlQuery )  
  
  def getGeneralName( self, from_element, name, to_element ):

#    self.rsVal.validateElement( from_element )
#    self.rsVal.validateElement( to_element )

    if from_element == 'Service':
      kwargs = { 'meta' : { 'columns' : [ 'SiteName' ] }, 'serviceName' : name }
      resQuery = self._getElement( 'Service', **kwargs )
      #resQuery = self.rsClient.getService( serviceName = name, **kwargs )  

    elif from_element == 'Resource':
      kwargs = { 'meta' : { 'columns' : [ 'ServiceType' ] }, 'resourceName' : name }
      resQuery = self._getElement( 'Resource', **kwargs )
      #resQuery = self.rsClient.getResource( resourceName = name, **kwargs )    
      serviceType = resQuery[ 'Value' ][ 0 ][ 0 ]

      if serviceType == 'Computing':
        kwargs = { 'meta' : { 'columns' : [ 'SiteName' ] }, 'resourceName' : name }
        resQuery = self._getElement( 'Resource', **kwargs )  
        #resQuery = self.rsClient.getResource( resourceName = name, **kwargs )
      else:
        kwargs = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'resourceName' : name }    
        #gridSiteNames = self.rsClient.getResource( resourceName = name, **kwargs )
        gridSiteNames = self._getElement( 'Resource', **kwargs )
        kwargs = { 
                   'meta' : { 'columns'      : [ 'SiteName' ] }, 
                   'gridSiteName' : list( gridSiteNames[ 'Value' ] ) 
                 }  
        resQuery = self._getElement( 'Site', **kwargs )
        #resQuery = self.rsClient.getSite( gridSiteName = list( gridSiteNames[ 'Value' ] ), **kwargs )
        
    elif from_element == 'StorageElement':

      if to_element == 'Resource':
        kwargs = { 'meta' : { 'columns' : [ 'ResourceName' ] }, 'storageElementName' : name }   
        resQuery = self._getElement( 'StorageElement', **kwargs )
        #resQuery = self.rsClient.getStorageElement( storageElementName = name, **kwargs )
      else:
        kwargs = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'storageElementName' : name }  
        #gridSiteNames = self.rsClient.getStorageElement( storageElementName = name, **kwargs )
        gridSiteNames = self._getElement( 'StorageElement', **kwargs )
        kwargs = { 
                   'meta' : { 'columns'      : [ 'SiteName' ] }, 
                   'gridSiteName' : list( gridSiteNames[ 'Value' ] ) 
                 }
        resQuery = self._getElement( 'Site', **kwargs )
        #resQuery = self.rsClient.getSite( gridSiteName = list( gridSiteNames[ 'Value' ] ), **kwargs )

        if to_element == 'Service':
          serviceType = 'Storage'

    else:
      return S_ERROR( 'Expected from_element either Service, Resource or StorageElement' )

    if not resQuery[ 'Value' ]:
      return resQuery

    newNames = [ x[0] for x in resQuery[ 'Value' ] ]

    if to_element == 'Service':
      return S_OK( [ serviceType + '@' + x for x in newNames ] )
    else:
      return S_OK( newNames )

  def getGridSiteName( self, granularity, name ):

#    self.rsVal.validateElement( granularity )

    elementName = '%sName' % ( granularity[0].lower() + granularity[1:] ) 

    rDict = {
              elementName : name
             }

    kwargs = { 
              'meta' : { 'columns' : [ 'GridSiteName' ] } 
             }
    
    kwargs.update( rDict )
    
    return self._getElement( granularity, **kwargs )
 #   getter = getattr( self.rsClient, 'get%s' % granularity )
    #return getter( **kwargs )

  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ):

#    self.rsVal.validateElement( granularity )  

    rDict = { 'element' : granularity }
    if name is not None:
      rDict[ 'elementName' ] = name
      
    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( granularity, statusType )
      rDict[ 'statusType' ] = statusType

    kw = { 'meta' : {}}
    kw[ 'meta' ][ 'columns' ] = kwargs.pop( 'columns', None )
    if tokenExpiration is not None:
      kw[ 'meta' ][ 'minor' ]   = { 'tokenExpiration' : tokenExpiration }

    kw.update( rDict )
    
     
    return self._getElement( 'ElementStatus', **kw ) 
    #getter = getattr( self.rsClient, 'get%sStatus' % granularity )  
    #return getter( **kw ) 

  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):

#    self.rsVal.validateElement( granularity )
#    self.rsVal.validateElementStatusTypes( granularity, statusType )
    
    #updatter = getattr( self.rsClient, 'update%sStatus' % granularity )
    
    rDict = { 
             'elementName'         : name,
             'statusType'          : statusType,
             'reason'              : reason,
             'tokenOwner'          : tokenOwner,
             'tokenExpiration'     : tokenExpiration
             }
    
    #print ( granularity, name, rDict )
    return self.modifyElementStatus( granularity, **rDict )
    #return self._updateElement( 'ElementStatus', granularity, name, **rDict )
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
    
    #self.rsVal.validateElement( granularity )

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

    for g in ValidRes:
      
      #getter = getattr( self.rsClient, 'get%ss' % g )

      elementName = '%sName' % (g[0].lower() + g[1:])

      rDict  = { elementName : name, 'elementTable' : g }
      resQuery = self._getElement( **rDict )
           
      if not resQuery[ 'Value' ]:
        continue
      else:
        return S_OK( g )

    return S_OK( 'Unknown' )  

  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):

#    self.rsVal.validateElement( granularity )

    toCheck = {}

    now = datetime.utcnow().replace( microsecond = 0 )

    for freqName, freq in checkFrequency.items():
      toCheck[ freqName ] = ( now - timedelta( minutes = freq ) ).isoformat(' ')

    if not kwargs.has_key( 'meta' ):
      kwargs[ 'meta' ] = {}
    if not kwargs['meta'].has_key( 'sort' ): 
      kwargs[ 'meta' ][ 'sort' ] = 'LastCheckTime'

    kwargs[ 'meta' ][ 'or' ] = []
        
    for k,v in toCheck.items():
          
      siteType, status = k.replace( '_CHECK_FREQUENCY', '' ).split( '_' )
      status = status[0] + status[1:].lower()
        
      dict = { 'Status' : status, 'SiteType' : siteType }
      kw   = { 'minor' : { 'LastCheckTime' : v } }
                
      orDict = { 'dict': dict, 'kwargs' : kw }          
                
      kwargs[ 'meta' ][ 'or' ].append( orDict )          
                   
    #getter = getattr( self.rsClient, 'get%sPresent' % granularity )
    #return getter( **kwargs )  
    return self._getElement( '%sPresent' % granularity, **kwargs )

  def getMonitoredStatus( self, granularity, name ):
 
    #getter = getattr( self.rsClient, 'get%sPresent' % granularity )
    
    elementName = '%sName' % ( granularity[0].lower() + granularity[1:] ) 
    kwargs = { elementName : name, 'meta' : { 'columns' : [ 'Status' ] }}
    
    return self._getElement( '%sPresent' % granularity, **kwargs )
    #return getter( **kwargs )

  def getMonitoredsStatusWeb( self, granularity, selectDict, startItem, 
                              maxItems ):

#    self.rsVal.validateElement( granularity )

    if granularity == 'Site':
      paramNames = [ 'SiteName', 'Tier', 'GridType', 'Country', 'StatusType',
                     'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
      paramsList = [ 'SiteName', 'SiteType', 'StatusType','Status', 
                     'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity == 'Service':
      paramNames = [ 'ServiceName', 'ServiceType', 'Site', 'Country', 
                     'StatusType','Status', 'DateEffective', 'FormerStatus', 
                     'Reason' ]
      paramsList = [ 'ServiceName', 'ServiceType', 'SiteName', 'StatusType',
                     'Status', 'DateEffective', 'FormerStatus', 'Reason' ]
    elif granularity == 'Resource':
      paramNames = [ 'ResourceName', 'ServiceType', 'SiteName', 'ResourceType',
                     'Country', 'StatusType','Status', 'DateEffective', 
                     'FormerStatus', 'Reason' ]
      paramsList = [ 'ResourceName', 'ServiceType', 'SiteName', 'GridSiteName', 
                     'ResourceType', 'StatusType','Status', 'DateEffective', 
                     'FormerStatus', 'Reason' ]
    elif granularity == 'StorageElement':
      paramNames = [ 'StorageElementName', 'ResourceName', 'SiteName', 
                     'Country', 'StatusType','Status', 'DateEffective', 
                     'FormerStatus', 'Reason' ]
      paramsList = [ 'StorageElementName', 'ResourceName', 'GridSiteName', 
                     'StatusType','Status', 'DateEffective', 'FormerStatus', 
                     'Reason' ]
    else:
      return S_ERROR( '%s is not a valid granularity' % granularity )
    

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
      paramsList = [ '%sName', 'StatusType', 'Status', 'Reason', 
                     'DateEffective' ]
      elements   = rDict[ 'Expanded%sHistory' % granularity ]
      #hgetter    = getattr( self.rsClient, 'get%ssHhistory' )
      kwargs     = { '%sName' % granularity : elements, 'columns' : paramsList, 'element' : granularity }  
      #elementsH  = hgetter( **kwargs )
      elementsH = self._getElement( 'ElementHistory', **kwargs )
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
      kwargs = { 'meta' : { 'columns' : paramsList }}  
      if granularity == 'Site':
        
        kwargs[ 'siteName' ] = rDict['SiteName']
        kwargs[ 'status' ]   = rDict['Status']
        kwargs[ 'siteType' ] = rDict['SiteType']
        
        sitesList = self._getElement( 'SitePresent', **kwargs )  
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
        
        servicesList = self._getElement( 'ServicePresent', **kwargs )
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
          kw = { 'meta' : { 'columns' : [ 'SiteName' ] } }
          #sites_select = self.rsClient.getSitePresent( **kw )
          sites_select = self._getElement( 'SitePresent', **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                       paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ] 
          
        kw = { 'meta' : { 'columns' : [ 'GridSiteName' ] }, 'siteName' : rDict[ 'SiteName'] }
        
        #gridSites_select = self.rsClient.getSitePresent( siteName = rDict[ 'SiteName'], **kw )
        gridSites_select = self._getElement( 'SitePresent', **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        kwargs[ 'resourceName' ] = rDict['ResourceName']
        kwargs[ 'status' ]       = rDict['Status']
        kwargs[ 'siteType' ]     = rDict['SiteType']
        kwargs[ 'resourceType' ] = rDict['ResourceType']
        kwargs[ 'gridSiteName' ] = gridSites_select

        resourcesList = self._getElement( 'ResourcePresent', **kwargs )
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
              #raise RSSException, 'Error executing getDIRACSiteName'
              return S_ERROR( 'Error executing getDIRACSiteName' )
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
          kw = { 'meta' : { 'columns' : [ 'SiteName' ] } }
          #sites_select = self.rsClient.getSitePresent( **kw )
          sites_select = self._getElement( 'SitePresent', **kw )
          #sites_select = self.getMonitoredsList( 'Site',
          #                                      paramsList = [ 'SiteName' ] )
          rDict[ 'SiteName' ] = [ x[ 0 ] for x in sites_select[ 'Value' ] ]

        kw = { 
               'meta' : { 'columns'  : [ 'GridSiteName' ] }, 
               'siteName' : rDict[ 'SiteName' ] 
              }
        #gridSites_select = self.rsClient.getSitePresent( siteName = rDict[ 'SiteName' ], **kw )
        gridSites_select = self._getElement( 'SitePresent', **kw )
        #gridSites_select = self.getMonitoredsList( 'Site',
        #                                           paramsList = [ 'GridSiteName' ],
        #                                           siteName = rDict[ 'SiteName' ] )
        gridSites_select = [ x[ 0 ] for x in gridSites_select[ 'Value' ] ]

        kwargs[ 'storageElementName' ] = rDict[ 'StorageElementName' ]
        kwargs[ 'status' ]             = rDict[ 'Status' ]
        kwargs[ 'gridSiteName']        = gridSites_select 

        storageElementsList = self._getElement( 'StorageElementPresent', 
                                                 **kwargs )
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
            #raise RSSException, 'Error executing getDIRACSiteName'
            return S_ERROR( 'Error executing getDIRACSiteName' )
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
  # addOrModify PRIVATE FUNCTIONS
  ##############################################################################
  '''

  #def __addOrModifyElement( self, element, *args ):
  def __addOrModifyElement( self, element, kwargs ):

    del kwargs[ 'self' ]
       
    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
    sqlQuery = self._getElement( element, **kwargs )
    
    #kwargs = { 'onlyUniqueKeys' : True }    
    #sqlQuery = self._getElement( element, *args, **kwargs )  
     
    del kwargs[ 'meta' ] 
       
    if sqlQuery[ 'Value' ]:      
      return self._updateElement( element, **kwargs )
    else: 
      sqlQuery = self._insertElement( element, **kwargs )
      if sqlQuery[ 'OK' ]:       
        return self.__setElementInitStatus( element, **kwargs )
      else:
        return sqlQuery  

  def __setElementInitStatus( self, element, **kwargs ):
    
    defaultStatus  = 'Banned'
    defaultReasons = [ 'Added to DB', 'Init' ]

    # This three lines make not much sense, but sometimes statusToSet is '',
    # and we need it as a list to work properly
    statusToSet = ValidStatusTypes[ element ][ 'StatusType' ]
    
    elementName = '%sName' % ( element[0].lower() + element[1:] )
    
    if not isinstance( statusToSet, list ):
      statusToSet = [ statusToSet ]
    
    for statusType in statusToSet:

      # Trick to populate ElementHistory table with one entry. This allows
      # us to use PresentElement views ( otherwise they do not work ).
      for defaultReason in defaultReasons:

        rDict = {}
        rDict[ 'elementName' ] = kwargs[ elementName ]
        rDict[ 'statusType' ]  = statusType
        rDict[ 'status']       = defaultStatus
        rDict[ 'reason' ]      = defaultReason

        #rList = [ kwargs[ elementName ], statusType, defaultStatus, defaultReason ] 
        
        sqlQuery = self.__addOrModifyElementStatus( element, rDict  )        
                
        if not sqlQuery[ 'OK' ]:
          return sqlQuery
        
    return S_OK()     

  def __addOrModifyElementStatus( self, element, rDict ):

    # VALIDATION ?
   
    #rList += self.__setStatusDefaults()
    rDict.update( self.__setStatusDefaults())

    #elementName = '%sName' % ( element[0].lower() + element[1:] )
    kwargs = { 
               'element'        : element,
               'elementName'    : rDict[ 'elementName' ], 
               'statusType'     : rDict[ 'statusType' ], 
               'meta'           : { 'onlyUniqueKeys' : True } 
             }
    #sqlQuery = self._getElement( '%sStatus' % element, **kwargs )

    sqlQuery = self._getElement( 'ElementStatus', **kwargs )


    rDict[ 'element' ] = element

    if not sqlQuery[ 'Value' ]:
      return self._insertElement( 'ElementStatus', **rDict )
      #return self._insertElement( 'ElementStatus', element, *tuple( rList ) )

    
    updateSQLQuery = self._updateElement( 'ElementStatus', **rDict )
    #updateSQLQuery = self._updateElement( 'ElementStatus', element, *tuple( rList ) )
    if not updateSQLQuery[ 'OK' ]:
      return updateSQLQuery 

    sqlQ      = list( sqlQuery[ 'Value' ][ 0 ] )[1:]
    # EHistory.DateEnd = EStatus.DateEffective
    # This is vital for the views !!!!
    #sqlQ[ 6 ] = rList[ 5 ]   
    sqlQ[ 6 ] = rDict[ 'dateEffective' ]
        
    sqlDict = {}
    sqlDict[ 'elementName' ]     = sqlQ[ 0 ]
    sqlDict[ 'statusType' ]      = sqlQ[ 1 ]
    sqlDict[ 'status']           = sqlQ[ 2 ]
    sqlDict[ 'reason' ]          = sqlQ[ 3 ]
    sqlDict[ 'dateCreated' ]     = sqlQ[ 4 ]
    sqlDict[ 'dateEffective' ]   = sqlQ[ 5 ]   
    sqlDict[ 'dateEnd' ]         = rDict[ 'dateEffective' ]
    sqlDict[ 'lastCheckTime' ]   = sqlQ[ 7 ]
    sqlDict[ 'tokenOwner' ]      = sqlQ[ 8 ]
    sqlDict[ 'tokenExpiration' ] = sqlQ[ 9 ]   
           
    sqlDict[ 'element' ] = element       
           
    return self._insertElement( 'ElementHistory', **sqlDict )    
    #return self._insertElement( 'ElementHistory', element , *tuple( sqlQ ) )

  def __setStatusDefaults( self ):#, rDict ):
     
    now    = datetime.utcnow().replace( microsecond = 0 )
    never  = datetime( 9999, 12, 31, 23, 59, 59 ).replace( microsecond = 0 )

    iDict = {}
    iDict[ 'dateCreated'] = now
    iDict[ 'dateEffective'] = now
    iDict[ 'dateEnd'] = never
    iDict[ 'lastCheckTime'] = now
    iDict[ 'tokenOwner'] = 'RS_SVC'
    iDict[ 'tokenExpiration'] = never

    return iDict
    
    #dateCreated, dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration
    #iList = [ now, now, never, now, 'RS_SVC', never ] 
    #return iList

  '''
  ##############################################################################
  # Modify PRIVATE FUNCTIONS
  ##############################################################################
  '''
  
  def __modifyElementStatus( self,kwargs ):
      
    del kwargs[ 'self' ]  
    #del kwargs[ 'element' ]
    
    kwargs[ 'meta' ] = { 'onlyUniqueKeys' : True }
    #sqlQuery = self._getElement( element, **kwargs )
      
    #args = list(args)

    #elementName = '%sName' % ( element[0].lower() + element[1:] )
    #kwargs = { 'elementName' : args[ 0 ], 'statusType' : args[ 1 ] }
    sqlQuery = self._getElement( 'ElementStatus', **kwargs )

    del kwargs[ 'meta' ]

    if not sqlQuery[ 'OK' ]:
      return sqlQuery
    if not sqlQuery[ 'Value' ]:
      _msg = 'Impossible to modify, %s (%s) is not on the DB' 
      _msg = _msg % ( kwargs[ 'elementName' ],kwargs[ 'statusType' ] )
      return S_ERROR( _msg )

    #DateEffective
    if kwargs[ 'dateEffective' ] is None:
      kwargs[ 'dateEffective' ] = datetime.utcnow().replace( microsecond = 0 )

    #LastCheckTime
    if kwargs[ 'lastCheckTime' ] is None:
      kwargs[ 'lastCheckTime' ] = datetime.utcnow().replace( microsecond = 0 )
    
    #updateSQLQuery = self._updateElement( 'ElementStatus', element, 
    #                                       *tuple( args ) )
    updateSQLQuery = self._updateElement( 'ElementStatus', **kwargs ) 
    
    if not updateSQLQuery[ 'OK' ]:
      return updateSQLQuery 

    sqlQ      = list( sqlQuery[ 'Value' ][ 0 ] )[1:]

    sqlDict = {}
    sqlDict[ 'elementName' ]     = sqlQ[ 0 ]
    sqlDict[ 'statusType' ]      = sqlQ[ 1 ]
    sqlDict[ 'status']           = sqlQ[ 2 ]
    sqlDict[ 'reason' ]          = sqlQ[ 3 ]
    sqlDict[ 'dateCreated' ]     = sqlQ[ 4 ]
    sqlDict[ 'dateEffective' ]   = sqlQ[ 5 ]   
    sqlDict[ 'dateEnd' ]         = kwargs[ 'dateEffective' ]
    sqlDict[ 'lastCheckTime' ]   = sqlQ[ 7 ]
    sqlDict[ 'tokenOwner' ]      = sqlQ[ 8 ]
    sqlDict[ 'tokenExpiration' ] = sqlQ[ 9 ]  
    
    sqlDict[ 'element' ] = kwargs[ 'element' ]
    
    # EHistory.DateEnd = EStatus.DateEffective
    # This is vital for the views !!!!
    #sqlQ[ 6 ] = args[ 5 ]   

    #return self._insertElement( 'ElementHistory', element , *tuple( sqlQ ) )  
    return self._insertElement( 'ElementHistory', **sqlDict )
  
  '''
  ##############################################################################
  # remove PRIVATE FUNCTIONS
  ##############################################################################
  '''
  
  def __removeElement( self, element, elementName ):
  
    tables = [ 'ScheduledStatus', 'Status', 'History' ]
    for table in tables:
      
      rDict = { 'elementName' : elementName, 'element' : element }
      
      sqlQuery = self._deleteElement( 'Element%s' % table, **rDict )
      if not sqlQuery[ 'OK' ]:
        return sqlQuery
    
    _elementName = '%sName' % ( element[0].lower() + element[1:])
    rDict = { _elementName : elementName }
    sqlQuery = self._deleteElement( element, **rDict )

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


################################################################################

  '''
  ##############################################################################
  # Getter functions
  ##############################################################################
  '''

  #def _insertElement( self, element, *args, **kwargs ):
  def _insertElement( self, elementTable, **kwargs ):
    
    fname = 'insert%s' % elementTable
    f = getattr( self, fname )
  # return f( *args, **kwargs )
    return f( **kwargs )

  #def _updateElement( self, element, *args, **kwargs ):
  def _updateElement( self, elementTable, **kwargs ):
    
    fname = 'update%s' % elementTable
    f = getattr( self, fname )
    return f( **kwargs )
  # return f( *args, **kwargs )

  #def _getElement( self, element, *args, **kwargs ):
  def _getElement( self, elementTable, **kwargs ):
    
    fname = 'get%s' % elementTable
    f = getattr( self, fname )
    return f( **kwargs )
  # return f( *args, **kwargs )

  #def _deleteElement( self, element, *args, **kwargs ):
  def _deleteElement( self, elementTable, **kwargs ): 
    
    fname = 'delete%s' % elementTable
    f = getattr( self, fname )
    return f( **kwargs )
  # return f( *args, **kwargs )      
      
#  def insert( self, *args, **kwargs ):
#    '''
#    This method calls the insert function in :class:`ResourceStatusDB`, either
#    directly or remotely through the RPC Server :class:`ResourceStatusHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.     
#
#    :Parameters:
#      **\*args** - `[,tuple]`
#        arguments for the mysql query ( must match table columns ! ).
#    
#      **\*\*kwargs** - `[,dict]`
#        metadata for the mysql query. It must contain, at least, `table` key
#        with the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    return self.gate.insert( args, kwargs )
#
#  def update( self, *args, **kwargs ):
#    '''
#    This method calls the update function in :class:`ResourceStatusDB`, either
#    directly or remotely through the RPC Server :class:`ResourceStatusHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.
#      
#    :Parameters:
#      **\*args** - `[,tuple]`
#        arguments for the mysql query ( must match table columns ! ).
#    
#      **\*\*kwargs** - `[,dict]`
#        metadata for the mysql query. It must contain, at least, `table` key
#        with the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    return self.gate.update( args, kwargs )
#
#  def get( self, *args, **kwargs ):
#    '''
#    This method calls the get function in :class:`ResourceStatusDB`, either
#    directly or remotely through the RPC Server :class:`ResourceStatusHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.
#      
#    :Parameters:
#      **\*args** - `[,tuple]`
#        arguments for the mysql query ( must match table columns ! ).
#    
#      **\*\*kwargs** - `[,dict]`
#        metadata for the mysql query. It must contain, at least, `table` key
#        with the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    return self.gate.get( args, kwargs )
#
#  def delete( self, *args, **kwargs ):
#    '''
#    This method calls the delete function in :class:`ResourceStatusDB`, either
#    directly or remotely through the RPC Server :class:`ResourceStatusHandler`. 
#    It does not add neither processing nor validation. If you need to know more 
#    about this method, you must keep reading on the database documentation.
#      
#    :Parameters:
#      **\*args** - `[,tuple]`
#        arguments for the mysql query ( must match table columns ! ).
#    
#      **\*\*kwargs** - `[,dict]`
#        metadata for the mysql query. It must contain, at least, `table` key
#        with the proper table name.
#
#    :return: S_OK() || S_ERROR()
#    '''
#    return self.gate.delete( args, kwargs )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    