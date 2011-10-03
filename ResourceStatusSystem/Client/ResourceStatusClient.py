"""
ResourceStatusClient class is a client for requesting info from the ResourceStatusService.
"""
# it crashes epydoc
# __docformat__ = "restructuredtext en"

from DIRAC                                            import S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                       import RPCClient
#from DIRAC.ResourceStatusSystem.Utilities.Exceptions  import InvalidRes, RSSException
#from DIRAC.ResourceStatusSystem.Utilities.Utils       import where
from DIRAC.ResourceStatusSystem                       import ValidRes, ValidStatus, \
  ValidStatusTypes, ValidSiteType, ValidServiceType, ValidResourceType

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB   import ResourceStatusDB 
from DIRAC.ResourceStatusSystem.Utilities.ResourceStatusBooster import ResourceStatusBooster

from DIRAC.ResourceStatusSystem.Utilities.Decorators import ClientExecutor

#import types
#import inspect

#class ClientExecutor( object ):
#  
#  def __init__( self, f ):
#    self.f = f
#    
#  def __get__( self, obj, objtype = None ):
#    return types.MethodType( self, obj, objtype )
#    
#  def __call__( self, *args, **kwargs ):  
# 
#    ins = inspect.getargspec( self.f )
#    newArgs = ( 1 and ins.defaults ) or ()
#
#    if newArgs:
#      #Keyword arguments on function self.f
#      funkwargs = ins.args[ -len( ins.defaults ): ]
#      
#      kw      = dict(zip( funkwargs, ins.defaults ) )
#      kw.update( kwargs ) 
#      newArgs = [ kw[k] for k in funkwargs ]
#      
#      for fk in funkwargs:
#        kwargs.pop( fk, None )  
#      
#    gate    = args[ 0 ].gate  
#    booster = args[ 0 ].booster
#     
#    fname   = self.f.__name__  
#    args    = tuple( list(args)[1:] + list( newArgs )) 
#       
#    try:
#      gateFunction = getattr( gate, fname )
#    except AttributeError:
#      gateFunction = getattr( booster, fname )  
#    except Exception, x:
#      return S_ERROR( x )    
#
#    return gateFunction( *args, **kwargs )

       
class ResourceStatusClient:

################################################################################

  def __init__( self , serviceIn = None ):
    """ Constructor of the ResourceStatusClient class
    """
 
    if serviceIn == None:
      try:
        self.gate = ResourceStatusDB()
      except:  
        self.gate = RPCClient( "ResourceStatus/ResourceStatus" )
        
    else:
      self.gate = serviceIn
      
    self.booster = ResourceStatusBooster( self )  

################################################################################

################################################################################
# Sites functions
################################################################################

################################################################################

  @ClientExecutor
  def addOrModifySite( self, siteName, siteType, gridSiteName ):
    pass

  @ClientExecutor
  def setSiteStatus( self, siteName, statusType, status, reason, tokenOwner, 
                     tokenExpiration = None, dateCreated = None, 
                     dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass

  @ClientExecutor
  def setSiteScheduledStatus( self, siteName, statusType, status, reason, tokenOwner, 
                              tokenExpiration = None, dateCreated = None, 
                              dateEffective = None, dateEnd = None, lastCheckTime = None):
    pass
  
  @ClientExecutor
  def updateSiteStatus( self, siteName, statusType = None, status = None, reason = None, 
                        tokenOwner = None, tokenExpiration = None, dateCreated = None, 
                        dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass

  @ClientExecutor
  def getSites( self, siteName = None, siteType = None, gridSiteName = None, **kwargs ):
    pass
  
  @ClientExecutor
  def getSitesStatus( self, siteName = None, statusType = None, status = None, 
                      reason = None, tokenOwner = None, tokenExpiration = None, 
                      dateCreated = None, dateEffective = None, dateEnd = None, 
                      lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecutor
  def getSitesHistory( self, siteName = None, statusType = None, status = None, 
                       reason = None, tokenOwner = None, tokenExpiration = None, 
                       dateCreated = None, dateEffective = None, dateEnd = None, 
                       lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecutor
  def getSitesScheduledStatus( self, siteName = None, statusType = None, 
                               status = None, reason = None, tokenOwner = None, 
                               tokenExpiration = None, dateCreated = None, 
                               dateEffective = None, dateEnd = None, 
                               lastCheckTime = None, **kwargs):
    pass
  
  @ClientExecutor
  def getSitesPresent( self, siteName = None, siteType = None, gridSiteName = None,
                       gridTier = None, statusType = None, status = None, dateEffective = None,
                       reason = None, lastCheckTime = None, tokenOwner = None,
                       tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
  
  @ClientExecutor
  def deleteSites( self, siteName ):
    pass
  
  @ClientExecutor
  def deleteSitesScheduledStatus( self, siteName = None, statusType = None, 
                                  status = None, reason = None, tokenOwner = None, 
                                  tokenExpiration = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None):
    pass
  
  @ClientExecutor
  def deleteSitesHistory( self, siteName = None, statusType = None, status = None, 
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):
    pass
  
################################################################################

################################################################################
# Services functions
################################################################################

################################################################################

  @ClientExecutor
  def addOrModifyService( self, serviceName, serviceType, siteName ):
    pass
  
  @ClientExecutor  
  def setServiceStatus( self, serviceName, statusType, status, reason, tokenOwner, 
                        tokenExpiration = None, dateCreated = None, dateEffective = None, 
                        dateEnd = None, lastCheckTime = None ):
    pass
   
  @ClientExecutor  
  def setServiceScheduledStatus( self, serviceName, statusType, status, reason, 
                                 tokenOwner, tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None ):
    pass
  
  @ClientExecutor  
  def updateServiceStatus( self, serviceName, statusType = None, status = None, 
                           reason = None, tokenOwner = None, tokenExpiration = None, 
                           dateCreated = None, dateEffective = None, dateEnd = None, 
                           lastCheckTime = None ):
    pass
  
  @ClientExecutor
  def getServices( self, serviceName = None, serviceType = None, siteName = None, 
                   **kwargs ):
    pass
  
  @ClientExecutor  
  def getServicesStatus( self, serviceName = None, statusType = None, status = None, 
                         reason = None, tokenOwner = None, tokenExpiration = None, 
                         dateCreated = None, dateEffective = None, dateEnd = None, 
                         lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecutor  
  def getServicesHistory( self, serviceName = None, statusType = None, status = None, 
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecutor  
  def getServicesScheduledStatus( self, serviceName = None, statusType = None, 
                                 status = None, reason = None, tokenOwner = None, 
                                 tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecutor  
  def getServicesPresent( self, serviceName = None, siteName = None, siteType = None, 
                          serviceType = None, statusType = None, status = None, 
                          dateEffective = None, reason = None, lastCheckTime = None, 
                          tokenOwner = None, tokenExpiration = None, 
                          formerStatus = None, **kwargs ):
    pass
  
  @ClientExecutor  
  def deleteServices( self, serviceName ):
    pass
  
  @ClientExecutor  
  def deleteServicesScheduledStatus( self, serviceName = None, statusType = None, 
                                     status = None, reason = None, tokenOwner = None, 
                                     tokenExpiration = None, dateCreated = None, 
                                     dateEffective = None, dateEnd = None, 
                                     lastCheckTime = None):
    pass
  
  @ClientExecutor  
  def deleteServicesHistory( self, serviceName = None, statusType = None, status = None, 
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):                                              
    pass
  
################################################################################

################################################################################
# Resources functions
################################################################################

################################################################################

  @ClientExecutor  
  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName,
                           gridSiteName ):
    pass
  
  @ClientExecutor      
  def setResourceStatus( self, resourceName, statusType, status, reason, tokenOwner, 
                         tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass
  
  @ClientExecutor      
  def setResourceScheduledStatus( self, resourceName, statusType, status, reason, 
                                  tokenOwner, tokenExpiration = None, dateCreated = None, 
                                  dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass
  
  @ClientExecutor          
  def updateResourceStatus( self, resourceName, statusType = None, status = None, reason = None, 
                         tokenOwner = None, tokenExpiration = None, dateCreated = None, 
                         dateEffective = None, dateEnd = None, lastCheckTime = None ):
    pass
  
  @ClientExecutor      
  def getResources( self, resourceName = None, resourceType = None, 
                    serviceType = None, siteName = None, gridSiteName = None, 
                    **kwargs ):
    pass
  
  @ClientExecutor      
  def getResourcesStatus( self, resourceName = None, statusType = None, status = None,
                          reason = None, tokenOwner = None, tokenExpiration = None, 
                          dateCreated = None, dateEffective = None, dateEnd = None, 
                          lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecutor      
  def getResourcesHistory( self, resourceName = None, statusType = None, status = None,
                           reason = None, tokenOwner = None, tokenExpiration = None, 
                           dateCreated = None, dateEffective = None, dateEnd = None, 
                           lastCheckTime = None, **kwargs ):
    pass
  
  @ClientExecutor        
  def getResourcesScheduledStatus( self, resourceName = None, statusType = None, status = None,
                                  reason = None, tokenOwner = None, tokenExpiration = None, 
                                  dateCreated = None, dateEffective = None, dateEnd = None, 
                                  lastCheckTime = None, **kwargs):
    pass
  
  @ClientExecutor      
  def getResourcesPresent( self, resourceName = None, siteName = None, serviceType = None,
                           gridSiteName = None, siteType = None, resourceType = None,
                           statusType = None, status = None, dateEffective = None, 
                           reason = None, lastCheckTime = None, tokenOwner = None, 
                           tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
  
  @ClientExecutor     
  def deleteResources( self, resourceName ):
    pass
  
  @ClientExecutor      
  def deleteResourcesScheduledStatus( self, resourceName = None, statusType = None, 
                                      status = None, reason = None, tokenOwner = None, 
                                      tokenExpiration = None, dateCreated = None, 
                                      dateEffective = None, dateEnd = None, 
                                      lastCheckTime = None):
    pass
  
  @ClientExecutor      
  def deleteResourcesHistory( self, resourceName = None, statusType = None, status = None, 
                              reason = None, tokenOwner = None, tokenExpiration = None, 
                              dateCreated = None, dateEffective = None, dateEnd = None, 
                              lastCheckTime = None, **kwargs ):
    pass

################################################################################

################################################################################
# StorageElements functions
################################################################################

################################################################################

  @ClientExecutor   
  def addOrModifyStorageElement( self, storageElementName, resourceName, 
                                 gridSiteName ):
    pass
  
  @ClientExecutor       
  def setStorageElementStatus( self, storageElementName, statusType, status, 
                               reason, tokenOwner, tokenExpiration = None, 
                               dateCreated = None, dateEffective = None, dateEnd = None, 
                               lastCheckTime = None ):
    pass
      
  @ClientExecutor       
  def setStorageElementScheduledStatus( self, storageElementName, statusType, status, 
                                        reason, tokenOwner, tokenExpiration = None, 
                                        dateCreated = None, dateEffective = None, 
                                        dateEnd = None, lastCheckTime = None ):
    pass
      
  @ClientExecutor       
  def updateStorageElementStatus( self, storageElementName, statusType = None, status = None, 
                                 reason = None , tokenOwner = None, tokenExpiration = None, 
                                 dateCreated = None, dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None ):
    pass
      
  @ClientExecutor       
  def getStorageElements( self, storageElementName = None, resourceName = None, 
                          gridSiteName = None, **kwargs ):
    pass
      
  @ClientExecutor       
  def getStorageElementsStatus( self, storageElementName = None, statusType = None, 
                                status = None, reason = None, tokenOwner = None, 
                                tokenExpiration = None, dateCreated = None, 
                                dateEffective = None, dateEnd = None, 
                                lastCheckTime = None, **kwargs ):
    pass    
    
  @ClientExecutor       
  def getStorageElementsHistory( self, storageElementName = None, statusType = None, 
                                 status = None, reason = None, tokenOwner = None, 
                                 tokenExpiration = None, dateCreated = None, 
                                 dateEffective = None, dateEnd = None, 
                                 lastCheckTime = None, **kwargs ):
    pass
      
  @ClientExecutor       
  def getStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                         status = None, reason = None, tokenOwner = None, 
                                         tokenExpiration = None, dateCreated = None, 
                                         dateEffective = None, dateEnd = None, 
                                         lastCheckTime = None, **kwargs ):
    pass
      
  @ClientExecutor      
  def getStorageElementsPresent( self, storageElementName = None, resourceName = None, 
                                 gridSiteName = None, siteType = None, statusType = None, 
                                 status = None, dateEffective = None, reason = None, 
                                 lastCheckTime = None, tokenOwner = None,
                                 tokenExpiration = None, formerStatus = None, **kwargs ):
    pass
      
  @ClientExecutor                                    
  def deleteStorageElements( self, storageElementName ):
    pass
      
  @ClientExecutor  
  def deleteStorageElementsScheduledStatus( self, storageElementName = None, statusType = None, 
                                            status = None, reason = None, tokenOwner = None, 
                                            tokenExpiration = None, dateCreated = None, 
                                            dateEffective = None, dateEnd = None, 
                                            lastCheckTime = None ):
    pass
      
  @ClientExecutor
  def deleteStorageElementsHistory( self, storageElementName = None, statusType = None, 
                                    status = None, reason = None, tokenOwner = None, 
                                    tokenExpiration = None, dateCreated = None, 
                                    dateEffective = None, dateEnd = None, 
                                    lastCheckTime = None, **kwargs ):          
    pass

################################################################################

################################################################################
# Stats functions
################################################################################

################################################################################
  
  @ClientExecutor
  def getServiceStats( self, siteName, statusType = None ):
    pass
    
  @ClientExecutor  
  def getResourceStats( self, element, name, statusType = None ):
    pass
    
  @ClientExecutor  
  def getStorageElementStats( self, element, name, statusType = None ):      
    pass

################################################################################

################################################################################
# GridSites functions
################################################################################

################################################################################

  @ClientExecutor
  def addOrModifyGridSite( self, gridSiteName, gridTier ):
    pass
  
  @ClientExecutor    
  def getGridSites( self, gridSiteName = None, gridTier = None, **kwargs ):
    pass

  @ClientExecutor    
  def deleteGridSites( self, gridSiteName ):        
    pass

################################################################################

################################################################################
# BOOSTER functions
################################################################################

################################################################################

  @ClientExecutor 
  def getGeneralName( self, from_element, name, to_element ):
    pass
    
  @ClientExecutor     
  def getGridSiteName( self, granularity, name ):
    pass
    
  @ClientExecutor     
  def getTokens( self, granularity, name = None, tokenExpiration = None, 
                 statusType = None, **kwargs ): 
    pass
   
  @ClientExecutor    
  def setToken( self, granularity, name, statusType, reason, tokenOwner, 
                tokenExpiration ):
    pass
    
  @ClientExecutor     
  def setReason( self, granularity, name, statusType, reason ):     
    pass
  
  @ClientExecutor     
  def setDateEnd( self, granularity, name, statusType, dateEnd ):     
    pass 
    
  @ClientExecutor     
  def whatIs( self, name ):  
    pass
  
  @ClientExecutor   
  def getStuffToCheck( self, granularity, checkFrequency, **kwargs ):
    pass    
        
################################################################################

################################################################################
# General config valid elements functions
################################################################################

################################################################################

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

################################################################################
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

##############################################################################
#
#  def getServiceStats( self, siteName, statusType ):
#    """
#    Returns simple statistics of active, probing and banned services of a site;
#
#    :Parameters:
#      `granularity`
#        string, has to be 'Site'
#
#      `name`
#        string - a service name
#
#    :returns:
#      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
#    """
#
##    if granularity not in ( 'Site', 'Sites' ):
##      raise InvalidRes, where( self, self.getServiceStats )
#
#    return self.rsS.getServiceStats( siteName, statusType )
##    if not res[ 'OK' ]:
##      raise RSSException, where( self, self.getServiceStats ) + " " + res[ 'Message' ]
#
##    return res
#
##############################################################################
#
#  def getResourceStats( self, granularity, name, statusType ):
#    """
#    Returns simple statistics of active, probing and banned resources of a site or a service;
#
#    :Parameters:
#      `granularity`
#        string, should be in ('Site', 'Service')
#
#      `name`
#        string, name of site or service
#
#    :returns:
#      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
#    """
#
#    res = self.rsS.getResourceStats( granularity, name, statusType )
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getResourceStats ) + " " + res[ 'Message' ]
#
#    #return res[ 'Value' ]
#    return res
#
##############################################################################
#
#  def getStorageElementStats(self, granularity, name, statusType ):
#    """
#    Returns simple statistics of active, probing and banned storageElements of a site or a resource;
#
#    :Parameters:
#      `granularity`
#        string, should be in ['Site', 'Resource']
#
#      `name`
#        string, name of site or resource
#
#      `access`
#        string, either Read or Write
#
#    :returns:
#      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
#    """
#
#    res = self.rsS.getStorageElementStats( granularity, name, statusType )
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getStorageElementStats ) + " " + res[ 'Message' ]
#
#    #return res[ 'Value' ]
#    return res
#
##############################################################################
#
#
#  def getPeriods( self, granularity, name, status, hours ):
#    """
#    Returns a list of periods of time where name was in status
#
#    :returns:
#      {
#        'Periods':[list of periods]
#      }
#    """
#    #da migliorare!
#
#    if granularity not in ValidRes:
#      raise InvalidRes, where( self, self.getPeriods )
#
#    res = self.rsS.getPeriods( granularity, name, status, hours )
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getPeriods ) + " " + res[ 'Message' ]
#
#    #return { 'Periods' : res[ 'Value' ] }
#    return res
#
##############################################################################
#
#  def getGeneralName( self, granularity, name, toGranularity ):
#    """
#    Returns simple statistics of active, probing and banned storageElements of a site or a resource;
#
#    :Parameters:
#      `granularity`
#        string, should be a ValidRes
#
#      `name`
#        string, name of site or resource
#
#      `toGranularity`
#        string, should be a ValidRes
#
#    :returns:
#      { 'Active':xx, 'Probing':yy, 'Banned':zz, 'Total':xyz }
#    """
#
#    res = self.rsS.getGeneralName( granularity, name, toGranularity )
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getGeneralName ) + " " + res[ 'Message' ]
#
#    #return res[ 'Value' ]
#    return res
#
##############################################################################
#
#  def getMonitoredStatus( self, granularity, names ):
#    """
#    Returns RSS status of names (could be a string or a list of strings)
#
#    :Parameters:
#      `granularity`
#        string, should be a ValidRes
#
#      `names`
#        string or dict, name(s) of the ValidRes
#
#    :returns:
#      'Active'|'Probing'|'Banned'|None
#    """
#
#    if not isinstance( names, list ):
#      names = [ names ]
#
#    statusList = []
#
#    for name in names:
#      if granularity == 'Site':
#        res = self.rsS.g( { 'SiteName' : name }, 0, 1 )
#      elif granularity == 'Service':
#        res = self.rsS.getServicesStatusWeb( { 'ServiceName' : name }, 0, 1 )
#      elif granularity  == 'Resource':
#        res = self.rsS.getResourcesStatusWeb( { 'ResourceName' : name }, 0, 1 )
#      elif granularity == 'StorageElement':
#        res = self.rsS.getStorageElementsStatusWeb( { 'StorageElementName' : name }, 0, 1 )
#      else:
#        raise InvalidRes, where( self, self.getMonitoredStatus )
#
#      if not res[ 'OK' ]:
#        raise RSSException, where( self, self.getMonitoredStatus ) + " " + res[ 'Message' ]
#      else:
#        try:
#            
#          if granularity == 'Resource':
#            statusList.append( res[ 'Value' ][ 'Records' ][ 0 ][ 6 ] )
#          else:
#            statusList.append( res[ 'Value' ][ 'Records' ][ 0 ][ 5 ] )
#        except IndexError:
#          return S_ERROR( None )
#
#    return S_OK( statusList )
#
##############################################################################
#
#  def getGridSiteName( self, granularity, name ):
#    """
#    Returns the grid site name (what is in GOC BD)
#
#    :Parameters:
#      `granularity`
#        string, should be a ValidRes
#
#      `name`
#        string, name of site or resource
#    """
#
#    res = self.rsS.getGridSiteName( granularity, name )
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getGridSiteName ) + " " + res[ 'Message' ]
#
#    #return res[ 'Value' ]
#    return res
#
##############################################################################
#
#  def getResourcesList( self ):
#    """
#    Returns the list of resources in the RSS DB
#
#    """
#
#    res = self.rsS.getResourcesList()
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getResourcesList ) + " " + res[ 'Message' ]
#    #
#    #return res[ 'Value' ]
#    return res
#
##############################################################################
#
#  def getStorageElementsList( self, access ):
#    """
#    Returns the list of storage elements in the RSS DB
#
#    """
#
#    res = self.rsS.getStorageElementsList( access )
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getStorageElementsList ) + " " + res[ 'Message' ]
#    #
#    #return res[ 'Value' ]
#    return res
#
##############################################################################
#
#  def getServicesList( self ):
#    """
#    Returns the list of services in the RSS DB
#
#    """
#
#    res = self.rsS.getServicesList()
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getServicesList ) + " " + res[ 'Message' ]
#
#    return res
#
##############################################################################
#
#  def getSitesList( self ):
#    """
#    Returns the list of sites in the RSS DB
#
#    """
#
#    res = self.rsS.getSitesList()
#    if not res[ 'OK' ]:
#      raise RSSException, where( self, self.getSitesList ) + " " + res[ 'Message' ]
#
#    return res
#
##############################################################################
#
#  def getStorageElement( self, name, access ):
#
#    subaccess = access
#
#    if access == 'Remove':
#      subaccess = 'Read'
#
#    res = self.rsS.getStorageElement( name, subaccess )
#    if not res['OK']:
#      raise RSSException, where( self, self.getStorageElement ) + " " + res[ 'Message' ]
#
#    if res['Value']:
#
#      res = res[ 'Value' ]
#
#      if res[ 0 ].endswith( 'ARCHIVE' ) and ( access == 'Read' or access == 'Remove' ):
#        status = gConfig.getValue( '/Resources/StorageElements/%s/%sAccess' % ( name, access ) )
#
#        if status:
#          res[ 1 ] = status
#        else:
#          return S_ERROR( 'StorageElement %s, access %s not found' % ( name, access ) )
#
#      return S_OK( res )
#    else:
#      return S_ERROR( 'Unknown SE' )
#
##############################################################################
#
#  def setStorageElementStatus( self, name, status, reason, token, access ):
#
#    res = self.rsS.setStorageElementStatus( name, status, reason, token, access )
#    if not res['OK']:
#      raise RSSException, where( self, self.setStorageElementStatus ) + " " + res[ 'Message' ]
#
#    return S_OK()
#
##############################################################################
#
##  def updateStorageElement( self, name , access, status ):
#
##    se = self.rsS.getStorageElement( name, access )
#
##    self.addOrModifyStorageElement(  )
#
##############################################################################
#
#  def getResource( self, name, access ):
#
#    res = self.rsS.getResource( name, access )
#    if not res['OK']:
#      raise RSSException, where( self, self.getResource ) + " " + res[ 'Message' ]
#
#    if res['Value']:
#      return S_OK( res[ 'Value' ][ 0 ] )
#    else:
#      return S_ERROR( 'Unknown Resource' )
#
#
##############################################################################
#
#  def getService( self, name, access ):
#
#    res = self.rsS.getService( name, access )
#    if not res['OK']:
#      raise RSSException, where( self, self.getService ) + " " + res[ 'Message' ]
#
#    if res['Value']:
#      return S_OK( res[ 'Value' ][ 0 ] )
#    else:
#      return S_ERROR( 'Unknown Service' )
#
#
##############################################################################
#
#  def getSite( self, name, access ):
#
#    res = self.rsS.getSite( name, access )
#    if not res['OK']:
#      raise RSSException, where( self, self.getSite ) + " " + res[ 'Message' ]
#
#    if res[ 'OK' ] and res['Value']:
#      return S_OK( res[ 'Value' ][ 0 ] )
#
#    else:
#      return S_ERROR( 'Unknown Site' )
#
##############################################################################
