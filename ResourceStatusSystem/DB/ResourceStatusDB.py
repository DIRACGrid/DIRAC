__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.Utilities.Decorators  import DBDec
from DIRAC.ResourceStatusSystem.Utilities.MySQLMonkey import MySQLMonkey, localsToDict
from DIRAC.ResourceStatusSystem.Utilities.Validator   import ResourceStatusValidator

################################################################################

class ResourceStatusDB:
  """
  The ResourceStatusDB class is a front-end to the ResourceStatusDB MySQL db.
  If exposes four basic actions per table:
  
    o insert
    o update
    o get
    o delete
  
  all them defined on the MySQL monkey class.
  Moreover, there are a set of key-worded parameters that can be used, specially
  on the getX and deleteX functions ( to know more, again, check the MySQL monkey
  documentation ).
  
  The DB schema has NO foreign keys, so there may be some small consistency checks,
  called validators on the insert and update functions.  
  
  The simplest way to instantiate an object of type :class:`ResourceStatusDB`
  is simply by calling

   >>> rsDB = ResourceStatusDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`.
  But there's the possibility to use other DB classes.
  For example, we could pass custom DB instantiations to it,
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rsDB = ResourceStatusDB( DBin = AnotherDB )

  Alternatively, for testing purposes, you could do:

   >>> from mock import Mock
   >>> mockDB = Mock()
   >>> rsDB = ResourceStatusDB( DBin = mockDB )

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rsDB = ResourceStatusDB( DBin = [ 'UserName', 'Password' ] )
   
  If you want to know more about ResourceStatusDB, scroll down to the end of the
  file. 
  """
 
  # This is an small & temporary 'hack' used for the MySQLMonkey.
  # Check MySQL monkey for more
  # Now is hard-coded for simplicity, eventually will be calculated automatically  
  __TABLES__ = {}

  def __init__( self, *args, **kwargs ):

    if len( args ) == 1:
      if isinstance( args[ 0 ], str ):
        maxQueueSize = 10
      if isinstance( args[ 0 ], int ):
        maxQueueSize = args[ 0 ]
    elif len( args ) == 2:
      maxQueueSize = args[ 1 ]
    elif len( args ) == 0:
      maxQueueSize = 10

    if 'DBin' in kwargs.keys():
      DBin = kwargs[ 'DBin' ]
      if isinstance( DBin, list ):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL( 'localhost', DBin[ 0 ], DBin[ 1 ], 'ResourceStatusDB' )
      else:
        self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB( 'ResourceStatusDB', 'ResourceStatus/ResourceStatusDB', maxQueueSize )
    self.mm    = MySQLMonkey( self )  
    self.rsVal = ResourceStatusValidator( self )

################################################################################
################################################################################

  '''
  ##############################################################################
  # SITE FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'Site' ] = { 'uniqueKeys' : [ 'SiteName' ] } 
 
  @DBDec
  def insertSite( self, siteName, siteType, gridSiteName, **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateSiteType( siteType )
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateSite( self, siteName, siteType, gridSiteName, **kwargs ):
  
    rDict = localsToDict( locals() )   
    # VALIDATION #
    self.rsVal.validateSiteType( siteType, False )   
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getSite( self, siteName, siteType, gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )
  
  @DBDec
  def deleteSite( self, siteName, siteType, gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # SITE STATUS FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'SiteStatus' ] = { 'uniqueKeys' : [ 'SiteName', 'StatusType' ] }

  @DBDec
  def insertSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration, 
                        **kwargs ):
  
    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateSite( siteName )
    self.rsVal.validateStatusType( 'Site', statusType )
    self.rsVal.validateStatus( status )
    # END VALIDATION # 
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration, 
                        **kwargs ):
  
    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateSite( siteName, False )
    self.rsVal.validateStatusType( 'Site', statusType, False )
    self.rsVal.validateStatus( status, False )
    # END VALIDATION # 
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                     dateEffective, dateEnd, lastCheckTime, tokenOwner,
                     tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )
  
  @DBDec
  def deleteSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner,
                        tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )
  
  '''
  ##############################################################################
  # SITE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'SiteScheduledStatus' ] = { 'uniqueKeys' : [ 'SiteName', 'StatusType', 
                                                           'DateEffective' ] }

  @DBDec
  def insertSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateSite( siteName )
    self.rsVal.validateStatusType( 'Site', statusType )
    self.rsVal.validateStatus( status )
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateSite( siteName, False )
    self.rsVal.validateStatusType( 'Site', statusType, False )
    self.rsVal.validateStatus( status, False )
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                              dateEffective, dateEnd, lastCheckTime, tokenOwner,
                              tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.get( rDict, **kwargs )  

  @DBDec
  def deleteSiteScheduledStatus( self, siteName, statusType, status, reason,
                                 dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration,
                                 **kwargs ):
    
    rDict = localsToDict( locals() )  
    # NO VALIDATION #    
    return self.mm.delete( rDict, **kwargs )
  
  '''
  ##############################################################################
  # SITE HISTORY FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'SiteHistory' ] = { 'uniqueKeys' : [ 'SiteName', 'StatusType', 'DateEnd' ] }

  @DBDec
  def insertSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    self.rsVal.validateSite( siteName )
    self.rsVal.validateStatusType( 'Site', statusType )
    self.rsVal.validateStatus( status )    
    return self.mm.insert( rDict, **kwargs )    
  
  @DBDec
  def updateSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateSite( siteName, False )
    self.rsVal.validateStatusType( 'Site', statusType, False )
    self.rsVal.validateStatus( status, False )    
    # END VALIDATION #    
    return self.mm.update( rDict, **kwargs )  
  
  @DBDec
  def getSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                      dateEffective, dateEnd, lastCheckTime, tokenOwner,
                      tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'SiteName', 'SiteHistoryID' ]
    return self.mm.get( rDict, **kwargs )  

  @DBDec
  def deleteSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # SITE PRESENT FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'SitePresent' ] = { 'uniqueKeys' : [ 'SiteName', 'StatusType' ] } 

  @DBDec
  def getSitePresent( self, siteName, siteType, gridSiteName, gridTier,
                      statusType, status, dateEffective, reason, lastCheckTime,
                      tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #   
    return self.mm.get( rDict, **kwargs )

################################################################################
################################################################################

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'Service' ] = { 'uniqueKeys' : [ 'ServiceName' ] }

  @DBDec
  def insertService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = localsToDict( locals() )
    # START VALIDATION #
    self.rsVal.validateServiceType( serviceType )
    self.rsVal.validateSite( siteName )
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = localsToDict( locals() )
    # START VALIDATION #
    self.rsVal.validateServiceType( serviceType, False )
    self.rsVal.validateSite( siteName, False )
    # END VALIDATION #

    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # SERVICE STATUS FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'ServiceStatus'  ] = { 'uniqueKeys' : [ 'ServiceName', 'StatusType' ] }
 
  @DBDec
  def insertServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner,
                           tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateService( serviceName )
    self.rsVal.validateStatusType( 'Service', statusType )
    self.rsVal.validateStatus( status )    
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner,
                           tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateService( serviceName, False )
    self.rsVal.validateStatusType( 'Service', statusType, False )
    self.rsVal.validateStatus( status, False )  
    # END VALIDATION #    
    return self.mm.update( rDict, **kwargs )           

  @DBDec
  def getServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner,
                        tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )
  
  @DBDec
  def deleteServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime, tokenOwner,
                           tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )
  
  '''
  ##############################################################################
  # SERVICE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'ServiceScheduledStatus' ] = { 'uniqueKeys' : [ 'ServiceName', 
                                                             'StatusType', 
                                                             'DateEffective' ] }
   
  @DBDec
  def insertServiceScheduledStatus( self, serviceName, statusType, status,
                                    reason, dateCreated, dateEffective, dateEnd,
                                    lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateService( serviceName )
    self.rsVal.validateStatusType( 'Service', statusType )
    self.rsVal.validateStatus( status )    
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateServiceScheduledStatus( self, serviceName, statusType, status,
                                    reason, dateCreated, dateEffective, dateEnd,
                                    lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateService( serviceName, False )
    self.rsVal.validateStatusType( 'Service', statusType, False )
    self.rsVal.validateStatus( status, False )    
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )
  
  @DBDec
  def getServiceScheduledStatus( self, serviceName, statusType, status,
                                 reason, dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration,
                                 **kwargs ):
    
    rDict = localsToDict( locals() )
    # NO VALIDATION #   
    return self.mm.get( rDict, **kwargs )      

  @DBDec
  def deleteServiceScheduledStatus( self, serviceName, statusType, status,
                                    reason, dateCreated, dateEffective, dateEnd,
                                    lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):
    
    rDict = localsToDict( locals() )
    # NO VALIDATION #   
    return self.mm.delete( rDict, **kwargs )  
  
  '''
  ##############################################################################
  # SERVICE HISTORY STATUS FUNCTIONS
  ##############################################################################
  '''    
  __TABLES__[ 'ServiceHistory' ] = { 'uniqueKeys' : [ 'ServiceName', 'StatusType', 
                                                      'DateEnd' ] }
  
  @DBDec
  def insertServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime, tokenOwner,
                            tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateService( serviceName )
    self.rsVal.validateStatusType( 'Service', statusType )
    self.rsVal.validateStatus( status )    
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateService( serviceName, False )
    self.rsVal.validateStatusType( 'Service', statusType, False )
    self.rsVal.validateStatus( status, False )    
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ServiceName', 'ServiceHistoryID' ]
    return self.mm.get( rDict, **kwargs )    

  @DBDec
  def deleteServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime, tokenOwner,
                            tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )  
  
  '''
  ##############################################################################
  # SERVICE PRESENT FUNCTIONS
  ##############################################################################
  '''     
  __TABLES__[ 'ServicePresent' ] = { 'uniqueKeys' : [ 'ServiceName', 'StatusType' ] }

  @DBDec
  def getServicePresent( self, serviceName, siteName, siteType, serviceType,
                         statusType, status, dateEffective, reason, lastCheckTime,
                         tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

################################################################################
################################################################################

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'Resource' ] = { 'uniqueKeys' : [ 'ResourceName' ] }
  
  @DBDec
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # START VALIDATION #
    self.rsVal.validateResourceType( resourceType )
    self.rsVal.validateServiceType( serviceType )
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # START VALIDATION #
    self.rsVal.validateResourceType( resourceType, False )
    self.rsVal.validateServiceType( serviceType, False )    
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )
  
  @DBDec
  def getResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # RESOURCE STATUS FUNCTIONS
  ##############################################################################
  '''   
  __TABLES__[ 'ResourceStatus' ] = { 'uniqueKeys' : [ 'ResourceName', 'StatusType' ] }
  
  @DBDec
  def insertResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateResource( resourceName )
    self.rsVal.validateStatusType( 'Resource', statusType )
    self.rsVal.validateStatus( status )        
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateResource( resourceName, False )
    self.rsVal.validateStatusType( 'Resource', statusType, False )
    self.rsVal.validateStatus( status, False )            
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # RESOURCE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'ResourceScheduledStatus' ] = { 'uniqueKeys' : [ 'ResourceName', 
                                                               'StatusType', 
                                                               'DateEffective' ] }
  
  @DBDec
  def insertResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateResource( resourceName )
    self.rsVal.validateStatusType( 'Resource', statusType )
    self.rsVal.validateStatus( status )        
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateResource( resourceName, False )
    self.rsVal.validateStatusType( 'Resource', statusType, False )
    self.rsVal.validateStatus( status, False )      
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getResourceScheduledStatus( self, resourceName, statusType, status, 
                                  reason, dateCreated, dateEffective, dateEnd, 
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # RESOURCE HISTORY FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'ResourceHistory' ] = { 'uniqueKeys' : [ 'ResourceName', 'StatusType', 
                                                       'DateEnd' ] }
  
  @DBDec
  def insertResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateResource( resourceName )
    self.rsVal.validateStatusType( 'Resource', statusType )
    self.rsVal.validateStatus( status )        
    # END VALIDATION #    
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateResource( resourceName, False )
    self.rsVal.validateStatusType( 'Resource', statusType, False )
    self.rsVal.validateStatus( status, False )        
    # END VALIDATION #    
    return self.mm.update( rDict, **kwargs )
  
  @DBDec
  def getResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
                          tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #        
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ResourceName', 'ResourceHistoryID' ]
    return self.mm.get( rDict, **kwargs )  

  @DBDec
  def deleteResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #        
    return self.mm.delete( rDict, **kwargs )
  
  '''
  ##############################################################################
  # RESOURCE PRESENT FUNCTIONS
  ##############################################################################
  '''   
  __TABLES__[ 'ResourcePresent' ] = { 'uniqueKeys' : [ 'ResourceName', 'StatusType' ] }

  @DBDec
  def getResourcePresent( self, resourceName, siteName, serviceType, gridSiteName,
                          siteType, resourceType, statusType, status, dateEffective,
                          reason, lastCheckTime, tokenOwner, tokenExpiration,
                          formerStatus, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #        
    return self.mm.get( rDict, **kwargs )

################################################################################
################################################################################
  
  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'StorageElement' ] = { 'uniqueKeys' : [ 'StorageElementName' ] }
  
  @DBDec
  def insertStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    # START VALIDATION #
    self.rsVal.validateResource( resourceName )
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # START VALIDATION #
    self.rsVal.validateResource( resourceName, False )
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # STORAGE ELEMENT STATUS FUNCTIONS
  ##############################################################################
  ''' 
  __TABLES__[ 'StorageElementStatus' ] = { 'uniqueKeys' : [ 'StorageElementName', 
                                                            'StatusType' ] }
  @DBDec
  def insertStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateStorageElement( storageElementName )
    self.rsVal.validateStatusType( 'StorageElement', statusType )
    self.rsVal.validateStatus( status )     
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateStorageElement( storageElementName, False )
    self.rsVal.validateStatusType( 'StorageElement', statusType, False )
    self.rsVal.validateStatus( status, False )      
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElementStatus( self, storageElementName, statusType, status,
                               reason, dateCreated, dateEffective, dateEnd,
                               lastCheckTime, tokenOwner, tokenExpiration,
                               **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )
 
  '''
  ##############################################################################
  # STORAGE ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'StorageElementScheduledStatus' ] = { 'uniqueKeys' : [ 'StorageElementName', 
                                                                     'StatusType', 
                                                                     'DateEffective' ] }
  
  @DBDec
  def insertStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateStorageElement( storageElementName )
    self.rsVal.validateStatusType( 'StorageElement', statusType )
    self.rsVal.validateStatus( status )        
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )  

  @DBDec
  def updateStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateStorageElement( storageElementName, False )
    self.rsVal.validateStatusType( 'StorageElement', statusType, False )
    self.rsVal.validateStatus( status, False )           
    # END VALIDATION #   
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # STORAGE ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'StorageElementHistory'] = { 'uniqueKeys' : [ 'StorageElementName', 
                                                            'StatusType', 
                                                            'DateEnd' ] }

  @DBDec
  def insertStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateStorageElement( storageElementName )
    self.rsVal.validateStatusType( 'StorageElement', statusType )
    self.rsVal.validateStatus( status )        
    # END VALIDATION #    
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    self.rsVal.validateStorageElement( storageElementName, False )
    self.rsVal.validateStatusType( 'StorageElement', statusType, False )
    self.rsVal.validateStatus( status, False )        
    # END VALIDATION #        
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #           
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'StorageElementName', 'StorageElementHistoryID' ]    
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # STORAGE ELEMENT PRESENT FUNCTIONS
  ##############################################################################
  '''    
  __TABLES__[ 'StorageElementPresent']= {'uniqueKeys' : [ 'StorageElementName', 'StatusType' ] }   


  @DBDec
  def getStorageElementPresent( self, storageElementName, resourceName,
                                 gridSiteName, siteType, statusType, status,
                                 dateEffective, reason, lastCheckTime, tokenOwner,
                                 tokenExpiration, formerStatus, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION # 
    return self.mm.get( rDict, **kwargs )

################################################################################
################################################################################

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  ''' 
  __TABLES__[ 'GridSite' ] = { 'uniqueKeys' : [ 'GridSiteName' ] } 

  @DBDec
  def insertGridSite( self, gridSiteName, gridTier, **kwargs ):

    rDict    = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.insert( rDict, **kwargs )  

  @DBDec
  def updateGridSite( self, gridSiteName, gridTier, **kwargs ):

    rDict    = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.update( rDict, **kwargs )
    
  @DBDec
  def getGridSite( self, gridSiteName, gridTier, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteGridSite( self, gridSiteName, gridTier, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
      
'''

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

#  '''
#  ##############################################################################
#  # MISC FUNCTIONS
#  ##############################################################################
#  '''
#  Check the booster ResourceStatusSystem.Utilities.ResourceStatusBooster
#  def setMonitoredToBeChecked( self, monitoreds, granularity, name ):
#    """
#    Set LastCheckTime to 0 to monitored(s)
#
#    :params:
#      :attr:`monitoreds`: string, or a list of strings where each is a ValidRes:
#      which granularity has to be set to be checked
#
#      :attr:`granularity`: string, a ValidRes: from who this set comes
#
#      :attr:`name`: string, name of Site or Resource
#    """
#
#    znever = datetime.min
#
#    if type( monitoreds ) is not list:
#      monitoreds = [ monitoreds ]
#
#    for monitored in monitoreds:
#
#      if monitored == 'Site':
#
#        siteName = self.getGeneralName( granularity, name, monitored )[ 'Value' ]
#        self.updateSiteStatus(siteName = siteName, lastCheckTime = znever )
#
#      elif monitored == 'Service' :
#
#        if granularity =='Site':
#          serviceName = self.getMonitoredsList( 'Service', paramsList = [ 'ServiceName' ],
#                                                siteName = name )[ 'Value' ]
#          if type( serviceName ) is not list:
#            serviceName = [ serviceName ]
#          if serviceName != []:
##            raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No services for site %s" %name
##          else:
#            serviceName = [ x[0] for x in serviceName ]
#            self.updateServiceStatus( serviceName = serviceName, lastCheckTime = znever )
#        else:
#          serviceName = self.getGeneralName( granularity, name, monitored )[ 'Value' ]
#          self.updateServiceStatus( serviceName = serviceName, lastCheckTime = znever )
#
#      elif monitored == 'Resource':
#
#        if granularity == 'Site' :
#          resourceName = self.getMonitoredsList( 'Resource', paramsList = [ 'ResourceName' ],
#                                                 siteName = name )[ 'Value' ]
#          if type( resourceName ) is not list:
#            resourceName = [ resourceName ]
#          if resourceName != []:
#            #raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No resources for site %s" %name
#          #else:
#            resourceName = [ x[0] for x in resourceName ]
#            self.updateResourceStatus( resourceName = resourceName, lastCheckTime = znever )
#
#        elif granularity == 'Service' :
#
#          #siteName = self.getGeneralName( granularity, name, 'Resource' )
#          serviceType, siteName = name.split('@')
#          gridSiteName          = self.getGridSiteName('Site', siteName)[ 'Value' ]
#
#          resourceName = self.getMonitoredsList( monitored, paramsList = [ 'ResourceName' ],
#                                                 gridSiteName = gridSiteName,
#                                                 serviceType = serviceType )[ 'Value' ]
#          if type( resourceName ) is not list:
#            resourceName = [ resourceName ]
#          if resourceName != []:
#         #   raise RSSDBException, where( self, self.setMonitoredToBeChecked ) + " No resources for service %s" %name
#         # else:
#            resourceName = [ x[0] for x in resourceName ]
#            self.updateResourceStatus( resourceName = resourceName, lastCheckTime = znever )
#
#        elif granularity == 'StorageElement':
#          resourceName = self.getGeneralName( granularity,  name, monitored )[ 'Value' ]
#          self.updateResourceStatus( resourceName = resourceName, lastCheckTime = znever )
#
#      # Put read and write together here... too much fomr copy/paste
#      elif monitored == 'StorageElement':
#
#        if granularity == 'Site':
#
#          gridSiteName          = self.getGridSiteName('Site', siteName)[ 'Value' ]
#          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
#                                           gridSiteName = gridSiteName )[ 'Value' ]
#          if type( SEName ) is not list:
#            SEName = [ SEName ]
#          if SEName != []:
#            #pass
#          #else:
#            SEName = [ x[0] for x in SEName ]
#            self.updateStorageElementStatus( storageElementName = SEName, lastCheckTime = znever )
#
#        elif granularity == 'Resource':
#          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
#                                           resourceName = name )[ 'Value' ]
#          if type( SEName ) is not list:
#            SEName = [ SEName ]
#          if SEName == []:
#            pass
##            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for resource %s" %name
#          else:
#            SEName = [ x[0] for x in SEName ]
#            self.updateStorageElementStatus( storageElementName = SEName, lastCheckTime = znever )
#
#        elif granularity == 'Service':
#
#          serviceType, siteName = name.split('@')
#          gridSiteName          = self.getGridSiteName('Site', siteName)[ 'Value' ]
#
#          SEName = self.getMonitoredsList( monitored, paramsList = [ 'StorageElementName' ],
#                                           gridSiteName = gridSiteName )[ 'Value' ]#name.split('@').pop() )[ 'Value' ]
#          if type( SEName ) is not list:
#            SEName = [ SEName ]
#          if SEName != []:
#            #pass
##            raise RSSDBException, where(self, self.setMonitoredToBeChecked) + "No storage elements for service %s" %name
#          #else:
#            SEName = [ x[0] for x in SEName ]
#            self.updateStorageElementStatus( storageElementName = SEName, lastCheckTime = znever )
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF