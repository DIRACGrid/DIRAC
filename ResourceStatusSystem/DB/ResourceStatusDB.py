"""
The ResourcesStatusDB module contains a couple of exception classes, and a
class to interact with the ResourceStatus DB.
"""

#from datetime import datetime

from DIRAC import S_OK#, S_ERROR

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSDBException
from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidStatusTypes

from DIRAC.ResourceStatusSystem.Utilities.MySQLMonkey import MySQLMonkey, localsToDict
from DIRAC.ResourceStatusSystem.Utilities.Validator import ResourceStatusValidator

from DIRAC.ResourceStatusSystem.Utilities.Decorators import DBDec

################################################################################

class ResourceStatusDB:
  """
  The ResourcesStatusDB class is a front-end to the Resource Status Database.

  The simplest way to instantiate an object of type :class:`ResourceStatusDB`
  is simply by calling

   >>> rsDB = ResourceStatusDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`.
  But there's the possibility to use other DB classes.
  For example, we could pass custom DB instantiations to it,
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rsDB = ResourceStatusDB(DBin = AnotherDB)

  Alternatively, for testing purposes, you could do:

   >>> from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
   >>> mockDB = Mock()
   >>> rsDB = ResourceStatusDB(DBin = mockDB)

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rsDB = ResourceStatusDB(DBin = ['UserName', 'Password'])

  """
 
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

#  @DBDec
#  def addOrModifySite( self, siteName, siteType, gridSiteName, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( siteName )
#    self.rsVal.validateSiteType( siteType )
#    self.rsVal.validateGridSite( gridSiteName )
#    # END VALIDATION #
#     
#    return self._addOrModifyElement( rDict, **kwargs )

  __TABLES__[ 'Site' ] = {'uniqueKeys' : [ 'SiteName' ] } 
  
  @DBDec
  def insertSite( self, siteName, siteType, gridSiteName, **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateSite( self, siteName, siteType, gridSiteName, **kwargs ):
  
    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getSite( self, siteName, siteType, gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteSite( self, siteName, siteType, gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateMultipleNames( siteName )
    # END VALIDATION #

    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # SITE STATUS FUNCTIONS
  ##############################################################################
  '''

#  @DBDec
#  def addOrModifySiteStatus( self, siteName, statusType, status, reason, dateCreated,
#                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
#                             tokenExpiration, **kwargs ):
#  
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( siteName )
#    self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
#    # END VALIDATION # 
#       
#    return self._addOrModifyElementStatus( rDict, **kwargs )

  __TABLES__[ 'SiteStatus' ] = {'uniqueKeys' : [ 'SiteName', 'StatusType' ] }

  @DBDec
  def insertSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration, 
                        **kwargs ):
  
    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( siteName )
    #self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
    # END VALIDATION # 
    #return self._addOrModifyElementStatus( rDict, **kwargs )
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner, tokenExpiration, 
                        **kwargs ):
  
    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( siteName )
    #self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
    # END VALIDATION # 
    #return self._addOrModifyElementStatus( rDict, **kwargs )
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                     dateEffective, dateEnd, lastCheckTime, tokenOwner,
                     tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    return self.mm.get( rDict, **kwargs )
  
  @DBDec
  def deleteSiteStatus( self, siteName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner,
                        tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    return self.mm.delete( rDict, **kwargs )
  
  '''
  ##############################################################################
  # SITE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
    
#  @DBDec
#  def addOrModifySiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
#                                      dateEffective, dateEnd, lastCheckTime, tokenOwner,
#                                      tokenExpiration, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( siteName )
#    self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
#    # END VALIDATION #
#    
#    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )

  __TABLES__[ 'SiteScheduledStatus' ] = {'uniqueKeys' : [ 'SiteName', 'StatusType', 'DateEffective' ] }

  @DBDec
  def insertSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( siteName )
    #self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    #return self._addOrModifyElementScheduledStatus( rDict, **kwargs )
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                                 dateEffective, dateEnd, lastCheckTime, tokenOwner,
                                 tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( siteName )
    #self.rsVal.validateSingleElementStatusType( 'Site', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    #return self._addOrModifyElementScheduledStatus( rDict, **kwargs )
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getSiteScheduledStatus( self, siteName, statusType, status, reason, dateCreated,
                              dateEffective, dateEnd, lastCheckTime, tokenOwner,
                              tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )  

  @DBDec
  def deleteSiteScheduledStatus( self, siteName, statusType, status, reason,
                                 dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration,
                                 **kwargs ):
    
    rDict = localsToDict( locals() )  
    return self.mm.delete( rDict, **kwargs )
  
  '''
  ##############################################################################
  # SITE HISTORY FUNCTIONS
  ##############################################################################
  '''  

  __TABLES__[ 'SiteHistory' ] = {'uniqueKeys' : [ 'SiteName', 'StatusType', 'DateEnd' ] }

  @DBDec
  def insertSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.insert( rDict, **kwargs )    
  
  @DBDec
  def updateSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.update( rDict, **kwargs )  
  
  @DBDec
  def getSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                      dateEffective, dateEnd, lastCheckTime, tokenOwner,
                      tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'SiteName', 'SiteHistoryID' ]
    return self.mm.get( rDict, **kwargs )  

  @DBDec
  def deleteSiteHistory( self, siteName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # SITE PRESENT FUNCTIONS
  ##############################################################################
  '''  

  __TABLES__[ 'SitePresent' ] = {'uniqueKeys' : [ 'SiteName', 'StatusType' ] } 

  @DBDec
  def getSitePresent( self, siteName, siteType, gridSiteName, gridTier,
                      statusType, status, dateEffective, reason, lastCheckTime,
                      tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = localsToDict( locals() )   
    return self.mm.get( rDict, **kwargs )


################################################################################
################################################################################

  '''
  ##############################################################################
  # SERVICE FUNCTIONS
  ##############################################################################
  '''

#  @DBDec
#  def addOrModifyService( self, serviceName, serviceType, siteName, **kwargs ):
#
#    rDict = localsToDict( locals() )
#
#    # START VALIDATION #
#    self.rsVal.validateName( serviceName )
#    self.rsVal.validateServiceType( serviceType )
#    self.rsVal.validateSite( siteName )
#    # END VALIDATION #
#
#    return self._addOrModifyElement( rDict, **kwargs )

  __TABLES__[ 'Service' ] = {'uniqueKeys' : [ 'ServiceName' ] }

  @DBDec
  def insertService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = localsToDict( locals() )

    # START VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateServiceType( serviceType )
    #self.rsVal.validateSite( siteName )
    # END VALIDATION #

    return self.mm.insert( rDict, **kwargs )

    #return self._addOrModifyElement( rDict, **kwargs )
  
  @DBDec
  def updateService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = localsToDict( locals() )

    # START VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateServiceType( serviceType )
    #self.rsVal.validateSite( siteName )
    # END VALIDATION #

    return self.mm.update( rDict, **kwargs )

    #return self._addOrModifyElement( rDict, **kwargs )

  @DBDec
  def getService( self, serviceName, serviceType, siteName, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteService( self, serviceName, **kwargs ):

    rDict = localsToDict( locals() )
    #VALIDATION#
    #self.rsVal.validateMultipleNames( serviceName )
    # END VALIDATION #

    #return self._deleteElement( rDict, **kwargs )
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # SERVICE STATUS FUNCTIONS
  ##############################################################################
  '''

  __TABLES__[ 'ServiceStatus'  ] = {'uniqueKeys' : [ 'ServiceName', 'StatusType' ] }

#  @DBDec
#  def addOrModifyServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
#                                dateEffective, dateEnd, lastCheckTime,tokenOwner,
#                                tokenExpiration, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( serviceName )
#    self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
#    # END VALIDATION #
#       
#    return self._addOrModifyElementStatus( rDict, **kwargs )    
  
  @DBDec
  def insertServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner,
                           tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    #return self._addOrModifyElementStatus( rDict, **kwargs )    
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime,tokenOwner,
                           tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
     
    return self.mm.update( rDict, **kwargs )   
    #return self._addOrModifyElementStatus( rDict, **kwargs )        

  @DBDec
  def getServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                        dateEffective, dateEnd, lastCheckTime, tokenOwner,
                        tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )
  
  @DBDec
  def deleteServiceStatus( self, serviceName, statusType, status, reason, dateCreated,
                           dateEffective, dateEnd, lastCheckTime, tokenOwner,
                           tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )
  
  '''
  ##############################################################################
  # SERVICE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  
  __TABLES__[ 'ServiceScheduledStatus' ] = {'uniqueKeys' : [ 'ServiceName', 'StatusType', 'DateEffective' ] }
  
#  @DBDec
#  def addOrModifyServiceScheduledStatus( self, serviceName, statusType, status,
#                                         reason, dateCreated, dateEffective, dateEnd,
#                                         lastCheckTime, tokenOwner, tokenExpiration,
#                                         **kwargs ):
#    
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( serviceName )
#    self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
#    # END VALIDATION #
#    
#    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )
  
  @DBDec
  def insertServiceScheduledStatus( self, serviceName, statusType, status,
                                    reason, dateCreated, dateEffective, dateEnd,
                                    lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateServiceScheduledStatus( self, serviceName, statusType, status,
                                    reason, dateCreated, dateEffective, dateEnd,
                                    lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self.mm.update( rDict, **kwargs )
  
  @DBDec
  def getServiceScheduledStatus( self, serviceName, statusType, status,
                                 reason, dateCreated, dateEffective, dateEnd,
                                 lastCheckTime, tokenOwner, tokenExpiration,
                                 **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self.mm.get( rDict, **kwargs )      

  @DBDec
  def deleteServiceScheduledStatus( self, serviceName, statusType, status,
                                    reason, dateCreated, dateEffective, dateEnd,
                                    lastCheckTime, tokenOwner, tokenExpiration,
                                    **kwargs ):
    
    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( serviceName )
    #self.rsVal.validateSingleElementStatusType( 'Service', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    return self.mm.delete( rDict, **kwargs )  
  
  '''
  ##############################################################################
  # SERVICE HISTORY STATUS FUNCTIONS
  ##############################################################################
  '''    

  __TABLES__[ 'ServiceHistory' ] = {'uniqueKeys' : [ 'ServiceName', 'StatusType', 'DateEnd' ] }
  
  @DBDec
  def insertServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime, tokenOwner,
                            tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                         dateEffective, dateEnd, lastCheckTime, tokenOwner,
                         tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ServiceName', 'ServiceHistoryID' ]
    return self.mm.get( rDict, **kwargs )    

  @DBDec
  def deleteServiceHistory( self, serviceName, statusType, status, reason, dateCreated,
                            dateEffective, dateEnd, lastCheckTime, tokenOwner,
                            tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.delete( rDict, **kwargs )  
  
  '''
  ##############################################################################
  # SERVICE PRESENT FUNCTIONS
  ##############################################################################
  '''     
  
  __TABLES__[ 'ServicePresent' ] = {'uniqueKeys' : [ 'ServiceName', 'StatusType' ] }

  @DBDec
  def getServicePresent( self, serviceName, siteName, siteType, serviceType,
                         statusType, status, dateEffective, reason, lastCheckTime,
                         tokenOwner, tokenExpiration, formerStatus, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

#  @DBDec
#  def deleteService( self, serviceName, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    #VALIDATION#
#    self.rsVal.validateMultipleNames( serviceName )
#    # END VALIDATION #
#
#    return self._deleteElement( rDict, **kwargs )

################################################################################
################################################################################

  '''
  ##############################################################################
  # RESOURCE FUNCTIONS
  ##############################################################################
  '''

  __TABLES__[ 'Resource' ] = {'uniqueKeys' : [ 'ResourceName' ] }
  
#  @DBDec
#  def addOrModifyResource( self, resourceName, resourceType, serviceType, siteName,
#                           gridSiteName, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    
#    # START VALIDATION #
#    self.rsVal.validateName( resourceName )  
#    self.rsVal.validateResourceType( resourceType )
#    self.rsVal.validateServiceType( serviceType )
#    # Not used, some resources have NULL site !!
##    self.rsVal.validateSite( siteName )
#    self.rsVal.validateGridSite( gridSiteName )
#    # END VALIDATION #
#
#    return self._addOrModifyElement( rDict, **kwargs )
  
  @DBDec
  def insertResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    
    # START VALIDATION #
    #self.rsVal.validateName( resourceName )  
    #self.rsVal.validateResourceType( resourceType )
    #self.rsVal.validateServiceType( serviceType )
    # Not used, some resources have NULL site !!
#    self.rsVal.validateSite( siteName )
    #self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #

    #return self._addOrModifyElement( rDict, **kwargs )
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    
    # START VALIDATION #
    # END VALIDATION #

    return self.mm.update( rDict, **kwargs )
  
  @DBDec
  def getResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    
    # START VALIDATION #
    # END VALIDATION #

    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteResource( self, resourceName, resourceType, serviceType, siteName,
                      gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    
    # START VALIDATION #
    # END VALIDATION #

    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # RESOURCE STATUS FUNCTIONS
  ##############################################################################
  '''  

#  @DBDec
#  def addOrModifyResourceStatus( self, resourceName, statusType, status, reason, 
#                                 dateCreated, dateEffective, dateEnd, lastCheckTime, 
#                                 tokenOwner,tokenExpiration, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( resourceName )
#    self.rsVal.validateSingleElementStatusType( 'Resource', rDict[ 'StatusType' ] )
#    # END VALIDATION #
#       
#    return self._addOrModifyElementStatus( rDict, **kwargs )    
  
  __TABLES__[ 'ResourceStatus' ] = {'uniqueKeys' : [ 'ResourceName', 'StatusType' ] }
  
  @DBDec
  def insertResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( resourceName )
    #self.rsVal.validateSingleElementStatusType( 'Resource', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    #return self._addOrModifyElementStatus( rDict, **kwargs )      
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
             
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
             
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteResourceStatus( self, resourceName, statusType, status, reason, 
                            dateCreated, dateEffective, dateEnd, lastCheckTime, 
                            tokenOwner,tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
             
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # RESOURCE SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  

#  @DBDec
#  def addOrModifyResourceScheduledStatus( self, resourceName, statusType, status, 
#                                          reason, dateCreated, dateEffective, dateEnd, 
#                                          lastCheckTime, tokenOwner, tokenExpiration,
#                                          **kwargs ):
#
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( resourceName )
#    self.rsVal.validateSingleElementStatusType( 'Resource', rDict[ 'StatusType' ] )
#    # END VALIDATION #
#    
#    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )
  
  __TABLES__[ 'ResourceScheduledStatus' ] = {'uniqueKeys' : [ 'ResourceName', 'StatusType', 'DateEffective' ] }
  
  @DBDec
  def insertResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( resourceName )
    #self.rsVal.validateSingleElementStatusType( 'Resource', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    #return self._addOrModifyElementScheduledStatus( rDict, **kwargs )  
  
    return self.mm.insert( rDict, **kwargs )
  
  @DBDec
  def updateResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
       
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getResourceScheduledStatus( self, resourceName, statusType, status, 
                                  reason, dateCreated, dateEffective, dateEnd, 
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
       
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteResourceScheduledStatus( self, resourceName, statusType, status, 
                                     reason, dateCreated, dateEffective, dateEnd, 
                                     lastCheckTime, tokenOwner, tokenExpiration,
                                     **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
       
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # RESOURCE HISTORY FUNCTIONS
  ##############################################################################
  '''  

#  @DBDec
#  def getResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
#                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
#                          tokenExpiration, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    if not kwargs.has_key( 'sort' ):
#      kwargs[ 'sort' ] = [ 'ResourceName', 'ResourceHistoryID' ]
#    return self.mm.get( rDict, **kwargs )
  
  __TABLES__[ 'ResourceHistory' ] = {'uniqueKeys' : [ 'ResourceName', 'StatusType', 'DateEnd' ] }
  
  @DBDec
  def insertResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.update( rDict, **kwargs )
  
  @DBDec
  def getResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                          dateEffective, dateEnd, lastCheckTime, tokenOwner,
                          tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'ResourceName', 'ResourceHistoryID' ]
    return self.mm.get( rDict, **kwargs )  

  @DBDec
  def deleteResourceHistory( self, resourceName, statusType, status, reason, dateCreated,
                             dateEffective, dateEnd, lastCheckTime, tokenOwner,
                             tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.delete( rDict, **kwargs )
  
  '''
  ##############################################################################
  # RESOURCE PRESENT FUNCTIONS
  ##############################################################################
  '''   
  
  __TABLES__[ 'ResourcePresent' ] = {'uniqueKeys' : [ 'ResourceName', 'StatusType' ] }


  @DBDec
  def getResourcePresent( self, resourceName, siteName, serviceType, gridSiteName,
                          siteType, resourceType, statusType, status, dateEffective,
                          reason, lastCheckTime, tokenOwner, tokenExpiration,
                          formerStatus, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

################################################################################
################################################################################
  
  '''
  ##############################################################################
  # STORAGE ELEMENT FUNCTIONS
  ##############################################################################
  '''

  __TABLES__[ 'StorageElement' ] = {'uniqueKeys' : [ 'StorageElementName' ] }
  
#  @DBDec
#  def addOrModifyStorageElement( self, storageElementName, resourceName,
#                                 gridSiteName, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    
#    # START VALIDATION #
#    self.rsVal.validateName( storageElementName )  
#    self.rsVal.validateResource( resourceName )
#    self.rsVal.validateGridSite( gridSiteName )
#    # END VALIDATION #
#
#    return self._addOrModifyElement( rDict, **kwargs )

  @DBDec
  def insertStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )
    
    # START VALIDATION #
    #self.rsVal.validateName( storageElementName )  
    #self.rsVal.validateResource( resourceName )
    #self.rsVal.validateGridSite( gridSiteName )
    # END VALIDATION #

    #return self._addOrModifyElement( rDict, **kwargs )
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # START VALIDATION #
    # END VALIDATION #

    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # START VALIDATION #
    # END VALIDATION #

    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElement( self, storageElementName, resourceName,
                            gridSiteName, **kwargs ):

    rDict = localsToDict( locals() )   
    # START VALIDATION #
    # END VALIDATION #

    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # STORAGE ELEMENT STATUS FUNCTIONS
  ##############################################################################
  '''

#  @DBDec
#  def addOrModifyStorageElementStatus( self, storageElementName, statusType, status,
#                                       reason, dateCreated, dateEffective, dateEnd,
#                                       lastCheckTime, tokenOwner, tokenExpiration,
#                                       **kwargs ):
#
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( storageElementName )
#    self.rsVal.validateSingleElementStatusType( 'StorageElement', rDict[ 'StatusType' ] )
#    # END VALIDATION #
#       
#    return self._addOrModifyElementStatus( rDict, **kwargs )  
  
  __TABLES__[ 'StorageElementStatus' ] = {'uniqueKeys' : [ 'StorageElementName', 'StatusType' ] }
  
  @DBDec
  def insertStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( storageElementName )
    #self.rsVal.validateSingleElementStatusType( 'StorageElement', rDict[ 'StatusType' ] )
    # END VALIDATION #
       
    #return self._addOrModifyElementStatus( rDict, **kwargs )    
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
           
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElementStatus( self, storageElementName, statusType, status,
                               reason, dateCreated, dateEffective, dateEnd,
                               lastCheckTime, tokenOwner, tokenExpiration,
                               **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
           
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElementStatus( self, storageElementName, statusType, status,
                                  reason, dateCreated, dateEffective, dateEnd,
                                  lastCheckTime, tokenOwner, tokenExpiration,
                                  **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
           
    return self.mm.delete( rDict, **kwargs )
 
  '''
  ##############################################################################
  # STORAGE ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  

#  @DBDec
#  def addOrModifyStorageElementScheduledStatus( self, storageElementName, statusType, status,
#                                                reason, dateCreated, dateEffective, dateEnd,
#                                                lastCheckTime, tokenOwner, tokenExpiration,
#                                                **kwargs ):
#
#    rDict = localsToDict( locals() )
#    # VALIDATION #
#    self.rsVal.validateName( storageElementName )
#    self.rsVal.validateSingleElementStatusType( 'StorageElement', rDict[ 'StatusType' ] )
#    # END VALIDATION #
#    
#    return self._addOrModifyElementScheduledStatus( rDict, **kwargs )
  
  __TABLES__[ 'StorageElementScheduledStatus' ] = {'uniqueKeys' : [ 'StorageElementName', 'StatusType', 'DateEffective' ] }
  
  @DBDec
  def insertStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    #self.rsVal.validateName( storageElementName )
    #self.rsVal.validateSingleElementStatusType( 'StorageElement', rDict[ 'StatusType' ] )
    # END VALIDATION #
    
    #return self._addOrModifyElementScheduledStatus( rDict, **kwargs )
    return self.mm.insert( rDict, **kwargs )  

  @DBDec
  def updateStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElementScheduledStatus( self, storageElementName, statusType, status,
                                           reason, dateCreated, dateEffective, dateEnd,
                                           lastCheckTime, tokenOwner, tokenExpiration,
                                           **kwargs ):

    rDict = localsToDict( locals() )
    # VALIDATION #
    # END VALIDATION #
    
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # STORAGE ELEMENT SCHEDULED STATUS FUNCTIONS
  ##############################################################################
  '''  

#  @DBDec
#  def getStorageElementHistory( self, storageElementName, statusType, status,
#                                 reason, dateCreated, dateEffective, dateEnd,
#                                 lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):
#
#    rDict = localsToDict( locals() )
#    if not kwargs.has_key( 'sort' ):
#      kwargs[ 'sort' ] = [ 'StorageElementName', 'StorageElementHistoryID' ]
#    return self.mm.get( rDict, **kwargs )

  __TABLES__[ 'StorageElementHistory'] = {'uniqueKeys' : [ 'StorageElementName', 'StatusType', 'DateEnd' ] }

  @DBDec
  def insertStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
    if not kwargs.has_key( 'sort' ):
      kwargs[ 'sort' ] = [ 'StorageElementName', 'StorageElementHistoryID' ]    
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteStorageElementHistory( self, storageElementName, statusType, status,
                                   reason, dateCreated, dateEffective, dateEnd,
                                   lastCheckTime, tokenOwner, tokenExpiration, **kwargs ):

    rDict = localsToDict( locals() )
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
    return self.mm.get( rDict, **kwargs )

################################################################################
################################################################################

  '''
  ##############################################################################
  # GRID SITE FUNCTIONS
  ##############################################################################
  '''

#  @DBDec
#  def addOrModifyGridSite( self, gridSiteName, gridTier, **kwargs ):
#
#    # VALIDATION #
#    self.rsVal.validateName( gridSiteName )
#    self.rsVal.validateGridSiteType( gridTier )
#    # END VALIDATION #
#
#    rDict    = localsToDict( locals() )
#    sqlQuery = self.mm.select( rDict, **kwargs )
#    
#    if sqlQuery[ 'Value' ]:      
#      return self.mm.update( rDict, **kwargs )
#    else: 
#      return self.mm.insert( rDict, **kwargs )  

  
  __TABLES__[ 'GridSite' ] = { 'uniqueKeys' : [ 'GridSiteName' ] } 

  @DBDec
  def insertGridSite( self, gridSiteName, gridTier, **kwargs ):

    # VALIDATION #
    #self.rsVal.validateName( gridSiteName )
    #self.rsVal.validateGridSiteType( gridTier )
    # END VALIDATION #

    rDict    = localsToDict( locals() )
    #sqlQuery = self.mm.select( rDict, **kwargs )
    
    #if sqlQuery[ 'Value' ]:      
    #  return self.mm.update( rDict, **kwargs )
    #else: 
    return self.mm.insert( rDict, **kwargs )  

  @DBDec
  def updateGridSite( self, gridSiteName, gridTier, **kwargs ):

    # VALIDATION #
    #self.rsVal.validateName( gridSiteName )
    #self.rsVal.validateGridSiteType( gridTier )
    # END VALIDATION #

    rDict    = localsToDict( locals() )
    #sqlQuery = self.mm.select( rDict, **kwargs )
    
    #if sqlQuery[ 'Value' ]:      
    return self.mm.update( rDict, **kwargs )
    #else: 
    #return self.mm.insert( rDict, **kwargs )  


  @DBDec
  def getGridSite( self, gridSiteName, gridTier, **kwargs ):

    rDict = localsToDict( locals() )
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteGridSite( self, gridSiteName, gridTier, **kwargs ):

    #VALIDATION#
    self.rsVal.validateMultipleNames( gridSiteName )
    # END VALIDATION #

    rDict = localsToDict( locals() )
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # STATS FUNCTIONS
  ##############################################################################
  '''

#  @DBDec
#  def getServiceStats( self, siteName, statusType ):
#    """
#    Returns simple statistics of active, probing, bad and banned services of a site;
#
#    :params:
#      :attr:`siteName`: string - a site name
#
#    :returns:
#      { 'Active':xx, 'Probing':yy, 'Bad':vv, 'Banned':zz, 'Total':xyz }
#    """
#
##    rDict = { 'SiteName' : siteName }
##
##    if statusType is not None:
##      self.__validateElementStatusTypes( 'Service', statusType )
##      rDict[ 'StatusType'] = statusType
#    rDict = localsToDict( locals() )
#    return self.__getElementStatusCount( 'Service', rDict )
#
#  @DBDec
#  def getResourceStats( self, element, name, statusType ):
#
#    rDict = {}
#
#    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( 'Service', statusType )
#      rDict[ 'StatusType'] = statusType
#
##    resourceDict = {}
#    resourceName, resourceType, serviceType, siteName, gridSiteName = None, None, None, None, None
#
#    if element == 'Site':
#      #name   = self.getGridSiteName( element, name )[ 'Value' ]
#      #rDict[ 'GridSiteName' ] = name
##      resourceDict = { 'siteName' : name }
#      siteName = name
#      #resourceNames = [ sn[0] for sn in self.getResources( siteName = name )[ 'Value' ] ]
###      rDict[ 'ResourceName' ] = resourceNames
#
#    elif element == 'Service':
#
#      serviceType = name.split( '@' )[ 0 ]
#      siteName    = name.split( '@' )[ 1 ]
#
#      if serviceType == 'Computing':
##        resourceDict = { 'siteName' : siteName }
#        siteName = siteName
#        #resourceName = [ sn[0] for sn in self.getResources( siteName = siteName )[ 'Value' ] ]
###        rDict[ 'ResourceName' ] = resourceNames
#        #rDict[ 'SiteName' ] = name
#      else:
#        #gridSiteName =
#        #rDict[ 'GridSiteName' ] = gridSiteName
##        resourceDict = { 'gridSiteName' : gridSiteName, 'serviceType' : serviceType }
#        kwargs = { 'columns' : [ 'GridSiteName' ] }
#        gridSiteName = [ gs[0] for gs in self.getSites( siteName, None, None, **kwargs )[ 'Value' ] ]
#        #gridSiteName = [ gs[0] for gs in self.getGridSiteName( 'Site', siteName )[ 'Value' ] ]
#        #serviceType  = serviceType
#        siteName = None
#        #resourceName = [ sn[0] for sn in self.getResources( None, None, serviceType, None,gridSiteName )[ 'Value' ] ]
#        #rDict[ 'SiteName' ] = siteNames
###        rDict[ 'ResourceName' ] = resourceNames
#        #rDict[ 'ServiceType' ]  = serviceType
#
#    else:
#      message = '%s is non accepted element. Only Site or Service' % element
#      return S_ERROR( message )
##      raise RSSDBException, where( self, self.getResourceStats ) + message
#
#    resourceArgs = ( resourceName, resourceType, serviceType, siteName, gridSiteName )
#    rDict[ 'ResourceName' ] = [ re[0] for re in self.getResources( *resourceArgs )[ 'Value' ] ]
#
#    return self.__getElementStatusCount( 'Resource', rDict )
#
#  @DBDec
#  def getStorageElementStats( self, element, name, statusType ):
#
#    rDict = {}
#
#    if statusType is not None:
#      self.rsVal.validateElementStatusTypes( 'StorageElement', statusType )
#      rDict[ 'StatusType'] = statusType
#
#    storageElementName, resourceName, gridSiteName = None, None, None
#
#    if element == 'Site':
#      #rDict[ 'GridSiteName' ] = self.getGridSiteName( element, name )[ 'Value' ]
#      kwargs = { 'columns' : [ 'GridSiteName' ] }
#      gridSiteName = [ gs[0] for gs in self.getSites( name, None, None, **kwargs )[ 'Value' ] ]
#      #gridSiteName = [ gs[0] for gs in self.getGridSiteName( element, name )[ 'Value' ] ]
##      seDict = { 'gridSiteName' : gridSiteName }
#      ##siteNames = [ sn[0] for sn in self.getSites( gridSiteName = gridSiteName )[ 'Value' ] ]
#      ##rDict[ 'SiteName' ] = siteNames
#      #seNames = [ sn[0] for sn in self.getStorageElements( gridSiteName = gridSiteName )[ 'Value' ] ]
##      rDict[ 'StorageElementName' ] = seNames
#
#    elif element == 'Resource':
#      #rDict[ 'ResourceName' ] = name
##      seDict = { 'resourceName' : name }
#      resourceName = name
#      #seNames = [ sn[0] for sn in self.getStorageElements( resourceName = name )[ 'Value' ] ]
##      rDict[ 'StorageElementName' ] = seNames
#
#    else:
#      message = '%s is non accepted element. Only Site or Resource' % element
#      return S_ERROR( message )
#
#    seArgs = ( storageElementName, resourceName, gridSiteName )
#    rDict[ 'StorageElementName' ] = [ se[0] for se in self.getStorageElements( *seArgs )[ 'Value' ] ]
#
#    return self.__getElementStatusCount( 'StorageElement', rDict )




  '''
  ##############################################################################
  # MISC FUNCTIONS
  ##############################################################################
  '''

  # Check the booster ResourceStatusSystem.Utilities.ResourceStatusBooster
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
