#from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidRes, InvalidGridSite
#import DIRAC.ResourceStatusSystem.Utilities.Exceptions as RSSExceptions
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import InvalidName, InvalidStatus, InvalidRes, \
  InvalidGridSiteType, InvalidSiteType,InvalidSiteType,InvalidServiceType,\
  InvalidResourceType, InvalidStatus, InvalidFormat, InvalidSite, InvalidGridSite, InvalidDate

from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidSiteType, \
    ValidResourceType, ValidServiceType, ValidStatusTypes

from DIRAC.ResourceStatusSystem.Utilities.Utils import where

from datetime import datetime

class ResourceStatusValidator:

  def __init__( self, rsGate = None ):
    
    if rsGate is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsGate = ResourceStatusClient()
    else:
      self.rsGate = rsGate  

  def validateName( self, name ):
    
    if not type( name ) == str:
      message = '%s is not a valid name' % name
      raise InvalidName, where( self, self.validateName ) + message

  def validateMultipleNames( self, names ):
    
    if not isinstance( names, list ):
      names = [ names ]
    for name in names:
      self.validateName( name )
    
  def validateStatus( self, status ):
    
    if not status in ValidStatus:              
      message = '%s is not a valid status' % status
      raise InvalidStatus, where( self, self.validateStatus ) + message

  def validateRes( self, res ):
    
    if not res in ValidRes:              
      message = '%s is not a valid res' % res
      raise InvalidRes, where( self, self.validateRes ) + message

  def validateSiteType( self, siteType ):
    
    if not siteType in ValidSiteType:
      message = '%s is not a valid site type' % siteType
      raise InvalidSiteType, where( self, self.validateSiteType ) + message

  def validateGridSiteType( self, gridSiteType ):
    
    if not gridSiteType in ValidSiteType:
      message = '%s is not a valid grid site type' % gridSiteType
      raise InvalidGridSiteType, where( self, self.validateGridSiteType ) + message
          
  def validateServiceType( self, serviceType ):
    
    if not serviceType in ValidServiceType:
      message = '%s is not a valid service type' % serviceType
      raise InvalidServiceType, where( self, self.validateServiceType ) + message

  def validateResourceType( self, resourceType ):
    
    if not resourceType in ValidResourceType:
      message = '%s is not a valid resource type' % resourceType
      raise InvalidResourceType, where( self, self.validateResourceType ) + message

  def validateSingleElementStatusType( self, element, statusType ):

    if not statusType in ValidStatusTypes[ element ][ 'StatusType' ]:
      message = '%s is not a valid statusType for %s' % ( statusType, element )
      raise InvalidStatus, where( self, self.validateSingleElementStatusType ) + message
    
  def validateElementStatusTypes( self, element, statusTypes ):
    
    if not isinstance( statusTypes, list ):
      statusTypes = [ statusTypes ]
    
    for statusType in statusTypes: 
      self.validateSingleElementStatusType( element, statusType )

################################################################################

  def validateSite( self, siteName ):
    
    if type( siteName ) != str:
      message = '%s is not string, as expected' % siteName
      raise InvalidFormat, where( self, self.validateSite ) + message
    
    res = self.rsGate.getSites( siteName = siteName )
    if not res[ 'OK' ] or not res[ 'Value' ]:
      message = '%s is not a known siteName' % siteName
      raise InvalidSite, where( self, self.validateSite ) + message

  def validateResource( self, resourceName ):
    
    if type( resourceName ) != str:
      message = '%s is not string, as expected' % resourceName
      raise InvalidFormat, where( self, self.validateSite ) + message
    
    res = self.rsGate.getResources( resourceName = resourceName )
    if not res[ 'OK' ] or not res[ 'Value' ]:
      message = '%s is not a known resourceName' % resourceName
      raise InvalidResource, where( self, self.validateSite ) + message
      
  def validateGridSite( self, gridSiteName ):
    
    res = self.rsGate.getGridSites( gridSiteName, None )
    if not res[ 'OK' ] or not res[ 'Value' ]:
      message = '%s is not a known GridSiteName' % gridSiteName
      raise InvalidGridSite, where( self, self.validateGridSite ) + message        
      
################################################################################

  def __validateSingleDate( self, singleDate ):
    
    if type( singleDate ) != datetime:
      message = '%s is not an accepted date' % singleDate
      raise InvalidDate, where( self, self.__validateSingleDate ) + message 
      
  def validateSingleDates( self, rDict ):
        
    dateKeys = [ 'TokenExpiration', 'DateCreated', 'DateEffective', 'DateEnd', 'LastCheckTime' ]
    for dateKey in dateKeys:
      if rDict.has_key( dateKey ):
        self.__validateSingleDate( rDict[ dateKey ])      

  def validateDates( self, rDict ):
    
    dateKeys = [ 'TokenExpiration', 'DateCreated', 'DateEffective', 'DateEnd', 'LastCheckTime' ] 
    
    for dateKey in dateKeys:
      if rDict.has_key( dateKey ):
        if not isinstance( rDict[ dateKey ], list ):
          rDict[ dateKey ] = [ rDict[ dateKey ] ]
        
        for singleDate in rDict[ dateKey ]:
          self.__validateSingleDate( singleDate )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF          