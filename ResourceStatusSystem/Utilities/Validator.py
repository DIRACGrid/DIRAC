################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import ValidatorException

from DIRAC.ResourceStatusSystem import ValidRes, ValidStatus, ValidSiteType, \
    ValidResourceType, ValidServiceType, ValidStatusTypes

from DIRAC.ResourceStatusSystem.Utilities.Utils import where

class ResourceStatusValidator:

  def __init__( self, rsGate ):
    
    self.isDB = False
    
    if rsGate is None:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
      self.rsGate = ResourceStatusClient()
    else:
      self.rsGate = rsGate
      self.isDB = True  

################################################################################
## COMMON ######################################################################

  def validateString( self, s ):

    if not type( s ) == str:
      message = '%s is not a string' % s
      raise ValidatorException, where( self, self.validateString ) + message

  def validateMultipleString( self, ms, raiseNone = True ):

    if not raiseNone and ms is None:
      return

    if not isinstance( ms, list ):
      ms = [ ms ]
    for s in ms:
      self.validateString( s )

#  def validateMultipleNoneString( self, ms ):
#
#    if ms is None:
#      return
#        
#    if not isinstance( ms, list ):
#      ms = [ ms ]
#    for s in ms: 
#      self.validateString( s )

## COMMON ######################################################################    
## CS ##########################################################################    

  def validateElement( self, element, raiseNone = True ):

    if not raiseNone and element is None:
      return 

    self.validateString( element )
    if not element in ValidRes:
      message = '"%s" is not a valid element' % element
      raise ValidatorException, where( self, self.validateElement ) + message

  def validateSiteType( self, siteType, raiseNone = True ):

    if not raiseNone and siteType is None:
      return 

    self.validateString( siteType )
    if not siteType in ValidSiteType:
      message = '"%s" is not a valid site type' % siteType
      raise ValidatorException, where( self, self.validateSiteType ) + message

  def validateServiceType( self, serviceType, raiseNone = True ):

    if not raiseNone and serviceType is None:
      return 

    self.validateString( serviceType )
    if not serviceType in ValidServiceType:
      message = '"%s" is not a valid service type' % serviceType
      raise ValidatorException, where( self, self.validateServiceType ) + message

  def validateResourceType( self, resourceType, raiseNone = True ):

    if not raiseNone and resourceType is None:
      return 

    self.validateString( resourceType )
    if not resourceType in ValidResourceType:
      message = '"%s" is not a valid resource type' % resourceType
      raise ValidatorException, where( self, self.validateResourceType ) + message

  def validateStatusType( self, element, statusType, raiseNone = True ):

    if not raiseNone and statusType is None:
      return 

    self.validateString( statusType )
    if not statusType in ValidStatusTypes[ element ][ 'StatusType' ]:
      message = '"%s" is not a valid statusType for "%s"' % ( statusType, element )
      raise ValidatorException, where( self, self.validateStatusType ) + message      

  def validateStatus( self, status, raiseNone = True ):
    
    if not raiseNone and status is None:
      return 
    
    if not status in ValidStatus:              
      message = '"%s" is not a valid status' % status
      raise ValidatorException, where( self, self.validateStatus ) + message

## CS ##########################################################################
## ELEMENT #####################################################################

  def validateSite( self, siteName, raiseNone = True ):
    
    if not raiseNone and siteName is None:
      return 
        
    self.validateString( siteName )
    res = self.rsGate.getSite(siteName, None, None)
    
    if not res[ 'OK' ] or not res[ 'Value' ]:
      message = '"%s" is not a known siteName' % siteName
      raise ValidatorException, where( self, self.validateSite ) + message

  def validateService( self, serviceName, raiseNone = True ):
    
    if not raiseNone and serviceName is None:
      return 
        
    self.validateString( serviceName )
    res = self.rsGate.getService( serviceName, None, None )
    
    if not res[ 'OK' ] or not res[ 'Value' ]:
      message = '"%s" is not a known serviceName' % serviceName
      raise ValidatorException, where( self, self.validateService ) + message

  def validateResource( self, resourceName, raiseNone = True ):
    
    if not raiseNone and resourceName is None:
      return 
        
    self.validateString( resourceName )
    res = self.rsGate.getResource( resourceName, None, None )
    
    if not res[ 'OK' ] or not res[ 'Value' ]:
      message = '"%s" is not a known resourceName' % resourceName
      raise ValidatorException, where( self, self.validateResource ) + message

  def validateStorageElement( self, storageElementName, raiseNone = True ):
    
    if not raiseNone and storageElementName is None:
      return 
        
    self.validateString( storageElementName )
    res = self.rsGate.getStorageElementName( storageElementName, None, None )
    
    if not res[ 'OK' ] or not res[ 'Value' ]:
      message = '"%s" is not a known storageElementName' % storageElementName
      raise ValidatorException, where( self, self.validateStorageElementName ) + message

## ELEMENT #####################################################################
################################################################################
#  def validateName( self, name ):
#    
#    if not type( name ) == str:
#      message = '%s is not a valid name' % name
#      raise InvalidName, where( self, self.validateName ) + message
#
#  def validateMultipleNames( self, names ):
#    
#    if not isinstance( names, list ):
#      names = [ names ]
#    for name in names:
#      self.validateName( name )
#    

#
#  def validateRes( self, res ):
#    
#    if not res in ValidRes:              
#      message = '%s is not a valid res' % res
#      raise InvalidRes, where( self, self.validateRes ) + message
#
#  def validateGridSiteType( self, gridSiteType ):
#    
#    if not gridSiteType in ValidSiteType:
#      message = '%s is not a valid grid site type' % gridSiteType
#      raise InvalidGridSiteType, where( self, self.validateGridSiteType ) + message
#          
#  def validateServiceType( self, serviceType ):
#    
#    if not serviceType in ValidServiceType:
#      message = '%s is not a valid service type' % serviceType
#      raise InvalidServiceType, where( self, self.validateServiceType ) + message
#
#  def validateResourceType( self, resourceType ):
#    
#    if not resourceType in ValidResourceType:
#      message = '%s is not a valid resource type' % resourceType
#      raise InvalidResourceType, where( self, self.validateResourceType ) + message
#
#  def validateSingleElementStatusType( self, element, statusType ):
#
#    if not statusType in ValidStatusTypes[ element ][ 'StatusType' ]:
#      message = '%s is not a valid statusType for %s' % ( statusType, element )
#      raise InvalidStatus, where( self, self.validateSingleElementStatusType ) + message
#    
#  def validateElementStatusTypes( self, element, statusTypes ):
#    
#    if not isinstance( statusTypes, list ):
#      statusTypes = [ statusTypes ]
#    
#    for statusType in statusTypes: 
#      self.validateSingleElementStatusType( element, statusType )
#
#################################################################################
#
#  def validateSite( self, siteName ):
#    
#    if type( siteName ) != str:
#      message = '%s is not string, as expected' % siteName
#      raise InvalidFormat, where( self, self.validateSite ) + message
#    
#    if self.isDB:
#      res = self.rsGate.getSite( siteName, None, None )
#    else:
#      res = self.rsGate.getSite( siteName = siteName )
#
#    if not res[ 'OK' ] or not res[ 'Value' ]:
#      message = '%s is not a known siteName' % siteName
#      raise InvalidSite, where( self, self.validateSite ) + message
#
#  def validateResource( self, resourceName ):
#    
#    if type( resourceName ) != str:
#      message = '%s is not string, as expected' % resourceName
#      raise InvalidFormat, where( self, self.validateSite ) + message
#    
#    if self.isDB:    
#      res = self.rsGate.getResource( resourceName, None, None, None, None )
#      print res
#    else:
#      res = self.rsGate.getResource( resourceName = resourceName )
#      
#    if not res[ 'OK' ] or not res[ 'Value' ]:
#      message = '%s is not a known resourceName' % resourceName
#      raise InvalidResource, where( self, self.validateResource ) + message
#      
#  def validateGridSite( self, gridSiteName ):
#
#    if self.isDB:    
#      res = self.rsGate.getGridSite( gridSiteName, None )
#    else:
#      res = self.rsGate.getGridSite( gridSiteName = gridSiteName )
#        
#    if not res[ 'OK' ] or not res[ 'Value' ]:
#      message = '%s is not a known GridSiteName' % gridSiteName
#      raise InvalidGridSite, where( self, self.validateGridSite ) + message        
#      
#################################################################################
#
#  def __validateSingleDate( self, singleDate ):
#    
#    if type( singleDate ) != datetime:
#      message = '%s is not an accepted date' % singleDate
#      raise InvalidDate, where( self, self.__validateSingleDate ) + message 
#      
#  def validateSingleDates( self, rDict ):
#        
#    dateKeys = [ 'TokenExpiration', 'DateCreated', 'DateEffective', 'DateEnd', 'LastCheckTime' ]
#    for dateKey in dateKeys:
#      if rDict.has_key( dateKey ):
#        self.__validateSingleDate( rDict[ dateKey ])      
#
#  def validateDates( self, rDict ):
#    
#    dateKeys = [ 'TokenExpiration', 'DateCreated', 'DateEffective', 'DateEnd', 'LastCheckTime' ] 
#    
#    for dateKey in dateKeys:
#      if rDict.has_key( dateKey ):
#        if not isinstance( rDict[ dateKey ], list ):
#          rDict[ dateKey ] = [ rDict[ dateKey ] ]
#        
#        for singleDate in rDict[ dateKey ]:
#          self.__validateSingleDate( singleDate )

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF          