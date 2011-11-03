################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from datetime import datetime

from DIRAC.ResourceStatusSystem.API.private.ResourceManagementBaseAPI import \
  ResourceManagementBaseAPI

class ResourceManagementExtendedBaseAPI( ResourceManagementBaseAPI ):
  
  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    args = ( hashEnv, siteName, environment )
    return self.__addOrModifyElement( 'EnvironmentCache', *args )
  
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    args = ( granularity, name, policyName, statusType, status, reason, 
             dateEffective, lastCheckTime) 
    return self.__addOrModifyElement( 'PolicyResult', *args )

  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    args = ( name, commandName, opt_ID, value, result, dateEffective, 
             lastCheckTime )
    return self.__addOrModifyElement( 'ClientCache', *args )
    
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, 
                                  dateEffective, lastCheckTime ):
    args = ( name, plotType, plotName, result, dateEffective, lastCheckTime )
    return self.__addOrModifyElement( 'AccountingCache', *args )
    
  def addOrModifyUserRegistryCache( self, login, name, email ):
    args = ( login, name, email )
    return self.__addOrModifyElement( 'UserRegistryCache', *args )  
    
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
      if element in [ 'PolicyResult', 'ClientCache', 'AccountingCache' ]:
        args = list( args )
        #Force lastCheckTime to now if not set
        if args[ -1 ] is None:
          args[ -1 ] = datetime.utcnow().replace( microsecond = 0 )
        args = tuple( args )
      
      return self.__updateElement( element, *args )
    else: 
      if element in [ 'PolicyResult', 'ClientCache', 'AccountingCache' ]:
        args = list( args )
        #Force dateEffective to now if not set
        if args[ -2 ] is None:
          args[ -2 ] = datetime.utcnow().replace( microsecond = 0 )
        #Force lastCheckTime to now if not set
        if args[ -1 ] is None:
          args[ -1 ] = datetime.utcnow().replace( microsecond = 0 )
        args = tuple( args )
      
      return self.__insertElement( element, *args ) 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF     