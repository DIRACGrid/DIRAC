################################################################################
# $HeadURL $
################################################################################
__RCSID__  = "$Id$"

"""
  ResourceStatusBooster class comprises methods that are horrible or not popular
  enough to be added to a generic API, but still quite convenient. 

  Note that all interaction is done though the Client with its generic API !!
"""

from datetime import datetime

from DIRAC.ResourceStatusSystem.Utilities.Decorators import CheckExecution2
from DIRAC.ResourceStatusSystem.Utilities.Validator  import ResourceStatusValidator

class ResourceManagementBooster( object ):
  
  def __init__( self, rsClient ):
    self.rsClient   = rsClient
    self.rsVal      = ResourceStatusValidator( rsGate = rsClient ) 

################################################################################

  @CheckExecution2
  def insertElement( self, element, *args ):
    
    fname = 'insert%s' % element
    f = getattr( self.rsClient, fname )
    return f( *args )

  @CheckExecution2
  def updateElement( self, element, *args ):
    
    fname = 'update%s' % element
    f = getattr( self.rsClient, fname )
    return f( *args )

  @CheckExecution2
  def getElement( self, element, *args, **kwargs ):
    
    fname = 'get%s' % element
    f = getattr( self.rsClient, fname )
    return f( *args, **kwargs )

  @CheckExecution2
  def deleteElement( self, element, *args, **kwargs ):
    
    fname = 'delete%s' % element
    f = getattr( self.rsClient, fname )
    return f( *args, **kwargs )

  def _addOrModifyElement( self, element, *args ):
       
    kwargs = { 'onlyUniqueKeys' : True }
    
    sqlQuery = self.getElement( element, *args, **kwargs )     
    
    if sqlQuery[ 'Value' ]:      
      if element in [ 'PolicyResult', 'ClientCache', 'AccountingCache' ]:
        args = list( args )
        #Force lastCheckTime to now if not set
        if args[ -1 ] is None:
          args[ -1 ] = datetime.utcnow().replace( microsecond = 0 )
        args = tuple( args )
      
      return self.updateElement( element, *args )
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
      
      return self.insertElement( element, *args )  

################################################################################    

  @CheckExecution2
  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment ):
    return self._addOrModifyElement( 'EnvironmentCache', hashEnv, siteName, environment )
  
  @CheckExecution2
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime ):
    return self._addOrModifyElement( 'PolicyResult', granularity, name, policyName, 
                                     statusType, status, reason, dateEffective, 
                                     lastCheckTime )

  @CheckExecution2
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime ):
    return self._addOrModifyElement( 'ClientCache', name, commandName, opt_ID, 
                                     value, result, dateEffective, lastCheckTime )
    
  @CheckExecution2
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, dateEffective,
                                  lastCheckTime ):
    return self._addOrModifyElement( 'AccountingCache', name, plotType, plotName, 
                                     result, dateEffective, lastCheckTime )
    
  @CheckExecution2
  def addOrModifyUserRegistryCache( self, login, name, email ):
    return self._addOrModifyElement( 'UserRegistryCache', login, name, email )   

################################################################################
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF        