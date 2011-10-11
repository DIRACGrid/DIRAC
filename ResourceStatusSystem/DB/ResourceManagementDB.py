"""
The ResourcesManagementDB module contains a couple of exception classes, and a
class to interact with the ResourceManagement DB.
"""

#import datetime

#from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

from DIRAC.ResourceStatusSystem.Utilities.MySQLMonkey import MySQLMonkey

from DIRAC.ResourceStatusSystem.Utilities.Decorators import CheckExecution

################################################################################

class RSSManagementDBException(RSSException):
  """
  DB exception
  """

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Exception in the RSS Management DB: " + repr(self.message)

################################################################################

class ResourceManagementDB(object):
  """
  The ResourceManagementDB class is a front-end to the Resource Management Database.

  The simplest way to instantiate an object of type :class:`ResourceManagementDB`
  is simply by calling

   >>> rmDB = ResourceManagementDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`.
  But there's the possibility to use other DB classes.
  For example, we could pass custom DB instantiations to it,
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rmDB = ResourceManagementDB(DBin = AnotherDB)

  Alternatively, for testing purposes, you could do:

   >>> from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
   >>> mockDB = Mock()
   >>> rmDB = ResourceManagementDB(DBin = mockDB)

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rmDB = ResourceManagementDB(DBin = ['UserName', 'Password'])

  """
  
  TABLES = {
            'EnvironmentCache'  : {
              'uniqueKeys' : [ 'HashEnv', 'SiteName' ]                      
                                  },
            'PolicyResult'      : {
              'uniqueKeys' : [ 'Name', 'StatusType', 'PolicyName' ]                    
                                  },
            'ClientCache'       : {
              'uniqueKeys' : [ 'Name', 'CommandName', 'Value' ]                    
                                  },
            'AccountingCache'   : {
              'uniqueKeys' : [ 'Name', 'PlotType', 'PlotName' ]                    
                                  },            
            'UserRegistryCache' : {
              'uniqueKeys' : [ 'Login' ]                     
                                   }           
            }

  def __init__( self, *args, **kwargs ):

    if len(args) == 1:
      if isinstance(args[0], str):
        maxQueueSize=10
      if isinstance(args[0], int):
        maxQueueSize=args[0]
    elif len(args) == 2:
      maxQueueSize=args[1]
    elif len(args) == 0:
      maxQueueSize=10

    if 'DBin' in kwargs.keys():
      DBin = kwargs[ 'DBin' ]
      if isinstance(DBin, list):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL( 'localhost', DBin[0], DBin[1], 'ResourceManagementDB' )
      else:
        self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB( 'ResourceManagementDB', 'ResourceStatus/ResourceManagementDB', maxQueueSize )

    self.mm    = MySQLMonkey( self )  
#    self.rsVal = ResourceStatusValidator( self )


################################################################################

################################################################################
# EnvironmentCache functions
################################################################################

################################################################################

  @CheckExecution
  def addOrModifyEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    
    ## 
    rDict  = self.mm.localsToDict( locals() )
    ##        
            
    sqlQuery = self.mm.select( rDict, **kwargs )
    
    if sqlQuery[ 'Value' ]:      
      return self.mm.update( rDict, **kwargs )
    else: 
      return self.mm.insert( rDict, **kwargs )  

  @CheckExecution
  def getEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.get( rDict, **kwargs )

  @CheckExecution    
  def deleteEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):

    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.delete( rDict, **kwargs )

################################################################################

################################################################################
# PolicyRes functions
################################################################################

################################################################################

  @CheckExecution
  def addOrModifyPolicyResult( self, granularity, name, policyName, statusType,
                               status, reason, dateEffective, lastCheckTime, **kwargs ):

    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    sqlQuery = self.mm.select( rDict, **kwargs )

    if sqlQuery[ 'Value' ]:      
      return self.mm.update( rDict, **kwargs )
    else: 
      return self.mm.insert( rDict, **kwargs )
  
  @CheckExecution      
  def getPolicyResult( self, granularity, name, policyName, statusType, status, 
                        reason, dateEffective, lastCheckTime, **kwargs ):

    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deletePolicyResult( self, granularity, name, policyName, statusType, status, 
                           reason, dateEffective, lastCheckTime, **kwargs ):

    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.delete( rDict, **kwargs )

################################################################################

################################################################################
# ClientsCache functions
################################################################################

################################################################################

  @CheckExecution
  def addOrModifyClientCache( self, name, commandName, opt_ID, value, result,
                              dateEffective, lastCheckTime, **kwargs ):
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    sqlQuery = self.mm.select( rDict, **kwargs )

    if sqlQuery[ 'Value' ]:      
      return self.mm.update( rDict, **kwargs )
    else: 
      return self.mm.insert( rDict, **kwargs )

  @CheckExecution    
  def getClientCache( self, name, commandName, opt_ID, value, result,
                       dateEffective, lastCheckTime, **kwargs ):  
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.get( rDict, **kwargs )

  @CheckExecution  
  def deleteClientCache( self, name, commandName, opt_ID, value, result,
                          dateEffective, lastCheckTime, **kwargs ):
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.delete( rDict, **kwargs )

################################################################################

################################################################################
# AccountingCache functions
################################################################################

################################################################################

  @CheckExecution
  def addOrModifyAccountingCache( self, name, plotType, plotName, result, dateEffective,
                                  lastCheckTime, **kwargs ):
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    sqlQuery = self.mm.select( rDict, **kwargs )

    if sqlQuery[ 'Value' ]:      
      return self.mm.update( rDict, **kwargs )
    else: 
      return self.mm.insert( rDict, **kwargs )

  @CheckExecution
  def getAccountingCache( self, name, plotType, plotName, result, dateEffective,
                           lastCheckTime, **kwargs ):
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deleteAccountingCache( self, name, plotType, plotName, result, dateEffective,
                              lastCheckTime, **kwargs ):

    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.delete( rDict, **kwargs )

################################################################################

################################################################################
# UserRegistryCache functions
################################################################################

################################################################################

  @CheckExecution
  def addOrModifyUserRegistryCache( self, login, name, email, **kwargs ):
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    sqlQuery = self.mm.select( rDict, **kwargs )

    if sqlQuery[ 'Value' ]:      
      return self.mm.update( rDict, **kwargs )
    else: 
      return self.mm.insert( rDict, **kwargs )

  @CheckExecution
  def getUserRegistryCache( self, login, name, email, **kwargs ):
    
    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.get( rDict, **kwargs )

  @CheckExecution
  def deleteUserRegistryCache( self, login, name, email, **kwargs ):

    ##
    rDict  = self.mm.localsToDict( locals() )
    ##
    
    return self.mm.delete( rDict, **kwargs )


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF

'''
This goes to the booster

################################################################################
## Web functions
################################################################################
#
################################################################################
#
#  def getDownTimesWeb(self, selectDict, _sortList = [], startItem = 0, maxItems = 1000):
#    """
#    Get downtimes registered in the RSS DB (with a web layout)
#
#    :params:
#      :attr:`selectDict`: { 'Granularity':['Site', 'Resource'], 'Severity': ['OUTAGE', 'AT_RISK']}
#
#      :attr:`sortList`
#
#      :attr:`startItem`
#
#      :attr:`maxItems`
#    """
#
#    granularity = selectDict['Granularity']
#    severity = selectDict['Severity']
#
#    if not isinstance(granularity, list):
#      granularity = [granularity]
#    if not isinstance(severity, list):
#      severity = [severity]
#
#    paramNames = ['Granularity', 'Name', 'Severity', 'When']
#
#    req = "SELECT Granularity, Name, Reason FROM PolicyRes WHERE "
#    req = req + "PolicyName LIKE 'DT_%' AND Reason LIKE \'%found%\' "
#    req = req + "AND Granularity in (%s)" %(','.join(['"'+x.strip()+'"' for x in granularity]))
#    resQuery = self.db._query(req)
#    if not resQuery['OK']:
#      raise RSSManagementDBException, resQuery['Message']
#    if not resQuery['Value']:
#      records = []
#    else:
#      resQuery = resQuery['Value']
#      records = []
#      for tuple_ in resQuery:
#        sev = tuple_[2].split()[2]
#        if sev not in severity:
#          continue
#        when = tuple_[2].split(sev)[1][1:]
#        if when == '':
#          when = 'Ongoing'
#        records.append([tuple_[0], tuple_[1], sev, when])
#
#    finalDict = {}
#    finalDict['TotalRecords'] = len(records)
#    finalDict['ParameterNames'] = paramNames
#
#    # Return all the records if maxItems == 0 or the specified number otherwise
#    if maxItems:
#      finalDict['Records'] = records[startItem:startItem+maxItems]
#    else:
#      finalDict['Records'] = records
#
#    finalDict['Extras'] = None
#
#    return finalDict
#
################################################################################
'''