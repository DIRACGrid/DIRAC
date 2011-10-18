__RCSID__ = "$Id:  $"

from DIRAC.ResourceStatusSystem.Utilities.Decorators  import DBDec
from DIRAC.ResourceStatusSystem.Utilities.MySQLMonkey import MySQLMonkey, localsToDict

################################################################################

class ResourceManagementDB(object):
  """
  The ResourceManagementDB class is a front-end to the ResourceManagementDB MySQL db.
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

  The simplest way to instantiate an object of type :class:`ResourceManagementDB`
  is simply by calling

   >>> rmDB = ResourceManagementDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`.
  But there's the possibility to use other DB classes.
  For example, we could pass custom DB instantiations to it,
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rmDB = ResourceManagementDB( DBin = AnotherDB )

  Alternatively, for testing purposes, you could do:

   >>> from mock import Mock
   >>> mockDB = Mock()
   >>> rmDB = ResourceManagementDB( DBin = mockDB )

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rmDB = ResourceManagementDB( DBin = [ 'UserName', 'Password' ] )
   
  If you want to know more about ResourceManagementDB, scroll down to the end of
  the file. 
  """
  
  # This is an small & temporary 'hack' used for the MySQLMonkey.
  # Check MySQL monkey for more info
  # Now is hard-coded for simplicity, eventually will be calculated automatically
  __TABLES__ = {}

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

################################################################################
################################################################################

  '''
  ##############################################################################
  # ENVIRONMENT CACHE FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'EnvironmentCache' ] = { 'uniqueKeys' : [ 'HashEnv', 'SiteName' ] }

  @DBDec
  def insertEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    
    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.insert( rDict, **kwargs )    

  @DBDec
  def updateEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    
    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.update( rDict, **kwargs )    

  @DBDec
  def getEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):
    
    rDict  = localsToDict( locals() )
    # NO VALIDATION #  
    return self.mm.get( rDict, **kwargs )

  @DBDec    
  def deleteEnvironmentCache( self, hashEnv, siteName, environment, **kwargs ):

    rDict  = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # POLICY RESULT FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'PolicyResult' ] = { 'uniqueKeys' : [ 'Name', 'StatusType', 
                                                    'PolicyName' ] }

  @DBDec
  def insertPolicyResult( self, granularity, name, policyName, statusType, status, 
                          reason, dateEffective, lastCheckTime, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.insert( rDict, **kwargs )   

  @DBDec
  def updatePolicyResult( self, granularity, name, policyName, statusType, status, 
                          reason, dateEffective, lastCheckTime, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.update( rDict, **kwargs ) 
  
  @DBDec      
  def getPolicyResult( self, granularity, name, policyName, statusType, status, 
                       reason, dateEffective, lastCheckTime, **kwargs ):

    rDict  = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deletePolicyResult( self, granularity, name, policyName, statusType, status, 
                           reason, dateEffective, lastCheckTime, **kwargs ):

    rDict  = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # CLIENT CACHE FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'ClientCache' ] = { 'uniqueKeys' : [ 'Name', 'CommandName', 'Value' ] }
  
  @DBDec
  def insertClientCache( self, name, commandName, opt_ID, value, result, dateEffective, 
                         lastCheckTime, **kwargs ):
    
    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.insert( rDict, **kwargs )  

  @DBDec
  def updateClientCache( self, name, commandName, opt_ID, value, result, 
                         dateEffective, lastCheckTime, **kwargs ):
    
    rDict = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.update( rDict, **kwargs )  

  @DBDec    
  def getClientCache( self, name, commandName, opt_ID, value, result,
                      dateEffective, lastCheckTime, **kwargs ):  
    
    rDict  = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.get( rDict, **kwargs )

  @DBDec  
  def deleteClientCache( self, name, commandName, opt_ID, value, result,
                         dateEffective, lastCheckTime, **kwargs ):
    
    rDict  = localsToDict( locals() )    
    # NO VALIDATION #
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # ACCOUNTING CACHE FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'AccountingCache' ] = {'uniqueKeys' : [ 'Name', 'PlotType', 'PlotName' ] }

  @DBDec
  def insertAccountingCache( self, name, plotType, plotName, result, dateEffective,
                             lastCheckTime, **kwargs ):

    rDict  = localsToDict( locals() )
    # NO VALIDATION #
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateAccountingCache( self, name, plotType, plotName, result, dateEffective,
                             lastCheckTime, **kwargs ):

    rDict  = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.update( rDict, **kwargs )

  @DBDec
  def getAccountingCache( self, name, plotType, plotName, result, dateEffective,
                          lastCheckTime, **kwargs ):
    
    rDict  = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteAccountingCache( self, name, plotType, plotName, result, dateEffective,
                             lastCheckTime, **kwargs ):

    rDict  = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.delete( rDict, **kwargs )

  '''
  ##############################################################################
  # USER REGISTRY CACHE FUNCTIONS
  ##############################################################################
  '''
  __TABLES__[ 'UserRegistryCache' ] =  { 'uniqueKeys' : [ 'Login' ] }  

  @DBDec
  def insertUserRegistryCache( self, login, name, email, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.insert( rDict, **kwargs )

  @DBDec
  def updateUserRegistryCache( self, login, name, email, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.update( rDict, **kwargs )
  
  @DBDec
  def getUserRegistryCache( self, login, name, email, **kwargs ):
    
    rDict = localsToDict( locals() )
    # NO VALIDATION #       
    return self.mm.get( rDict, **kwargs )

  @DBDec
  def deleteUserRegistryCache( self, login, name, email, **kwargs ):

    rDict = localsToDict( locals() )
    # NO VALIDATION #    
    return self.mm.delete( rDict, **kwargs )
  
################################################################################

'''
  HOW DOES THIS WORK.
    
    will come soon...
'''     

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