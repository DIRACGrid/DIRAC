"""
The ResourcesManagementDB module contains a couple of exception classes, and a
class to interact with the ResourceManagement DB.
"""

import datetime
#from types import *

#from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping import *

from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

#############################################################################

class RSSManagementDBException(RSSException):
  """
  DB exception
  """

  def __init__(self, message = ""):
    self.message = message
    RSSException.__init__(self, message)

  def __str__(self):
    return "Exception in the RSS Management DB: " + repr(self.message)

#############################################################################

class ResourceManagementDB:
  """
  The ResourceManagementDB class is a front-end to the Resource Management Database.

  The simplest way to instantiate an object of type :class:`ResourceManagementDB`
  is simply by calling

   >>> rpDB = ResourceManagementDB()

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


  def __init__(self, *args, **kwargs):

    if len(args) == 1:
      if isinstance(args[0], str):
#        systemInstance=args[0]
        maxQueueSize=10
      if isinstance(args[0], int):
        maxQueueSize=args[0]
#        systemInstance='Default'
    elif len(args) == 2:
#      systemInstance=args[0]
      maxQueueSize=args[1]
    elif len(args) == 0:
#      systemInstance='Default'
      maxQueueSize=10

    if 'DBin' in kwargs.keys():
      DBin = kwargs['DBin']
      if isinstance(DBin, list):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL('localhost', DBin[0], DBin[1], 'ResourceManagementDB')
      else:
        self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB('ResourceManagementDB','ResourceStatus/ResourceManagementDB',maxQueueSize)
#      self.db = DB('ResourceStatusDB','ResourceStatus/ResourceStatusDB',maxQueueSize)


#############################################################################

#############################################################################
# Policy functions
#############################################################################

#############################################################################

  def addOrModifyEnvironmentCache( self, hash_, siteName, environment ):

    req  = "SELECT SiteName, Hash FROM EnvironmentCache "
    req += "WHERE SiteName = '%s' AND Hash = '%s'" % ( siteName, hash_ )
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.addOrModifyEnvironmentCache) + resQuery['Message']

    now = datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' ')

    if resQuery['Value']:
      req  = "UPDATE EnvironmentCache SET "
      req += "Environment = '%s', DateEffective = '%s' " % ( environment, now )
      req += "WHERE SiteName = '%s' AND Hash = '%s'" % ( siteName, hash_ )

      resUpdate = self.db._update(req)
      if not resUpdate['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyEnvironmentCache) + resUpdate['Message']
    else:
      req  = "INSERT INTO EnvironmentCache ( SiteName, Hash, DateEffective, Environment ) "
      req += "VALUES ( '%s', '%s', '%s', '%s' )" %( siteName, hash_, now, environment )

      resInsert = self.db._update(req)
      if not resInsert['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyEnvironmentCache) + resInsert['Message']

#############################################################################

  def getEnvironmentCache( self, hash_, siteName ):

    req  = "SELECT DateEffective, Environment "
    req += "FROM EnvironmentCache WHERE "
    req += "Hash = '%s' AND SiteName = '%s'"  % ( hash_, siteName )

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.getEnvironmentCache) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return resQuery['Value'][0]

#############################################################################
# Policy functions
#############################################################################

#############################################################################

  def addOrModifyPolicyRes(self, granularity, name, policyName,
                           status, reason, dateEffective = None):
    """
    Add or modify a Policy Result to the PolicyRes table.

    :params:
      :attr:`granularity`: string - a ValidRes
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`name`: string - name of the ValidRes

      :attr:`policyName`: string - the policy name

      :attr:`status`: string - a ValidStatus:
      see :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils`

      :attr:`reason`: string - free

      :attr:`dateEffective`: datetime.datetime -
      date from which the result is effective
    """

    now = datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' ')

    if dateEffective is None:
      dateEffective = now

    req = "SELECT Granularity, Name, PolicyName, Status, Reason FROM PolicyRes "
    req = req + "WHERE Granularity = '%s' AND Name = '%s' AND " %(granularity, name)
    req = req + "PolicyName = '%s'" %(policyName)

    resQuery = self.db._query(req)

    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.addOrModifyPolicyRes) + resQuery['Message']

    if resQuery['Value']:
      req = "UPDATE PolicyRes SET "
      if resQuery['Value'][0][3] != status:
        req = req + "Status = '%s', Reason = '%s', DateEffective = '%s', " %(status, reason, dateEffective)
      elif resQuery['Value'][0][4] != reason:
        req = req + "Reason = '%s', " %(reason)
      req = req + "LastCheckTime = '%s' WHERE Granularity = '%s' " %(now, granularity)
      req = req + "AND Name = '%s' AND PolicyName = '%s'" %(name, policyName)

      resUpdate = self.db._update(req)

      if not resUpdate['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyPolicyRes) + resUpdate['Message']
    else:
      req = "INSERT INTO PolicyRes (Granularity, Name, PolicyName, Status, Reason, DateEffective, "
      req = req + "LastCheckTime) VALUES ('%s', '%s', '%s', " %(granularity, name, policyName)
      req = req + "'%s', '%s', '%s', '%s')" %(status, reason, dateEffective, now)

      resUpdate = self.db._update(req)

      if not resUpdate['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyPolicyRes) + resUpdate['Message']

#############################################################################

  def getPolicyRes(self, name, policyName, lastCheckTime = False):
    """
    Get a Policy Result from the PolicyRes table.

    :params:
      :attr:`name`: string - name of the ValidRes

      :attr:`policyName`: string - the policy name

      :attr:`lastCheckTime`: optional - if TRUE, it will get also the
      LastCheckTime
    """

    req = "SELECT Status, Reason"
    if lastCheckTime:
      req = req + ", LastCheckTime"
    req = req + " FROM PolicyRes WHERE"
    req = req + " Name = '%s' AND PolicyName = '%s'" %(name, policyName)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.getPolicyRes) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return resQuery['Value'][0]

#############################################################################

#############################################################################
# ClientsCache functions
#############################################################################

#############################################################################

  def addOrModifyClientsCacheRes(self, name, commandName, value, result,
                                 opt_ID = None, dateEffective = None):
    """
    Add or modify a Client Result to the ClientCache table.

    :params:
      :attr:`name`: string - name of the ValidRes

      :attr:`commandName`: string - the command name

      :attr:`value`: string - the value

      :attr:`result`: string - command result

      :attr:`opt_ID`: string or integer - optional ID (e.g. used for downtimes)

      :attr:`dateEffective`: datetime.datetime -
      date from which the result is effective
    """

    now = datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' ')

    if dateEffective is None:
      dateEffective = now

    if opt_ID is not None:
      if isinstance(opt_ID, int):
        opt_ID = str(opt_ID)

    req = "SELECT Name, CommandName, "
    if opt_ID is not None:
      req = req + "Opt_ID, "
    req = req + "Value, Result FROM ClientsCache WHERE "
    req = req + "Name = '%s' AND CommandName = '%s' " %(name, commandName)
    if opt_ID is not None:
      req = req + "AND Opt_ID = '%s' " %opt_ID
    req = req + "AND Value = '%s' " %value
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.addOrModifyClientsCacheRes) + resQuery['Message']

    if resQuery['Value']:
      req = "UPDATE ClientsCache SET "
      if resQuery['Value'][0][3] != result:
        req = req + "Result = '%s', DateEffective = '%s', " %(result, dateEffective)
      req = req + "LastCheckTime = '%s' WHERE " %(now)
      req = req + "Name = '%s' AND CommandName = '%s' AND Value = '%s'" %(name, commandName, value)
      if opt_ID is not None:
        req = req + "AND Opt_ID = '%s' " %opt_ID

      resUpdate = self.db._update(req)
      if not resUpdate['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyClientsCacheRes) + resUpdate['Message']
    else:
      req = "INSERT INTO ClientsCache (Name, CommandName, "
      if opt_ID is not None:
        req = req + "Opt_ID, "
      req = req + "Value, Result, DateEffective, "
      req = req + "LastCheckTime) VALUES ('%s', '%s', " %(name, commandName)
      if opt_ID is not None:
        req = req + "'%s', " %opt_ID
      req = req + "'%s', '%s', '%s', '%s')" %(value, result, dateEffective, now)

      resUpdate = self.db._update(req)
      if not resUpdate['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyClientsCacheRes) + resUpdate['Message']

#############################################################################

  def getClientsCacheStuff(self, paramsList = None, ccID = None, name = None, commandName = None,
                           opt_ID = None, value = None, result = None, dateEffective = None,
                           lastCheckTime = None):
    """
    Generic function to get values from the ClientsCache table.

    :params:
      :attr:`paramsList` - string or list of strings

      :attr:`ccID` - string or list of strings

      :attr:`name` - string or list of strings

      :attr:`commandName` - string or list of strings

      :attr:`opt_ID` - string or list of strings

      :attr:`value` - string or list of strings

      :attr:`result` - string or list of strings

      :attr:`dateEffective` - string or list of strings

      :attr:`lastCheckTime` - string or list of strings
    """

    if (paramsList == None or paramsList == []):
      params = "ccID, Name, CommandName, Opt_ID, Value, Result, DateEffective "
    else:
      if type(paramsList) is not type([]):
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    req = "SELECT " + params + "FROM ClientsCache "

    if not (ccID == name == commandName == opt_ID == value ==
            result == dateEffective == lastCheckTime == None):
      req = req + "WHERE "

    if ccID is not None:
      if type(ccID) is not type([]):
        ccID = [ccID]
      req = req + "ccID IN (" + ','.join([str(x).strip() + ' ' for x in ccID]) + ")"

    if name is not None:
      if ccID is not None:
        req = req + " AND "
      if type(name) is not type([]):
        name = [name]
      req = req + "Name IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in name]) + ")"

    if commandName is not None:
      if ccID is not None or name is not None:
        req = req + " AND "
      if type(commandName) is not type([]):
        commandName = [commandName]
      req = req + "CommandName IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in commandName]) + ")"

    if opt_ID is not None:
      if ccID is not None or name is not None or commandName is not None:
        req = req + " AND "
      if type(opt_ID) is not type([]):
        opt_ID = [opt_ID]
      req = req + "Opt_ID IN (" + ','.join(['"' + str(x).strip() + '"' + ' ' for x in opt_ID]) + ")"

    if value is not None:
      if ccID is not None or name is not None or commandName is not None or opt_ID is not None:
        req = req + " AND "
      if type(value) is not type([]):
        value = [value]
      req = req + "Value IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in value]) + ")"

    if result is not None:
      if (ccID is not None or name is not None or commandName is not None or opt_ID is not None or value is not None):
        req = req + " AND "
      if type(result) is not type([]):
        result = [result]
      req = req + "result IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in result]) + ")"

    if dateEffective is not None:
      if (ccID is not None or name is not None or commandName is not None or opt_ID is not None or value is not None or result is not None):
        req = req + " AND "
      if type(dateEffective) is not type([]):
        dateEffective = [dateEffective]
      req = req + "dateEffective IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in dateEffective]) + ")"

    if lastCheckTime is not None:
      if (ccID is not None or name is not None or commandName is not None or opt_ID is not None or value is not None or result is not None or dateEffective is not None):
        req = req + " AND "
      if type(lastCheckTime) is not type([]):
        lastCheckTime = [lastCheckTime]
      req = req + "lastCheckTime IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in lastCheckTime]) + ")"

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.getClientsCacheStuff) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return resQuery['Value']

#############################################################################

#############################################################################
# AccountingCache functions
#############################################################################

#############################################################################

  def addOrModifyAccountingCacheRes(self, name, plotType, plotName, result, dateEffective = None):
    """
    Add or modify an Accounting Result to the AccountingCache table.

    :params:
      :attr:`name`: string - name of the ValidRes

      :attr:`plotType`: string - the plotType name (e.g. 'Pilot')

      :attr:`plotName`: string - the plot name

      :attr:`result`: string - command result

      :attr:`dateEffective`: datetime.datetime -
      date from which the result is effective
    """

    now = datetime.datetime.utcnow().replace(microsecond = 0).isoformat(' ')

    if dateEffective is None:
      dateEffective = now

    req = "SELECT Name, PlotType, PlotName, Result FROM AccountingCache WHERE "
    req = req + "Name = '%s' AND PlotType = '%s' AND PlotName = '%s' " %(name, plotType, plotName)
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.addOrModifyAccountingCacheRes) + resQuery['Message']

    if resQuery['Value']:
      req = "UPDATE AccountingCache SET "
      if resQuery['Value'][0][3] != result:
        req = req + "Result = \"%s\", DateEffective = '%s', " %(result, dateEffective)
      req = req + "LastCheckTime = '%s' WHERE " %(now)
      req = req + "Name = '%s' AND PlotType = '%s' AND PlotName = '%s'" %(name, plotType, plotName)

      resUpdate = self.db._update(req)
      if not resUpdate['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyAccountingCacheRes) + resUpdate['Message']
    else:
      req = "INSERT INTO AccountingCache (Name, PlotType, PlotName, Result, DateEffective, "
      req = req + "LastCheckTime) VALUES ('%s', '%s', '%s', " %(name, plotType, plotName)
      req = req + "\"%s\", '%s', '%s')" %(result, dateEffective, now)

      resUpdate = self.db._update(req)
      if not resUpdate['OK']:
        raise RSSManagementDBException, where(self, self.addOrModifyAccountingCacheRes) + resUpdate['Message']

#############################################################################

  def getAccountingCacheStuff(self, paramsList = None, acID = None, name = None, plotType = None,
                              plotName = None, result = None, dateEffective = None,
                              lastCheckTime = None):
    """
    Generic function to get values from the AccountingCache table.

    :params:
      :attr:`paramsList` - string or list of strings

      :attr:`acID` - string or list of strings

      :attr:`name` - string or list of strings

      :attr:`plotName` - string or list of strings

      :attr:`result` - string or list of strings

      :attr:`dateEffective` - string or list of strings

      :attr:`lastCheckTime` - string or list of strings
    """
    if (paramsList == None or paramsList == []):
      params = "acID, Name, PlotType, PlotName, Result, DateEffective "
    else:
      if type(paramsList) != type([]):
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    req = "SELECT " + params + "FROM AccountingCache "

    if not (acID == name == plotType == plotName == result == dateEffective == lastCheckTime == None):
      req = req + "WHERE "

    if acID is not None:
      if type(acID) is not type([]):
        acID = [acID]
      req = req + "acID IN (" + ','.join([str(x).strip() + ' ' for x in acID]) + ")"

    if name is not None:
      if acID is not None:
        req = req + " AND "
      if type(name) is not type([]):
        name = [name]
      req = req + "Name IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in name]) + ")"

    if plotName is not None:
      if acID is not None or name is not None:
        req = req + " AND "
      if type(plotName) is not type([]):
        plotName = [plotName]
      req = req + "PlotName IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in plotName]) + ")"

    if plotType is not None:
      if acID is not None or name is not None or plotName is not None:
        req = req + " AND "
      if type(plotType) is not type([]):
        plotType = [plotType]
      req = req + "PlotType IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in plotType]) + ")"

    if result is not None:
      if (acID is not None or name is not None or plotName is not None or plotType is not None):
        req = req + " AND "
      if type(result) is not type([]):
        result = [result]
      req = req + "Result IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in result]) + ")"

    if dateEffective is not None:
      if (acID is not None or name is not None or plotName is not None or plotType is not None or result is not None):
        req = req + " AND "
      if type(dateEffective) is not type([]):
        dateEffective = [dateEffective]
      req = req + "DateEffective IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in dateEffective]) + ")"

    if lastCheckTime is not None:
      if (acID is not None or name is not None or plotName is not None or plotType is not None or result is not None or dateEffective is not None):
        req = req + " AND "
      if type(lastCheckTime) is not type([]):
        lastCheckTime = [lastCheckTime]
      req = req + "LastCheckTime IN (" + ','.join(['"' + x.strip() + '"' + ' ' for x in lastCheckTime]) + ")"

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.getAccountingCacheStuff) + resQuery['Message']
    if not resQuery['Value']:
      return []

    return resQuery['Value']

#############################################################################

#############################################################################
# Status functions
#############################################################################

#############################################################################

  #usata solo nell'handler
  def addStatus(self, status, description=''):
    """
    Add a status.

    :params:
      :attr:`status`: string - a new status

      :attr:`description`: string - optional description
    """

    req = "INSERT INTO Status (Status, Description)"
    req = req + "VALUES ('%s', '%s')" % (status, description)

    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSManagementDBException, where(self, self.addStatus) + resUpdate['Message']

#############################################################################

  #usata solo nell'handler
  def removeStatus(self, status):
    """
    Remove a status from the Status table.

    :params:
      :attr:`status`: string - status
    """

    req = "DELETE from Status WHERE Status = '%s'" % (status)
    resDel = self.db._update(req)
    if not resDel['OK']:
      raise RSSManagementDBException, where(self, self.removeStatus) + resDel['Message']

#############################################################################

  def getStatusList(self):
    """
    Get list of status with no descriptions.
    """

    req = "SELECT Status from Status"

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, where(self, self.getStatusList) + resQuery['Message']
    if not resQuery['Value']:
      return []
    l = [ x[0] for x in resQuery['Value']]
    return l

#############################################################################
# Web functions
#############################################################################

#############################################################################

  def getDownTimesWeb(self, selectDict, _sortList = [], startItem = 0, maxItems = 1000):
    """
    Get downtimes registered in the RSS DB (with a web layout)

    :params:
      :attr:`selectDict`: { 'Granularity':['Site', 'Resource'], 'Severity': ['OUTAGE', 'AT_RISK']}

      :attr:`sortList`

      :attr:`startItem`

      :attr:`maxItems`
    """

    granularity = selectDict['Granularity']
    severity = selectDict['Severity']

    if not isinstance(granularity, list):
      granularity = [granularity]
    if not isinstance(severity, list):
      severity = [severity]

    paramNames = ['Granularity', 'Name', 'Severity', 'When']

    req = "SELECT Granularity, Name, Reason FROM PolicyRes WHERE "
    req = req + "PolicyName LIKE 'DT_%' AND Reason LIKE \'%found%\' "
    req = req + "AND Granularity in (%s)" %(','.join(['"'+x.strip()+'"' for x in granularity]))
    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSManagementDBException, resQuery['Message']
    if not resQuery['Value']:
      records = []
    else:
      resQuery = resQuery['Value']
      records = []
      for tuple_ in resQuery:
        sev = tuple_[2].split()[2]
        if sev not in severity:
          continue
        when = tuple_[2].split(sev)[1][1:]
        if when == '':
          when = 'Ongoing'
        records.append([tuple_[0], tuple_[1], sev, when])

    finalDict = {}
    finalDict['TotalRecords'] = len(records)
    finalDict['ParameterNames'] = paramNames

    # Return all the records if maxItems == 0 or the specified number otherwise
    if maxItems:
      finalDict['Records'] = records[startItem:startItem+maxItems]
    else:
      finalDict['Records'] = records

    finalDict['Extras'] = None

    return finalDict

#############################################################################

# User Registry Functions

  def registryAddUser(self, login, name, email):
    req = "INSERT INTO UserRegistryCache (login, name, email) VALUES "
    req += "('%s','%s','%s') " % (login, name, email)
    req += "ON DUPLICATE KEY UPDATE name='%s',email='%s'" % (name, email)
    resUpdate = self.db._update(req)
    if not resUpdate['OK']:
      raise RSSManagementDBException, where(self, self.addStatus) + resUpdate['Message']

  def registryDelUser(self, login):
    pass

  def registryGetMailFromLogin(self, login):
    req = "SELECT email from UserRegistryCache WHERE login='%s'" % login.lower()
    resQuery = self.db._query(req)
    if resQuery['OK'] == False:
      raise RSSManagementDBException, resQuery['Message']
    else:
      resQuery = resQuery['Value']
      return resQuery[0][0]

  def registryGetLoginFromName(self, name):
    req = "SELECT login from UserRegistryCache WHERE name='%s'" % name.lower()
    resQuery = self.db._query(req)
    if resQuery['OK'] == False:
      raise RSSManagementDBException, resQuery['Message']
    else:
      resQuery = resQuery['Value']
      if len(resQuery) > 1: # Many results = conflicts = problems
        raise RSSManagementDBException, "More than one user with name %s" % name
      else:
        # Return first and unique field of the first and unique result
        # returned
        return resQuery[0][0]

  def registryGetMailFromName(self, name):
    req = "SELECT email from UserRegistryCache WHERE name='%s'" % name.lower()
    resQuery = self.db._query(req)
    if resQuery['OK'] == False:
      raise RSSManagementDBException, resQuery['Message']
    else:
      resQuery = resQuery['Value']
      if len(resQuery) > 1: # Many results = conflicts = problems
        raise RSSManagementDBException, "More than one user with name %s" % name
      else:
        # Return first and unique field of the first and unique result
        # returned
        return resQuery[0][0]
