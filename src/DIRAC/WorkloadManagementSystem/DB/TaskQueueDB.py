""" TaskQueueDB class is a front-end to the task queues db
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id"

import six
import random
import string

from DIRAC import gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.PrettyPrint import printDict
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.Security import Properties
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.WorkloadManagementSystem.private.SharesCorrector import SharesCorrector

DEFAULT_GROUP_SHARE = 1000
TQ_MIN_SHARE = 0.001

# For checks at insertion time, and not only
singleValueDefFields = ('OwnerDN', 'OwnerGroup', 'Setup', 'CPUTime')
multiValueDefFields = ('Sites', 'GridCEs', 'BannedSites',
                       'Platforms', 'SubmitPools', 'JobTypes', 'Tags')

# Used for matching
multiValueMatchFields = ('GridCE', 'Site', 'Platform',
                         'SubmitPool', 'JobType', 'Tag')
bannedJobMatchFields = ('Site', )
mandatoryMatchFields = ('Setup', 'CPUTime')
priorityIgnoredFields = ('Sites', 'BannedSites')


def _lowerAndRemovePunctuation(s):
  if six.PY3:
    table = str.maketrans("", "", string.punctuation)  # pylint: disable=no-member
    return s.lower().translate(table)
  else:
    return s.lower().translate(None, string.punctuation)


class TaskQueueDB(DB):
  """ MySQL DB of "Task Queues"
  """

  def __init__(self):
    DB.__init__(self, 'TaskQueueDB', 'WorkloadManagement/TaskQueueDB')
    self.__maxJobsInTQ = 5000
    self.__defaultCPUSegments = [6 * 60,
                                 30 * 60,
                                 1 * 3600,
                                 6 * 3600,
                                 12 * 3600,
                                 1 * 86400,
                                 2 * 86400,
                                 3 * 86400,
                                 4 * 86400,
                                 6 * 86400,
                                 8 * 86400,
                                 10 * 86400,
                                 int(12.5 * 86400)]
    self.__maxMatchRetry = 3
    self.__jobPriorityBoundaries = (0.001, 10)
    self.__groupShares = {}
    self.__deleteTQWithDelay = DictCache(self.__deleteTQIfEmpty)
    self.__opsHelper = Operations()
    self.__ensureInsertionIsSingle = False
    self.__sharesCorrector = SharesCorrector(self.__opsHelper)
    result = self.__initializeDB()
    if not result['OK']:
      raise Exception("Can't create tables: %s" % result['Message'])

  def enableAllTaskQueues(self):
    """ Enable all Task queues
    """
    return self.updateFields("tq_TaskQueues", updateDict={"Enabled": "1"})

  def findOrphanJobs(self):
    """ Find jobs that are not in any task queue
    """
    result = self._query("select JobID from tq_Jobs WHERE TQId not in (SELECT TQId from tq_TaskQueues)")
    if not result['OK']:
      return result
    return S_OK([row[0] for row in result['Value']])

  def isSharesCorrectionEnabled(self):
    return self.__getCSOption("EnableSharesCorrection", False)

  def __getCSOption(self, optionName, defValue):
    return self.__opsHelper.getValue("JobScheduling/%s" % optionName, defValue)

  def getValidPilotTypes(self):
    return self.__getCSOption("AllPilotTypes", ['private'])

  def __initializeDB(self):
    """
    Create the tables
    """
    result = self._query("show tables")
    if not result['OK']:
      return result

    tablesInDB = [t[0] for t in result['Value']]
    tablesToCreate = {}
    self.__tablesDesc = {}

    self.__tablesDesc['tq_TaskQueues'] = {'Fields': {'TQId': 'INTEGER(11) UNSIGNED AUTO_INCREMENT NOT NULL',
                                                     'OwnerDN': 'VARCHAR(255) NOT NULL',
                                                     'OwnerGroup': 'VARCHAR(32) NOT NULL',
                                                     'Setup': 'VARCHAR(32) NOT NULL',
                                                     'CPUTime': 'BIGINT(20) UNSIGNED NOT NULL',
                                                     'Priority': 'FLOAT NOT NULL',
                                                     'Enabled': 'TINYINT(1) NOT NULL DEFAULT 0'
                                                     },
                                          'PrimaryKey': 'TQId',
                                          'Indexes': {'TQOwner': ['OwnerDN', 'OwnerGroup',
                                                                  'Setup', 'CPUTime']
                                                      }
                                          }

    self.__tablesDesc['tq_Jobs'] = {'Fields': {'TQId': 'INTEGER(11) UNSIGNED NOT NULL',
                                               'JobId': 'INTEGER(11) UNSIGNED NOT NULL',
                                               'Priority': 'INTEGER UNSIGNED NOT NULL',
                                               'RealPriority': 'FLOAT NOT NULL'
                                               },
                                    'PrimaryKey': 'JobId',
                                    'Indexes': {'TaskIndex': ['TQId']},
                                    'ForeignKeys': {'TQId': 'tq_TaskQueues.TQId'}
                                    }

    for multiField in multiValueDefFields:
      tableName = 'tq_TQTo%s' % multiField
      self.__tablesDesc[tableName] = {'Fields': {'TQId': 'INTEGER(11) UNSIGNED NOT NULL',
                                                 'Value': 'VARCHAR(64) NOT NULL'
                                                 },
                                      'PrimaryKey': ['TQId', 'Value'],
                                      'Indexes': {'TaskIndex': ['TQId'], '%sIndex' % multiField: ['Value']},
                                      'ForeignKeys': {'TQId': 'tq_TaskQueues.TQId'}
                                      }

    for tableName in self.__tablesDesc:
      if tableName not in tablesInDB:
        tablesToCreate[tableName] = self.__tablesDesc[tableName]

    return self._createTables(tablesToCreate)

  def getGroupsInTQs(self):
    cmdSQL = "SELECT DISTINCT( OwnerGroup ) FROM `tq_TaskQueues`"
    result = self._query(cmdSQL)
    if not result['OK']:
      return result
    return S_OK([row[0] for row in result['Value']])

  def fitCPUTimeToSegments(self, cpuTime):
    """
    Fit the CPU time to the valid segments
    """
    maxCPUSegments = self.__getCSOption("taskQueueCPUTimeIntervals", self.__defaultCPUSegments)
    try:
      maxCPUSegments = [int(seg) for seg in maxCPUSegments]
      # Check segments in the CS
      last = 0
      for cpuS in maxCPUSegments:
        if cpuS <= last:
          maxCPUSegments = self.__defaultCPUSegments
          break
        last = cpuS
    except Exception:
      maxCPUSegments = self.__defaultCPUSegments
    # Map to a segment
    for cpuSegment in maxCPUSegments:
      if cpuTime <= cpuSegment:
        return cpuSegment
    return maxCPUSegments[-1]

  def _checkTaskQueueDefinition(self, tqDefDict):
    """
    Check a task queue definition dict is valid
    """

    for field in singleValueDefFields:
      if field not in tqDefDict:
        return S_ERROR("Missing mandatory field '%s' in task queue definition" % field)
      if field in ["CPUTime"]:
        if not isinstance(tqDefDict[field], six.integer_types):
          return S_ERROR("Mandatory field %s value type is not valid: %s" % (field, type(tqDefDict[field])))
      else:
        if not isinstance(tqDefDict[field], six.string_types):
          return S_ERROR("Mandatory field %s value type is not valid: %s" % (field, type(tqDefDict[field])))
        result = self._escapeString(tqDefDict[field])
        if not result['OK']:
          return result
        tqDefDict[field] = result['Value']
    for field in multiValueDefFields:
      if field not in tqDefDict:
        continue
      if not isinstance(tqDefDict[field], (list, tuple)):
        return S_ERROR("Multi value field %s value type is not valid: %s" % (field, type(tqDefDict[field])))
      result = self._escapeValues(tqDefDict[field])
      if not result['OK']:
        return result
      tqDefDict[field] = result['Value']

    return S_OK(tqDefDict)

  def _checkMatchDefinition(self, tqMatchDict):
    """
    Check a task queue match dict is valid
    """
    def travelAndCheckType(value, validTypes, escapeValues=True):
      if isinstance(value, (list, tuple)):
        for subValue in value:
          if not isinstance(subValue, validTypes):
            return S_ERROR("List contained type %s is not valid -> %s" % (type(subValue), validTypes))
        if escapeValues:
          return self._escapeValues(value)
        return S_OK(value)
      else:
        if not isinstance(value, validTypes):
          return S_ERROR("Type %s is not valid -> %s" % (type(value), validTypes))
        if escapeValues:
          return self._escapeString(value)
        return S_OK(value)

    for field in singleValueDefFields:
      if field not in tqMatchDict:
        if field in mandatoryMatchFields:
          return S_ERROR("Missing mandatory field '%s' in match request definition" % field)
        continue
      fieldValue = tqMatchDict[field]
      if field in ["CPUTime"]:
        result = travelAndCheckType(fieldValue, six.integer_types, escapeValues=False)
      else:
        result = travelAndCheckType(fieldValue, six.string_types)
      if not result['OK']:
        return S_ERROR("Match definition field %s failed : %s" % (field, result['Message']))
      tqMatchDict[field] = result['Value']
    # Check multivalue
    for multiField in multiValueMatchFields:
      for field in (multiField, "Banned%s" % multiField, "Required%s" % multiField):
        if field in tqMatchDict:
          fieldValue = tqMatchDict[field]
          result = travelAndCheckType(fieldValue, six.string_types)
          if not result['OK']:
            return S_ERROR("Match definition field %s failed : %s" % (field, result['Message']))
          tqMatchDict[field] = result['Value']

    return S_OK(tqMatchDict)

  def __createTaskQueue(self, tqDefDict, priority=1, connObj=False):
    """
    Create a task queue
      :returns: S_OK( tqId ) / S_ERROR
    """
    if not connObj:
      result = self._getConnection()
      if not result['OK']:
        return S_ERROR("Can't create task queue: %s" % result['Message'])
      connObj = result['Value']
    tqDefDict['CPUTime'] = self.fitCPUTimeToSegments(tqDefDict['CPUTime'])
    sqlSingleFields = ['TQId', 'Priority']
    sqlValues = ["0", str(priority)]
    for field in singleValueDefFields:
      sqlSingleFields.append(field)
      sqlValues.append(tqDefDict[field])
    # Insert the TQ Disabled
    sqlSingleFields.append("Enabled")
    sqlValues.append("0")
    cmd = "INSERT INTO tq_TaskQueues ( %s ) VALUES ( %s )" % (
        ", ".join(sqlSingleFields), ", ".join([str(v) for v in sqlValues]))
    result = self._update(cmd, conn=connObj)
    if not result['OK']:
      self.log.error("Can't insert TQ in DB", result['Value'])
      return result
    if 'lastRowId' in result:
      tqId = result['lastRowId']
    else:
      result = self._query("SELECT LAST_INSERT_ID()", conn=connObj)
      if not result['OK']:
        self.cleanOrphanedTaskQueues(connObj=connObj)
        return S_ERROR("Can't determine task queue id after insertion")
      tqId = result['Value'][0][0]
    for field in multiValueDefFields:
      if field not in tqDefDict:
        continue
      values = List.uniqueElements([value for value in tqDefDict[field] if value.strip()])
      if not values:
        continue
      cmd = "INSERT INTO `tq_TQTo%s` ( TQId, Value ) VALUES " % field
      cmd += ", ".join(["( %s, %s )" % (tqId, str(value)) for value in values])
      result = self._update(cmd, conn=connObj)
      if not result['OK']:
        self.log.error("Failed to insert condition",
                       "%s : %s" % field, result['Message'])
        self.cleanOrphanedTaskQueues(connObj=connObj)
        return S_ERROR("Can't insert values %s for field %s: %s" % (str(values), field, result['Message']))
    self.log.info("Created TQ", tqId)
    return S_OK(tqId)

  def cleanOrphanedTaskQueues(self, connObj=False):
    """
    Delete all empty task queues
    """
    self.log.info("Cleaning orphaned TQs")
    sq = "SELECT TQId FROM `tq_TaskQueues` WHERE Enabled >= 1 AND TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )"
    result = self._query(sq, conn=connObj)
    if not result['OK']:
      return result
    orphanedTQs = result['Value']
    if not orphanedTQs:
      return S_OK()
    orphanedTQs = [str(otq[0]) for otq in orphanedTQs]

    for mvField in multiValueDefFields:
      result = self._update(
          "DELETE FROM `tq_TQTo%s` WHERE TQId in ( %s )" % (mvField, ','.join(orphanedTQs)), conn=connObj)
      if not result['OK']:
        return result

    result = self._update(
        "DELETE FROM `tq_TaskQueues` WHERE TQId in ( %s )" % ','.join(orphanedTQs), conn=connObj)
    if not result['OK']:
      return result
    return S_OK()

  def __setTaskQueueEnabled(self, tqId, enabled=True, connObj=False):
    if enabled:
      enabled = "+ 1"
    else:
      enabled = "- 1"
    upSQL = "UPDATE `tq_TaskQueues` SET Enabled = Enabled %s WHERE TQId=%d" % (enabled, tqId)
    result = self._update(upSQL, conn=connObj)
    if not result['OK']:
      self.log.error("Error setting TQ state", "TQ %s State %s: %s" % (tqId, enabled, result['Message']))
      return result
    updated = result['Value'] > 0
    if updated:
      self.log.verbose("Set enabled for TQ",
                       "(%s for TQ %s)" % (enabled, tqId))
    return S_OK(updated)

  def __hackJobPriority(self, jobPriority):
    jobPriority = min(max(int(jobPriority), self.__jobPriorityBoundaries[0]), self.__jobPriorityBoundaries[1])
    if jobPriority == self.__jobPriorityBoundaries[0]:
      return 10 ** (-5)
    if jobPriority == self.__jobPriorityBoundaries[1]:
      return 10 ** 6
    return jobPriority

  def insertJob(self, jobId, tqDefDict, jobPriority, skipTQDefCheck=False):
    """ Insert a job in a task queue (creating one if it doesn't exit)

        :param int jobId: job ID
        :param dict tqDefDict: dict for TQ definition
        :param int jobPriority: integer that defines the job priority

        :returns: S_OK() / S_ERROR
    """
    try:
      int(jobId)
    except ValueError:
      return S_ERROR("JobId is not a number!")
    retVal = self._getConnection()
    if not retVal['OK']:
      return S_ERROR("Can't insert job: %s" % retVal['Message'])
    connObj = retVal['Value']
    if not skipTQDefCheck:
      tqDefDict = dict(tqDefDict)
      retVal = self._checkTaskQueueDefinition(tqDefDict)
      if not retVal['OK']:
        self.log.error("TQ definition check failed", retVal['Message'])
        return retVal
      tqDefDict = retVal['Value']
    tqDefDict['CPUTime'] = self.fitCPUTimeToSegments(tqDefDict['CPUTime'])
    self.log.info("Inserting job with requirements",
                  "(%s : %s)" % (jobId, printDict(tqDefDict)))
    retVal = self.__findAndDisableTaskQueue(tqDefDict, skipDefinitionCheck=True, connObj=connObj)
    if not retVal['OK']:
      return retVal
    tqInfo = retVal['Value']
    newTQ = False
    if not tqInfo['found']:
      self.log.info("Creating a TQ for job", jobId)
      retVal = self.__createTaskQueue(tqDefDict, 1, connObj=connObj)
      if not retVal['OK']:
        return retVal
      tqId = retVal['Value']
      newTQ = True
    else:
      tqId = tqInfo['tqId']
      self.log.info("Found TQ for job requirements",
                    "(%s : %s)" % (tqId, jobId))
    try:
      result = self.__insertJobInTaskQueue(jobId, tqId, int(jobPriority), checkTQExists=False, connObj=connObj)
      if not result['OK']:
        self.log.error("Error inserting job in TQ", "Job %s TQ %s: %s" % (jobId, tqId, result['Message']))
        return result
      if newTQ:
        self.recalculateTQSharesForEntity(tqDefDict['OwnerDN'], tqDefDict['OwnerGroup'], connObj=connObj)
    finally:
      self.__setTaskQueueEnabled(tqId, True)
    return S_OK()

  def __insertJobInTaskQueue(self, jobId, tqId, jobPriority, checkTQExists=True, connObj=False):
    """ Insert a job in a given task queue

        :param int jobId: job ID
        :param dict tqDefDict: dict for TQ definition
        :param int jobPriority: integer that defines the job priority

        :returns: S_OK() / S_ERROR
    """
    self.log.info("Inserting job in TQ with priority",
                  "(%s : %s : %s)" % (jobId, tqId, jobPriority))
    if not connObj:
      result = self._getConnection()
      if not result['OK']:
        return S_ERROR("Can't insert job: %s" % result['Message'])
      connObj = result['Value']
    if checkTQExists:
      result = self._query("SELECT tqId FROM `tq_TaskQueues` WHERE TQId = %s" % tqId, conn=connObj)
      if not result['OK'] or not result['Value']:
        return S_OK("Can't find task queue with id %s: %s" % (tqId, result['Message']))
    hackedPriority = self.__hackJobPriority(jobPriority)
    result = self._update("INSERT INTO tq_Jobs ( TQId, JobId, Priority, RealPriority ) \
                            VALUES ( %s, %s, %s, %f ) ON DUPLICATE KEY UPDATE TQId = %s, \
                            Priority = %s, RealPriority = %f" % (tqId, jobId, jobPriority, hackedPriority,
                                                                 tqId, jobPriority, hackedPriority),
                          conn=connObj)
    if not result['OK']:
      return result
    return S_OK()

  def __generateTQFindSQL(self, tqDefDict, skipDefinitionCheck=False):
    """
        Generate the SQL to find a task queue that has exactly the given requirements

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
    """
    if not skipDefinitionCheck:
      tqDefDict = dict(tqDefDict)
      result = self._checkTaskQueueDefinition(tqDefDict)
      if not result['OK']:
        return result
      tqDefDict = result['Value']

    sqlCondList = []
    for field in singleValueDefFields:
      sqlCondList.append("`tq_TaskQueues`.%s = %s" % (field, tqDefDict[field]))
    # MAGIC SUBQUERIES TO ENSURE STRICT MATCH
    for field in multiValueDefFields:
      tableName = '`tq_TQTo%s`' % field
      if field in tqDefDict and tqDefDict[field]:
        firstQuery = "SELECT COUNT(%s.Value) \
                      FROM %s \
                      WHERE %s.TQId = `tq_TaskQueues`.TQId" % (tableName, tableName, tableName)
        grouping = "GROUP BY %s.TQId" % tableName
        valuesList = List.uniqueElements([value.strip() for value in tqDefDict[field] if value.strip()])
        numValues = len(valuesList)
        secondQuery = "%s AND %s.Value in (%s)" % (firstQuery, tableName,
                                                   ",".join(["%s" % str(value) for value in valuesList]))
        sqlCondList.append("%s = (%s %s)" % (numValues, firstQuery, grouping))
        sqlCondList.append("%s = (%s %s)" % (numValues, secondQuery, grouping))
      else:
        sqlCondList.append("`tq_TaskQueues`.TQId not in ( SELECT DISTINCT %s.TQId from %s )" % (tableName, tableName))
    # END MAGIC: That was easy ;)
    return S_OK(" AND ".join(sqlCondList))

  def __findAndDisableTaskQueue(self, tqDefDict, skipDefinitionCheck=False, retries=10, connObj=False):
    """ Disable and find TQ

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
    """
    for _ in range(retries):
      result = self.__findSmallestTaskQueue(tqDefDict, skipDefinitionCheck=skipDefinitionCheck, connObj=connObj)
      if not result['OK']:
        return result
      data = result['Value']
      if not data['found']:
        return result
      if data['enabled'] < 1:
        self.log.debug("TaskQueue {tqId} seems to be already disabled ({enabled})".format(**data))
      result = self.__setTaskQueueEnabled(data['tqId'], False)
      if result['OK']:
        return S_OK(data)
    return S_ERROR("Could not disable TQ")

  def __findSmallestTaskQueue(self, tqDefDict, skipDefinitionCheck=False, connObj=False):
    """
        Find a task queue that has at least the given requirements

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
    """
    result = self.__generateTQFindSQL(tqDefDict, skipDefinitionCheck=skipDefinitionCheck)
    if not result['OK']:
      return result

    sqlCmd = "SELECT COUNT( `tq_Jobs`.JobID ), `tq_TaskQueues`.TQId, `tq_TaskQueues`.Enabled \
FROM `tq_TaskQueues`, `tq_Jobs`"
    sqlCmd = "%s WHERE `tq_TaskQueues`.TQId = `tq_Jobs`.TQId AND %s GROUP BY `tq_Jobs`.TQId \
ORDER BY COUNT( `tq_Jobs`.JobID ) ASC" % (sqlCmd, result['Value'])
    result = self._query(sqlCmd, conn=connObj)
    if not result['OK']:
      self.log.error("Can't find task queue", result['Message'])
      return result
    data = result['Value']
    if not data or data[0][0] >= self.__maxJobsInTQ:
      return S_OK({'found': False})
    return S_OK({'found': True, 'tqId': data[0][1], 'enabled': data[0][2], 'jobs': data[0][0]})

  def matchAndGetJob(self, tqMatchDict, numJobsPerTry=50, numQueuesPerTry=10, negativeCond=None):
    """ Match a job based on requirements

        :param dict tqDefDict: dict for TQ definition
        :returns: S_OK() / S_ERROR
    """
    if negativeCond is None:
      negativeCond = {}
    # Make a copy to avoid modification of original if escaping needs to be done
    tqMatchDict = dict(tqMatchDict)
    retVal = self._checkMatchDefinition(tqMatchDict)
    if not retVal['OK']:
      self.log.error("TQ match request check failed", retVal['Message'])
      return retVal
    retVal = self._getConnection()
    if not retVal['OK']:
      return S_ERROR("Can't connect to DB: %s" % retVal['Message'])
    connObj = retVal['Value']
    preJobSQL = "SELECT `tq_Jobs`.JobId, `tq_Jobs`.TQId \
FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s AND `tq_Jobs`.Priority = %s"
    prioSQL = "SELECT `tq_Jobs`.Priority FROM `tq_Jobs` \
WHERE `tq_Jobs`.TQId = %s ORDER BY RAND() / `tq_Jobs`.RealPriority ASC LIMIT 1"
    postJobSQL = " ORDER BY `tq_Jobs`.JobId ASC LIMIT %s" % numJobsPerTry
    for _ in range(self.__maxMatchRetry):
      noJobsFound = False
      if 'JobID' in tqMatchDict:
        # A certain JobID is required by the resource, so all TQ are to be considered
        retVal = self.matchAndGetTaskQueue(tqMatchDict,
                                           numQueuesToGet=0,
                                           skipMatchDictDef=True,
                                           connObj=connObj)
        preJobSQL = "%s AND `tq_Jobs`.JobId = %s " % (preJobSQL, tqMatchDict['JobID'])
      else:
        retVal = self.matchAndGetTaskQueue(tqMatchDict,
                                           numQueuesToGet=numQueuesPerTry,
                                           skipMatchDictDef=True,
                                           negativeCond=negativeCond,
                                           connObj=connObj)
      if not retVal['OK']:
        return retVal
      tqList = retVal['Value']
      if not tqList:
        self.log.info("No TQ matches requirements")
        return S_OK({'matchFound': False, 'tqMatch': tqMatchDict})
      for tqId, tqOwnerDN, tqOwnerGroup in tqList:
        self.log.verbose("Trying to extract jobs from TQ", tqId)
        retVal = self._query(prioSQL % tqId, conn=connObj)
        if not retVal['OK']:
          return S_ERROR("Can't retrieve winning priority for matching job: %s" % retVal['Message'])
        if not retVal['Value']:
          noJobsFound = True
          continue
        prio = retVal['Value'][0][0]
        retVal = self._query("%s %s" % (preJobSQL % (tqId, prio), postJobSQL), conn=connObj)
        if not retVal['OK']:
          return S_ERROR("Can't begin transaction for matching job: %s" % retVal['Message'])
        jobTQList = [(row[0], row[1]) for row in retVal['Value']]
        if not jobTQList:
          self.log.info("Task queue seems to be empty, triggering a cleaning of", tqId)
          self.__deleteTQWithDelay.add(tqId, 300, (tqId, tqOwnerDN, tqOwnerGroup))
        while jobTQList:
          jobId, tqId = jobTQList.pop(random.randint(0, len(jobTQList) - 1))
          self.log.verbose("Trying to extract job from TQ",
                           "%s : %s" % (jobId, tqId))
          retVal = self.deleteJob(jobId, connObj=connObj)
          if not retVal['OK']:
            msgFix = "Could not take job"
            msgVar = " %s out from the TQ %s: %s" % (jobId, tqId, retVal['Message'])
            self.log.error(msgFix, msgVar)
            return S_ERROR(msgFix + msgVar)
          if retVal['Value']:
            self.log.info("Extracted job with prio from TQ",
                          "(%s : %s : %s)" % (jobId, prio, tqId))
            return S_OK({'matchFound': True, 'jobId': jobId, 'taskQueueId': tqId, 'tqMatch': tqMatchDict})
        self.log.info("No jobs could be extracted from TQ", tqId)
    if noJobsFound:
      return S_OK({'matchFound': False, 'tqMatch': tqMatchDict})

    self.log.info("Could not find a match after %s match retries" % self.__maxMatchRetry)
    return S_ERROR("Could not find a match after %s match retries" % self.__maxMatchRetry)

  def matchAndGetTaskQueue(self, tqMatchDict, numQueuesToGet=1, skipMatchDictDef=False,
                           negativeCond=None, connObj=False):
    """ Get a queue that matches the requirements
    """
    if negativeCond is None:
      negativeCond = {}
    # Make a copy to avoid modification of original if escaping needs to be done
    tqMatchDict = dict(tqMatchDict)
    if not skipMatchDictDef:
      retVal = self._checkMatchDefinition(tqMatchDict)
      if not retVal['OK']:
        return retVal
    retVal = self.__generateTQMatchSQL(tqMatchDict, numQueuesToGet=numQueuesToGet, negativeCond=negativeCond)
    if not retVal['OK']:
      return retVal
    matchSQL = retVal['Value']
    retVal = self._query(matchSQL, conn=connObj)
    if not retVal['OK']:
      return retVal
    return S_OK([(row[0], row[1], row[2]) for row in retVal['Value']])

  @staticmethod
  def __generateSQLSubCond(sqlString, value, boolOp='OR'):
    if not isinstance(value, (list, tuple)):
      return sqlString % str(value).strip()
    sqlORList = []
    for v in value:
      sqlORList.append(sqlString % str(v).strip())
    return "( %s )" % (" %s " % boolOp).join(sqlORList)

  def __generateNotSQL(self, negativeCond):
    """ Generate negative conditions
        Can be a list of dicts or a dict:
         - list of dicts will be  OR of conditional dicts
         - dicts will be normal conditional dict ( kay1 in ( v1, v2, ... ) AND key2 in ( v3, v4, ... ) )
    """
    if isinstance(negativeCond, (list, tuple)):
      sqlCond = []
      for cD in negativeCond:
        sqlCond.append(self.__generateNotDictSQL(cD))
      return " ( %s )" % " OR  ".join(sqlCond)
    elif isinstance(negativeCond, dict):
      return self.__generateNotDictSQL(negativeCond)
    raise RuntimeError("negativeCond has to be either a list or a dict or a tuple, and it's %s" % type(negativeCond))

  def __generateNotDictSQL(self, negativeCond):
    """ Generate the negative sql condition from a standard condition dict
        not ( cond1 and cond2 ) = ( not cond1 or not cond 2 )
        For instance: { 'Site': 'S1', 'JobType': [ 'T1', 'T2' ] }
          ( not 'S1' in Sites or ( not 'T1' in JobType and not 'T2' in JobType ) )
          S2 T1 -> not False or ( not True and not False ) -> True or ... -> True -> Eligible
          S1 T3 -> not True or ( not False and not False ) -> False or (True and True ) -> True -> Eligible
          S1 T1 -> not True or ( not True and not False ) -> False or ( False and True ) -> False -> Nop
    """
    condList = []
    for field in negativeCond:
      if field in multiValueMatchFields:
        fullTableN = '`tq_TQTo%ss`' % field
        valList = negativeCond[field]
        if not isinstance(valList, (list, tuple)):
          valList = (valList, )
        subList = []
        for value in valList:
          value = self._escapeString(value)['Value']
          sql = "%s NOT IN ( SELECT %s.Value FROM %s WHERE %s.TQId = tq.TQId )" % (value,
                                                                                   fullTableN,
                                                                                   fullTableN,
                                                                                   fullTableN)
          subList.append(sql)
        condList.append("( %s )" % " AND ".join(subList))
      elif field in singleValueDefFields:
        for value in negativeCond[field]:
          value = self._escapeString(value)['Value']
          sql = "%s != tq.%s " % (value, field)
          condList.append(sql)
    return "( %s )" % " OR ".join(condList)

  @staticmethod
  def __generateTablesName(sqlTables, field):
    fullTableName = 'tq_TQTo%ss' % field
    if fullTableName not in sqlTables:
      tableN = field.lower()
      sqlTables[fullTableName] = tableN
      return tableN, "`%s`" % fullTableName,
    return sqlTables[fullTableName], "`%s`" % fullTableName

  def __generateTQMatchSQL(self, tqMatchDict, numQueuesToGet=1, negativeCond=None):
    """
    Generate the SQL needed to match a task queue
    """
    self.log.debug(tqMatchDict)

    if negativeCond is None:
      negativeCond = {}
    # Only enabled TQs
    sqlCondList = []
    sqlTables = {"tq_TaskQueues": "tq"}
    # If OwnerDN and OwnerGroup are defined only use those combinations that make sense
    if 'OwnerDN' in tqMatchDict and 'OwnerGroup' in tqMatchDict:
      groups = tqMatchDict['OwnerGroup']
      if not isinstance(groups, (list, tuple)):
        groups = [groups]
      dns = tqMatchDict['OwnerDN']
      if not isinstance(dns, (list, tuple)):
        dns = [dns]
      ownerConds = []
      for group in groups:
        if Properties.JOB_SHARING in Registry.getPropertiesForGroup(group.replace('"', "")):
          ownerConds.append("tq.OwnerGroup = %s" % group)
        else:
          for dn in dns:
            ownerConds.append("( tq.OwnerDN = %s AND tq.OwnerGroup = %s )" % (dn, group))
      sqlCondList.append(" OR ".join(ownerConds))
    else:
      # If not both are defined, just add the ones that are defined
      for field in ('OwnerGroup', 'OwnerDN'):
        if field in tqMatchDict:
          sqlCondList.append(self.__generateSQLSubCond("tq.%s = %%s" % field,
                                                       tqMatchDict[field]))
    # Type of single value conditions
    for field in ('CPUTime', 'Setup'):
      if field in tqMatchDict:
        if field == 'CPUTime':
          sqlCondList.append(self.__generateSQLSubCond("tq.%s <= %%s" % field, tqMatchDict[field]))
        else:
          sqlCondList.append(self.__generateSQLSubCond("tq.%s = %%s" % field, tqMatchDict[field]))

    tag_fv = []

    # Match multi value fields
    for field in multiValueMatchFields:
      self.log.debug("Evaluating field %s" % field)
      # It has to be %ss , with an 's' at the end because the columns names
      # are plural and match options are singular

      # Just treating the (not so) special case of no Tag, No RequiredTag
      if 'Tag' not in tqMatchDict and 'RequiredTag' not in tqMatchDict:
        tqMatchDict['Tag'] = []

      if field in tqMatchDict:
        self.log.debug("Evaluating %s with value %s" % (field, tqMatchDict[field]))

        _, fullTableN = self.__generateTablesName(sqlTables, field)

        sqlMultiCondList = []
        csql = None

        # Now evaluating Tags
        if field == 'Tag':
          tag_fv = tqMatchDict.get('Tag')
          self.log.debug("Evaluating tag %s of type %s" % (tag_fv, type(tag_fv)))
          if isinstance(tag_fv, str):
            tag_fv = [tag_fv]

          # Is there something to consider?
          if any(_lowerAndRemovePunctuation(fvx) == 'any' for fvx in tag_fv):
            continue
          else:
            sqlMultiCondList.append(self.__generateTagSQLSubCond(fullTableN, tag_fv))

        # Now evaluating everything that is not tags
        else:
          fv = tqMatchDict.get(field)
          self.log.debug("Evaluating field %s of type %s" % (field, type(fv)))

          # Is there something to consider?
          if not fv:
            continue
          if isinstance(fv, str) and _lowerAndRemovePunctuation(fv) == 'any':
            continue
          if isinstance(fv, list) and any(_lowerAndRemovePunctuation(fvx) == 'any' for fvx in fv):
            continue
          # if field != 'GridCE' or 'Site' in tqMatchDict:
          # Jobs for masked sites can be matched if they specified a GridCE
          # Site is removed from tqMatchDict if the Site is mask. In this case we want
          # that the GridCE matches explicitly so the COUNT can not be 0. In this case we skip this
          # condition
          sqlMultiCondList.append("( SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId = tq.TQId ) = 0" % (fullTableN,
                                                                                                      fullTableN,
                                                                                                      fullTableN))
          sqlMultiCondList.append(self.__generateSQLSubCond("%%s IN ( SELECT %s.Value \
                                                            FROM %s \
                                                            WHERE %s.TQId = tq.TQId )" % (fullTableN,
                                                                                          fullTableN,
                                                                                          fullTableN),
                                                            tqMatchDict.get(field)))

        sqlCondList.append("( %s )" % " OR ".join(sqlMultiCondList))

        # In case of Site, check it's not in job banned sites
        if field in bannedJobMatchFields:
          fullTableN = '`tq_TQToBanned%ss`' % field
          csql = self.__generateSQLSubCond("%%s not in ( SELECT %s.Value \
                                                          FROM %s \
                                                          WHERE %s.TQId = tq.TQId )" % (fullTableN,
                                                                                        fullTableN,
                                                                                        fullTableN),
                                           tqMatchDict[field], boolOp='OR')
          sqlCondList.append(csql)

    # Add possibly RequiredTag conditions
    rtag_fv = tqMatchDict.get('RequiredTag', [])
    if isinstance(rtag_fv, str):
      rtag_fv = [rtag_fv]

    # Is there something to consider?
    if not rtag_fv or any(_lowerAndRemovePunctuation(fv) == 'any' for fv in rtag_fv):
      pass
    elif not set(rtag_fv).issubset(set(tag_fv)):
      return S_ERROR('Wrong conditions')
    else:
      self.log.debug("Evaluating RequiredTag %s" % rtag_fv)
      sqlCondList.append(self.__generateRequiredTagSQLSubCond('`tq_TQToTags`', rtag_fv))

    # Add possibly Resource banning conditions
    for field in multiValueMatchFields:
      bannedField = "Banned%s" % field

      # Is there something to consider?
      b_fv = tqMatchDict.get(bannedField)
      if not b_fv \
              or isinstance(b_fv, str) and _lowerAndRemovePunctuation(b_fv) == 'any' \
              or isinstance(b_fv, list) \
              and any(_lowerAndRemovePunctuation(fvx) == 'any' for fvx in b_fv):
        continue

      fullTableN = '`tq_TQTo%ss`' % field

      sqlCondList.append(self.__generateSQLSubCond("%%s not in ( SELECT %s.Value \
                                                      FROM %s \
                                                      WHERE %s.TQId = tq.TQId )" % (fullTableN,
                                                                                    fullTableN,
                                                                                    fullTableN),
                                                   b_fv,
                                                   boolOp='OR'))

    # Add extra negative conditions
    if negativeCond:
      sqlCondList.append(self.__generateNotSQL(negativeCond))

    # Generate the final query string
    tqSqlCmd = "SELECT tq.TQId, tq.OwnerDN, tq.OwnerGroup FROM `tq_TaskQueues` tq WHERE %s" % (
        " AND ".join(sqlCondList))

    # Apply priorities
    tqSqlCmd = "%s ORDER BY RAND() / tq.Priority ASC" % tqSqlCmd

    # Do we want a limit?
    if numQueuesToGet:
      tqSqlCmd = "%s LIMIT %s" % (tqSqlCmd, numQueuesToGet)
    return S_OK(tqSqlCmd)

  @staticmethod
  def __generateTagSQLSubCond(tableName, tagMatchList):
    """ Generate SQL condition where ALL the specified multiValue requirements must be
        present in the matching resource list
    """
    sql1 = "SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId=tq.TQId" % (tableName, tableName, tableName)
    if not tagMatchList:
      sql2 = sql1 + " AND %s.Value=''" % tableName
    else:
      if isinstance(tagMatchList, (list, tuple)):
        sql2 = sql1 + " AND %s.Value in ( %s )" % (tableName, ','.join(["%s" % v for v in tagMatchList]))
      else:
        sql2 = sql1 + " AND %s.Value=%s" % (tableName, tagMatchList)
    sql = '( ' + sql1 + ' ) = (' + sql2 + ' )'
    return sql

  @staticmethod
  def __generateRequiredTagSQLSubCond(tableName, tagMatchList):
    """ Generate SQL condition where the TQ corresponds to the requirements
        of the resource
    """
    sql = "SELECT COUNT(%s.Value) FROM %s WHERE %s.TQId=tq.TQId" % (tableName, tableName, tableName)
    if isinstance(tagMatchList, (list, tuple)):
      sql = sql + " AND %s.Value in ( %s )" % (tableName, ','.join(["%s" % v for v in tagMatchList]))
      nTags = len(tagMatchList)
    else:
      sql = sql + " AND %s.Value=%s" % (tableName, tagMatchList)
      nTags = 1
    sql = '( %s ) = %s' % (sql, nTags)
    return sql

  def deleteJob(self, jobId, connObj=False):
    """
    Delete a job from the task queues
    Return S_OK( True/False ) / S_ERROR
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal['OK']:
        return S_ERROR("Can't delete job: %s" % retVal['Message'])
      connObj = retVal['Value']
    retVal = self._query(
        "SELECT t.TQId, t.OwnerDN, t.OwnerGroup \
FROM `tq_TaskQueues` t, `tq_Jobs` j \
WHERE j.JobId = %s AND t.TQId = j.TQId" %
        jobId, conn=connObj)
    if not retVal['OK']:
      return S_ERROR("Could not get job from task queue %s: %s" % (jobId, retVal['Message']))
    data = retVal['Value']
    if not data:
      return S_OK(False)
    tqId, tqOwnerDN, tqOwnerGroup = data[0]
    self.log.verbose("Deleting job", jobId)
    retVal = self._update("DELETE FROM `tq_Jobs` WHERE JobId = %s" % jobId, conn=connObj)
    if not retVal['OK']:
      return S_ERROR("Could not delete job from task queue %s: %s" % (jobId, retVal['Message']))
    if retVal['Value'] == 0:
      # No job deleted
      return S_OK(False)
    # Always return S_OK() because job has already been taken out from the TQ
    self.__deleteTQWithDelay.add(tqId, 300, (tqId, tqOwnerDN, tqOwnerGroup))
    return S_OK(True)

  def getTaskQueueForJob(self, jobId, connObj=False):
    """
    Return TaskQueue for a given Job
    Return S_OK( [TaskQueueID] ) / S_ERROR
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal['OK']:
        return S_ERROR("Can't get TQ for job: %s" % retVal['Message'])
      connObj = retVal['Value']

    retVal = self._query('SELECT TQId FROM `tq_Jobs` WHERE JobId = %s' % jobId, conn=connObj)

    if not retVal['OK']:
      return retVal

    if not retVal['Value']:
      return S_ERROR('Not in TaskQueues')

    return S_OK(retVal['Value'][0][0])

  def getTaskQueueForJobs(self, jobIDs, connObj=False):
    """
    Return TaskQueues for a given list of Jobs
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal['OK']:
        self.log.error("Can't get TQs for a job list", retVal['Message'])
        return retVal
      connObj = retVal['Value']

    cmd = 'SELECT JobId,TQId FROM `tq_Jobs` WHERE JobId IN (%s) ' % ','.join(str(x) for x in jobIDs)
    retVal = self._query(cmd, conn=connObj)

    if not retVal['OK']:
      return retVal

    if not retVal['Value']:
      return S_ERROR('Not in TaskQueues')

    resultDict = {}
    for jobID, tqID in retVal['Value']:
      resultDict[int(jobID)] = int(tqID)

    return S_OK(resultDict)

  def __getOwnerForTaskQueue(self, tqId, connObj=False):
    retVal = self._query("SELECT OwnerDN, OwnerGroup from `tq_TaskQueues` WHERE TQId=%s" % tqId, conn=connObj)
    if not retVal['OK']:
      return retVal
    data = retVal['Value']
    if not data:
      return S_OK(False)
    return S_OK(retVal['Value'][0])

  def __deleteTQIfEmpty(self, args):
    (tqId, tqOwnerDN, tqOwnerGroup) = args
    retries = 3
    while retries:
      retries -= 1
      result = self.deleteTaskQueueIfEmpty(tqId, tqOwnerDN, tqOwnerGroup)
      if result['OK']:
        return
    self.log.error("Could not delete TQ",
                   "%s: %s" % (tqId, result['Message']))

  def deleteTaskQueueIfEmpty(self, tqId, tqOwnerDN=False, tqOwnerGroup=False, connObj=False):
    """
    Try to delete a task queue if its empty
    """
    if not connObj:
      retVal = self._getConnection()
      if not retVal['OK']:
        self.log.error("Can't insert job", retVal['Message'])
        return retVal
      connObj = retVal['Value']
    if not tqOwnerDN or not tqOwnerGroup:
      retVal = self.__getOwnerForTaskQueue(tqId, connObj=connObj)
      if not retVal['OK']:
        return retVal
      data = retVal['Value']
      if not data:
        return S_OK(False)
      tqOwnerDN, tqOwnerGroup = data

    sqlCmd = "SELECT TQId FROM `tq_TaskQueues` WHERE Enabled >= 1 AND `tq_TaskQueues`.TQId = %s " % tqId
    sqlCmd += "AND `tq_TaskQueues`.TQId not in ( SELECT DISTINCT TQId from `tq_Jobs` )"
    retVal = self._query(sqlCmd, conn=connObj)
    if not retVal['OK']:
      self.log.error("Could not select task queue",
                     "%s : %s" % tqId, retVal['Message'])
      return retVal
    tqToDel = retVal['Value']

    if tqToDel:
      for mvField in multiValueDefFields:
        retVal = self._update("DELETE FROM `tq_TQTo%s` WHERE TQId = %s" % (mvField, tqId), conn=connObj)
        if not retVal['OK']:
          return retVal
      retVal = self._update("DELETE FROM `tq_TaskQueues` WHERE TQId = %s" % tqId, conn=connObj)
      if not retVal['OK']:
        return retVal
      self.recalculateTQSharesForEntity(tqOwnerDN, tqOwnerGroup, connObj=connObj)
      self.log.info("Deleted empty and enabled TQ", tqId)
      return S_OK()
    return S_OK(False)

  def deleteTaskQueue(self, tqId, tqOwnerDN=False, tqOwnerGroup=False, connObj=False):
    """
    Try to delete a task queue even if it has jobs
    """
    self.log.info("Deleting TQ", tqId)
    if not connObj:
      retVal = self._getConnection()
      if not retVal['OK']:
        return S_ERROR("Can't insert job: %s" % retVal['Message'])
      connObj = retVal['Value']
    if not tqOwnerDN or not tqOwnerGroup:
      retVal = self.__getOwnerForTaskQueue(tqId, connObj=connObj)
      if not retVal['OK']:
        return retVal
      data = retVal['Value']
      if not data:
        return S_OK(False)
      tqOwnerDN, tqOwnerGroup = data
    sqlCmd = "DELETE FROM `tq_TaskQueues` WHERE `tq_TaskQueues`.TQId = %s" % tqId
    retVal = self._update(sqlCmd, conn=connObj)
    if not retVal['OK']:
      return S_ERROR("Could not delete task queue %s: %s" % (tqId, retVal['Message']))
    delTQ = retVal['Value']
    sqlCmd = "DELETE FROM `tq_Jobs` WHERE `tq_Jobs`.TQId = %s" % tqId
    retVal = self._update(sqlCmd, conn=connObj)
    if not retVal['OK']:
      return S_ERROR("Could not delete task queue %s: %s" % (tqId, retVal['Message']))
    for field in multiValueDefFields:
      retVal = self._update("DELETE FROM `tq_TQTo%s` WHERE TQId = %s" % (field, tqId), conn=connObj)
      if not retVal['OK']:
        return retVal
    if delTQ > 0:
      self.recalculateTQSharesForEntity(tqOwnerDN, tqOwnerGroup, connObj=connObj)
      return S_OK(True)
    return S_OK(False)

  def getMatchingTaskQueues(self, tqMatchDict, negativeCond=False):
    """ Get the info of the task queues that match a resource
    """
    result = self.matchAndGetTaskQueue(tqMatchDict, numQueuesToGet=0, negativeCond=negativeCond)
    if not result['OK']:
      return result
    return self.retrieveTaskQueues([tqTuple[0] for tqTuple in result['Value']])

  def getNumTaskQueues(self):
    """
     Get the number of task queues in the system
    """
    sqlCmd = "SELECT COUNT( TQId ) FROM `tq_TaskQueues`"
    retVal = self._query(sqlCmd)
    if not retVal['OK']:
      return retVal
    return S_OK(retVal['Value'][0][0])

  def retrieveTaskQueues(self, tqIdList=None):
    """
    Get all the task queues
    """
    sqlSelectEntries = ["`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority", "COUNT( `tq_Jobs`.TQId )"]
    sqlGroupEntries = ["`tq_TaskQueues`.TQId", "`tq_TaskQueues`.Priority"]
    for field in singleValueDefFields:
      sqlSelectEntries.append("`tq_TaskQueues`.%s" % field)
      sqlGroupEntries.append("`tq_TaskQueues`.%s" % field)
    sqlCmd = "SELECT %s FROM `tq_TaskQueues`, `tq_Jobs`" % ", ".join(sqlSelectEntries)
    sqlTQCond = ""
    if tqIdList is not None:
      if not tqIdList:
        # Empty list => Fast-track no matches
        return S_OK({})
      else:
        sqlTQCond += " AND `tq_TaskQueues`.TQId in ( %s )" % ", ".join([str(id_) for id_ in tqIdList])
    sqlCmd = "%s WHERE `tq_TaskQueues`.TQId = `tq_Jobs`.TQId %s GROUP BY %s" % (sqlCmd,
                                                                                sqlTQCond,
                                                                                ", ".join(sqlGroupEntries))
    retVal = self._query(sqlCmd)
    if not retVal['OK']:
      self.log.error("Can't retrieve task queues info", retVal['Message'])
      return retVal
    tqData = {}
    for record in retVal['Value']:
      tqId = record[0]
      tqData[tqId] = {'Priority': record[1], 'Jobs': record[2]}
      record = record[3:]
      for iP, _ in enumerate(singleValueDefFields):
        tqData[tqId][singleValueDefFields[iP]] = record[iP]

    tqNeedCleaning = False
    for field in multiValueDefFields:
      table = "`tq_TQTo%s`" % field
      sqlCmd = "SELECT %s.TQId, %s.Value FROM %s" % (table, table, table)
      retVal = self._query(sqlCmd)
      if not retVal['OK']:
        self.log.error("Can't retrieve task queues field",
                       "%s info: %s" % (field, retVal['Message']))
        return retVal
      for record in retVal['Value']:
        tqId = record[0]
        value = record[1]
        if tqId not in tqData:
          if tqIdList is None or tqId in tqIdList:
            self.log.verbose(
                "Task Queue is defined for a field, but does not exist: triggering a cleaning",
                "TQID: %s, field: %s" % (tqId, field))
            tqNeedCleaning = True
        else:
          if field not in tqData[tqId]:
            tqData[tqId][field] = []
          tqData[tqId][field].append(value)
    if tqNeedCleaning:
      self.cleanOrphanedTaskQueues()
    return S_OK(tqData)

  def __updateGlobalShares(self):
    """
    Update internal structure for shares
    """
    # Update group shares
    self.__groupShares = self.getGroupShares()
    # Apply corrections if enabled
    if self.isSharesCorrectionEnabled():
      result = self.getGroupsInTQs()
      if not result['OK']:
        self.log.error("Could not get groups in the TQs", result['Message'])
      activeGroups = result['Value']
      newShares = {}
      for group in activeGroups:
        if group in self.__groupShares:
          newShares[group] = self.__groupShares[group]
      newShares = self.__sharesCorrector.correctShares(newShares)
      for group in self.__groupShares:
        if group in newShares:
          self.__groupShares[group] = newShares[group]

  def recalculateTQSharesForAll(self):
    """
    Recalculate all priorities for TQ's
    """
    if self.isSharesCorrectionEnabled():
      self.log.info("Updating correctors state")
      self.__sharesCorrector.update()
    self.__updateGlobalShares()
    self.log.info("Recalculating shares for all TQs")
    retVal = self._getConnection()
    if not retVal['OK']:
      return S_ERROR("Can't insert job: %s" % retVal['Message'])
    result = self._query("SELECT DISTINCT( OwnerGroup ) FROM `tq_TaskQueues`")
    if not result['OK']:
      return result
    for group in [r[0] for r in result['Value']]:
      self.recalculateTQSharesForEntity("all", group)
    return S_OK()

  def recalculateTQSharesForEntity(self, userDN, userGroup, connObj=False):
    """
    Recalculate the shares for a userDN/userGroup combo
    """
    self.log.info("Recalculating shares",
                  "for %s@%s TQs" % (userDN, userGroup))
    if userGroup in self.__groupShares:
      share = self.__groupShares[userGroup]
    else:
      share = float(DEFAULT_GROUP_SHARE)
    if Properties.JOB_SHARING in Registry.getPropertiesForGroup(userGroup):
      # If group has JobSharing just set prio for that entry, userDN is irrelevant
      return self.__setPrioritiesForEntity(userDN, userGroup, share, connObj=connObj)

    selSQL = "SELECT OwnerDN, COUNT(OwnerDN) FROM `tq_TaskQueues` WHERE OwnerGroup='%s' GROUP BY OwnerDN" % (userGroup)
    result = self._query(selSQL, conn=connObj)
    if not result['OK']:
      return result
    # Get owners in this group and the amount of times they appear
    data = [(r[0], r[1]) for r in result['Value'] if r]
    numOwners = len(data)
    # If there are no owners do now
    if numOwners == 0:
      return S_OK()
    # Split the share amongst the number of owners
    share /= numOwners
    entitiesShares = dict([(row[0], share) for row in data])
    # If corrector is enabled let it work it's magic
    if self.isSharesCorrectionEnabled():
      entitiesShares = self.__sharesCorrector.correctShares(entitiesShares, group=userGroup)
    # Keep updating
    owners = dict(data)
    # IF the user is already known and has more than 1 tq, the rest of the users don't need to be modified
    # (The number of owners didn't change)
    if userDN in owners and owners[userDN] > 1:
      return self.__setPrioritiesForEntity(userDN, userGroup, entitiesShares[userDN], connObj=connObj)
    # Oops the number of owners may have changed so we recalculate the prio for all owners in the group
    for userDN in owners:
      self.__setPrioritiesForEntity(userDN, userGroup, entitiesShares[userDN], connObj=connObj)
    return S_OK()

  def __setPrioritiesForEntity(self, userDN, userGroup, share, connObj=False, consolidationFunc="AVG"):
    """
    Set the priority for a userDN/userGroup combo given a splitted share
    """
    self.log.info("Setting priorities", "to %s@%s TQs" % (userDN, userGroup))
    tqCond = ["t.OwnerGroup='%s'" % userGroup]
    allowBgTQs = gConfig.getValue("/Registry/Groups/%s/AllowBackgroundTQs" % userGroup, False)
    if Properties.JOB_SHARING not in Registry.getPropertiesForGroup(userGroup):
      res = self._escapeString(userDN)
      if not res['OK']:
        return res
      userDN = res['Value']
      tqCond.append("t.OwnerDN= %s " % userDN)
    tqCond.append("t.TQId = j.TQId")
    if consolidationFunc == 'AVG':
      selectSQL = "SELECT j.TQId, SUM( j.RealPriority )/COUNT(j.RealPriority) \
FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE "
    elif consolidationFunc == 'SUM':
      selectSQL = "SELECT j.TQId, SUM( j.RealPriority ) FROM `tq_TaskQueues` t, `tq_Jobs` j WHERE "
    else:
      return S_ERROR("Unknown consolidation func %s for setting priorities" % consolidationFunc)
    selectSQL += " AND ".join(tqCond)
    selectSQL += " GROUP BY t.TQId"
    result = self._query(selectSQL, conn=connObj)
    if not result['OK']:
      return result

    tqDict = dict(result['Value'])
    if not tqDict:
      return S_OK()
    # Calculate Sum of priorities
    totalPrio = 0
    for k in tqDict:
      if tqDict[k] > 0.1 or not allowBgTQs:
        totalPrio += tqDict[k]
    # Update prio for each TQ
    for tqId in tqDict:
      if tqDict[tqId] > 0.1 or not allowBgTQs:
        prio = (share / totalPrio) * tqDict[tqId]
      else:
        prio = TQ_MIN_SHARE
      prio = max(prio, TQ_MIN_SHARE)
      tqDict[tqId] = prio

    # Generate groups of TQs that will have the same prio=sum(prios) maomenos
    result = self.retrieveTaskQueues(list(tqDict))
    if not result['OK']:
      return result
    allTQsData = result['Value']
    tqGroups = {}
    for tqid in allTQsData:
      tqData = allTQsData[tqid]
      for field in ('Jobs', 'Priority') + priorityIgnoredFields:
        if field in tqData:
          tqData.pop(field)
      tqHash = []
      for f in sorted(tqData):
        tqHash.append("%s:%s" % (f, tqData[f]))
      tqHash = "|".join(tqHash)
      if tqHash not in tqGroups:
        tqGroups[tqHash] = []
      tqGroups[tqHash].append(tqid)
    tqGroups = [tqGroups[td] for td in tqGroups]

    # Do the grouping
    for tqGroup in tqGroups:
      totalPrio = 0
      if len(tqGroup) < 2:
        continue
      for tqid in tqGroup:
        totalPrio += tqDict[tqid]
      for tqid in tqGroup:
        tqDict[tqid] = totalPrio

    # Group by priorities
    prioDict = {}
    for tqId in tqDict:
      prio = tqDict[tqId]
      if prio not in prioDict:
        prioDict[prio] = []
      prioDict[prio].append(tqId)

    # Execute updates
    for prio in prioDict:
      tqList = ", ".join([str(tqId) for tqId in prioDict[prio]])
      updateSQL = "UPDATE `tq_TaskQueues` SET Priority=%.4f WHERE TQId in ( %s )" % (prio, tqList)
      self._update(updateSQL, conn=connObj)
    return S_OK()

  @staticmethod
  def getGroupShares():
    """
    Get all the shares as a DICT
    """
    result = gConfig.getSections("/Registry/Groups")
    if result['OK']:
      groups = result['Value']
    else:
      groups = []
    shares = {}
    for group in groups:
      shares[group] = gConfig.getValue("/Registry/Groups/%s/JobShare" % group, DEFAULT_GROUP_SHARE)
    return shares
