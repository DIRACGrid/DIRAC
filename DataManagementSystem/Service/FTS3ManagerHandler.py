"""
Service handler for FT3SDB using DISET

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN FTS3Manager
  :end-before: ##END
  :dedent: 2
  :caption: FTS3Manager options

"""

__RCSID__ = "$Id$"

# from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Security.Properties import FULL_DELEGATION, LIMITED_DELEGATION, TRUSTED_HOST
from DIRAC.Core.Utilities import DErrno


from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB
from DIRAC.Core.Utilities.JEncode import encode, decode

########################################################################


class FTS3ManagerHandler(RequestHandler):
  """
  .. class:: FTS3ManagerHandler

  """

  fts3db = None

  @classmethod
  def initializeHandler(cls, serviceInfoDict):
    """ initialize handler """
    try:
      maxThreads = getServiceOption(serviceInfoDict, 'MaxThreads', 15)
      cls.fts3db = FTS3DB(pool_size=maxThreads)
    except RuntimeError as error:
      gLogger.exception(error)
      return S_ERROR(error)

    # # create tables for empty db
    return cls.fts3db.createTables()

  @staticmethod
  def _isAllowed(opObj, remoteCredentials):
    """
        Make sure the client is allowed to persist an operation
        (FULL_DELEGATION or LIMITED_DELEGATION). This is the case of pilots,
        the RequestExecutingAgent or the FTS3Agent

        :param opObj: the FTS3Operation object
        :param remoteCredentials: credentials from the clients

        :returns: True if everything is fine, False otherwise
    """

    credDN = remoteCredentials['DN']
    credGroup = remoteCredentials['group']
    credProperties = remoteCredentials['properties']

    # First, get the DN matching the username
    res = getDNForUsername(opObj.username)
    # if we have an error, do not allow
    if not res['OK']:
      gLogger.error("Error retrieving DN for username", res)
      return False

    # List of DN matching the username
    dnList = res['Value']

    # If the credentials in the Request match those from the credentials, it's OK
    if credDN in dnList and opObj.userGroup == credGroup:
      return True

    # From here, something/someone is putting a request on behalf of someone else

    # Only allow this if the credentials have Full or Limited delegation properties

    if FULL_DELEGATION in credProperties or LIMITED_DELEGATION in credProperties:
      return True

    return False

  types_persistOperation = [basestring]

  def export_persistOperation(self, opJSON):
    """ update or insert request into db

        :param opJSON: json string representing the operation

        :return: OperationID
    """

    opObj, _size = decode(opJSON)

    isAuthorized = FTS3ManagerHandler._isAllowed(opObj, self.getRemoteCredentials())

    if not isAuthorized:
      return S_ERROR(DErrno.ENOAUTH, "Credentials in the requests are not allowed")

    return self.fts3db.persistOperation(opObj)

  types_getOperation = [(long, int)]

  @classmethod
  def export_getOperation(cls, operationID):
    """ Get the FTS3Operation from the database

        :param operationID: ID of the operation

        :return: the FTS3Operation JSON string matching
    """
    getOperation = cls.fts3db.getOperation(operationID)
    if not getOperation["OK"]:
      gLogger.error("getOperation: %s" % getOperation["Message"])
      return getOperation

    getOperation = getOperation["Value"]
    opJSON = encode(getOperation)
    return S_OK(opJSON)

  types_getActiveJobs = [(long, int), [None] + [basestring], basestring]

  @classmethod
  def export_getActiveJobs(cls, limit, lastMonitor, jobAssignmentTag):
    """ Get all the FTSJobs that are not in a final state

        :param limit: max number of jobs to retrieve
        :param jobAssignmentTag: tag to put in the DB
        :param lastMonitor: jobs monitored earlier than the given date
        :return: json list of FTS3Job
    """

    res = cls.fts3db.getActiveJobs(
        limit=limit,
        lastMonitor=lastMonitor,
        jobAssignmentTag=jobAssignmentTag)
    if not res['OK']:
      return res

    activeJobs = res['Value']
    activeJobsJSON = encode(activeJobs)

    return S_OK(activeJobsJSON)

  types_getNonFinishedOperations = [(long, int), basestring]

  @classmethod
  def export_getNonFinishedOperations(cls, limit, operationAssignmentTag):
    """ Get all the FTS3Operations that are missing a callback, i.e.
        in 'Processed' state

        :param limit: max number of operations to retrieve
        :return: json list of FTS3Operation
    """

    res = cls.fts3db.getNonFinishedOperations(
        limit=limit, operationAssignmentTag=operationAssignmentTag)
    if not res['OK']:
      return res

    nonFinishedOperations = res['Value']
    nonFinishedOperationsJSON = encode(nonFinishedOperations)

    return S_OK(nonFinishedOperationsJSON)

  types_getOperationsFromRMSOpID = [(long, int)]

  @classmethod
  def export_getOperationsFromRMSOpID(cls, rmsOpID):
    """ Get the FTS3Operation associated to a given rmsOpID

        :param rmsOpID: ID of the operation in the RMS

        :return: JSON encoded list of FTS3Operations
    """
    res = cls.fts3db.getOperationsFromRMSOpID(rmsOpID)
    if not res["OK"]:
      gLogger.error("getOperationsFromRMSOpID: %s" % res["Message"])
      return res

    operations = res["Value"]
    opsJSON = encode(operations)

    return S_OK(opsJSON)
