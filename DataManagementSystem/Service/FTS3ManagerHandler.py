"""
:mod: FTS3ManagerHandler

.. module: FTS3ManagerHandler
  :synopsis: handler for FTS3DB using DISET

Service handler for FT3SDB using DISET
"""

__RCSID__ = "$Id$"

import json
from types import DictType, IntType, LongType, ListType, StringTypes, NoneType

# from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RequestHandler                    import RequestHandler


from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB
from DIRAC.DataManagementSystem.private.FTS3Utilities import FTS3JSONEncoder, FTS3JSONDecoder

########################################################################
class FTS3ManagerHandler( RequestHandler ):
  """
  .. class:: FTS3ManagerHandler

  """

  fts3db = None
  ftsValidator = None
  ftsPlacement = None

  @classmethod
  def initializeHandler( cls, _serviceInfoDict ):
    """ initialize handler """
    try:
      cls.fts3db = FTS3DB()
    except RuntimeError, error:
      gLogger.exception( error )
      return S_ERROR( error )

    # # create tables for empty db
    return cls.fts3db.createTables()




  types_persistOperation = [ StringTypes ]
  @classmethod
  def export_persistOperation( cls, opJSON ):
    """ update or insert request into db
        :param opJSON: json string representing the operation

        :return: OperationID
    """

    opObj = json.loads( opJSON, cls = FTS3JSONDecoder )
    return cls.fts3db.persistOperation( opObj )


  types_getOperation = [ ( LongType, IntType ) ]
  @classmethod
  def export_getOperation( cls, operationID ):
    """ Get the FTS3Operation from the database

        :param operationID: ID of the operation

        :return: the FTS3Operation JSON string matching
    """
    getOperation = cls.fts3db.getOperation( operationID )
    if not getOperation["OK"]:
      gLogger.error( "getOperation: %s" % getOperation["Message"] )
      return getOperation

    getOperation = getOperation["Value"]
    opJSON = getOperation.toJSON()
    return S_OK( opJSON )


  types_getActiveJobs = [ ( LongType, IntType ), [NoneType ] + list( StringTypes ), StringTypes ]
  @classmethod
  def export_getActiveJobs( cls, limit, lastMonitor, jobAssignmentTag ):
    """ Get all the FTSJobs that are not in a final state
        :param limit: max number of jobs to retrieve
        :param jobAssignmentTag: tag to put in the DB
        :param lastMonitor: jobs monitored earlier than the given date
        :return: json list of FTS3Job
    """

    res = cls.fts3db.getActiveJobs( limit = limit, lastMonitor = lastMonitor, jobAssignmentTag = jobAssignmentTag )
    if not res['OK']:
      return res

    activeJobs = res['Value']
    activeJobsJSON = json.dumps( activeJobs, cls = FTS3JSONEncoder )

    return S_OK( activeJobsJSON )


  types_updateFileStatus = [ DictType ]
  @classmethod
  def export_updateFileStatus( cls, fileStatusDict ):
    """ Update the file ftsStatus and error
       :param fileStatusDict : { fileID : { status , error } }
    """

    return cls.fts3db.updateFileStatus( fileStatusDict )

  types_updateJobStatus = [ DictType ]
  @classmethod
  def export_updateJobStatus( cls, jobStatusDict ):
    """ Update the job Status and error
       :param jobStatusDict : { jobID : { status , error } }
    """

    return cls.fts3db.updateJobStatus( jobStatusDict )

#   types_getProcessedOperations = [( LongType, IntType )]
#   @classmethod
#   def export_getProcessedOperations( cls, limit ):
#     """ Get all the FTS3Operations that are missing a callback, i.e.
#         in 'Processed' state
#         :param limit: max number of operations to retrieve
#         :return: json list of FTS3Operation
#     """
#
#     res = cls.fts3db.getProcessedOperations( limit = limit )
#     if not res['OK']:
#       return res
#
#     processedOperations = res['Value']
#     processedOperationsJSON = json.dumps( processedOperations, cls = FTS3JSONEncoder )
#
#     return S_OK( processedOperationsJSON )

  types_getNonFinishedOperations = [( LongType, IntType ), StringTypes]
  @classmethod
  def export_getNonFinishedOperations( cls, limit, operationAssignmentTag ):
    """ Get all the FTS3Operations that are missing a callback, i.e.
        in 'Processed' state
        :param limit: max number of operations to retrieve
        :return: json list of FTS3Operation
    """

    res = cls.fts3db.getNonFinishedOperations( limit = limit, operationAssignmentTag = operationAssignmentTag )
    if not res['OK']:
      return res

    nonFinishedOperations = res['Value']
    nonFinishedOperationsJSON = json.dumps( nonFinishedOperations, cls = FTS3JSONEncoder )

    return S_OK( nonFinishedOperationsJSON )


#   types_getOperationsWithFilesToSubmit = [( LongType, IntType )]
#   @classmethod
#   def export_getOperationsWithFilesToSubmit( cls, limit ):
#     """ Get all the FTS3Operations that have files in New or Failed state
#         (reminder: Failed is NOT terminal for files. Failed is when fts failed, but we
#          can retry)
#         :param limit: max number of operation to retrieve
#         :return: json list of FTS3Operation
#     """
#
#     res = cls.fts3db.getOperationsWithFilesToSubmit( limit = limit )
#     if not res['OK']:
#       return res
#
#     operations = res['Value']
#     operationsJSON = json.dumps( operations, cls = FTS3JSONEncoder )
#
#     return S_OK( operationsJSON )
