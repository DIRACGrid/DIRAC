import pytest

from sqlalchemy import engine, event, func, update
from sqlalchemy.orm import Session
from DIRAC import gLogger
from diraccfg import CFG
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.DataManagementSystem.DB import FTS3DB
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation, FTS3TransferOperation, FTS3StagingOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
import DIRAC.DataManagementSystem.DB.test.FTS3TestUtils as baseTestModule

gLogger.setLevel("DEBUG")


# pylint: disable=unsubscriptable-object
@pytest.fixture
def fts3db():
    FTS3DB.utc_timestamp = func.datetime
    FTS3DB.fts3FileTable.columns["lastUpdate"].onupdate = func.datetime
    FTS3DB.fts3JobTable.columns["lastUpdate"].onupdate = func.datetime
    FTS3DB.fts3OperationTable.columns["lastUpdate"].onupdate = func.datetime
    db = FTS3DB.FTS3DB(url="sqlite+pysqlite:///:memory:")

    @event.listens_for(engine.Engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        """Make sure that the foreign keys are checked
        See https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#foreign-key-support
        """
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    db.createTables()
    yield db
    # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
    # not to conflict with other tests that might be using a local dirac.cfg
    # Note that here we do not use it specifically, but the FTS3 objets
    # are doing it
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()


def test_raceCondition(fts3db):
    """This tests a race condition that was exhibited when
    running multiple agent in parallel. What was happening
    was that we were getting some nonFinishedOperations
    for further processing while some jobs associated to that
    operation were being monitored.

    This test reproduces all the possible combination of job/operation
    being assigned/non assigned

    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | OpID | OpAssigned | JobID | JobAssigned | Comment                                                                                  |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 1    |            |       |             | No job                                                                                   |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 2    | Yes        |       |             | No Job                                                                                   |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 3    |            | 1     |             | Nothing is Assigned                                                                      |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 4    |            | 2     | yes         | Job is assigned, so can't use the operation                                              |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 5    | yes        | 3     |             | Op is assigned, so can't use it                                                          |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 6    | yes        | 4     | yes         | That would be a problematic situation !!                                                 |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 7    |            | 5     | yes         | Job 5 is assigned, so Op 7 cannot be used, even if Job6 is unassigned (this was the bug) |
    |      |            | 6     |             |                                                                                          |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |
    | 8    | yes        | 7     | yes         | Op8 is assigned, so can't be used  (and is problematic like op6)                         |
    |      | yes        | 8     |             |                                                                                          |
    | ---- | ---------- | ----- | ----------- |:---------------------------------------------------------------------------------------- |

    Under these circumstances, we want:

    * getNonFinishedOperation to return operations 1 and 3
    * getActiveJobs to return jobs 1 and 6


    """

    # Utility to create a FT3File.
    # All operations must have at least one file associated
    # for the queries to make sense
    def _makeFile():
        f = FTS3File()
        f.targetSE = "targetSE"
        return f

    # op1: Non assigned operation without any job
    op1 = FTS3TransferOperation()
    op1.operationID = 1
    op1.ftsFiles.append(_makeFile())

    # op2: assigned operation without any job
    op2 = FTS3TransferOperation()
    op2.operationID = 2
    op2.ftsFiles.append(_makeFile())

    # op3: Non assigned operation with one non assigned job
    op3 = FTS3TransferOperation()
    op3.operationID = 3
    op3.ftsFiles.append(_makeFile())
    j1 = FTS3Job()
    j1.jobID = 1
    op3.ftsJobs.append(j1)

    # op4: Non assigned operation with one assigned job
    op4 = FTS3TransferOperation()
    op4.operationID = 4
    op4.ftsFiles.append(_makeFile())
    j2 = FTS3Job()
    j2.jobID = 2
    op4.ftsJobs.append(j2)

    # op5: assigned operation with one non assigned job
    op5 = FTS3TransferOperation()
    op5.operationID = 5
    op5.ftsFiles.append(_makeFile())
    j3 = FTS3Job()
    j3.jobID = 3
    op5.ftsJobs.append(j3)

    # op6: assigned operation with one assigned job
    # This is a very problematic case that we want
    # to avoid

    op6 = FTS3TransferOperation()
    op6.operationID = 6
    op6.ftsFiles.append(_makeFile())
    j4 = FTS3Job()
    j4.jobID = 4
    op6.ftsJobs.append(j4)

    # op7: Non assigned operation with one assigned job and one non assigned job
    op7 = FTS3TransferOperation()
    op7.operationID = 7
    op7.ftsFiles.append(_makeFile())
    j5 = FTS3Job()
    j5.jobID = 5
    op7.ftsJobs.append(j5)
    j6 = FTS3Job()
    op7.ftsFiles.append(_makeFile())
    j6.jobID = 6
    op7.ftsJobs.append(j6)

    # op8: assigned operation with one assigned job and one non assigned job
    # That is problematic, like op6
    op8 = FTS3TransferOperation()
    op8.operationID = 8
    j7 = FTS3Job()
    op8.ftsFiles.append(_makeFile())
    j7.jobID = 7
    op8.ftsJobs.append(j7)
    j8 = FTS3Job()
    j8.jobID = 8
    op8.ftsJobs.append(j8)

    allOps = [op1, op2, op3, op4, op5, op6, op7, op8]
    for op in allOps:
        res = fts3db.persistOperation(op)
        assert res["OK"]

    with fts3db.engine.begin() as conn:
        conn.execute(
            update(FTS3DB.fts3JobTable).values(assignment="Yes").where(FTS3DB.fts3JobTable.c.jobID.in_([2, 4, 5, 7]))
        )

    with fts3db.engine.begin() as conn:
        conn.execute(
            update(FTS3DB.fts3OperationTable)
            .values(assignment="Yes")
            .where(FTS3DB.fts3OperationTable.c.operationID.in_([2, 5, 6, 8]))
        )

    res = fts3db.getNonFinishedOperations(operationAssignmentTag=None)
    assert res["OK"]
    nonFinishedOps = res["Value"]
    nonFinishedOpsIDs = [op.operationID for op in nonFinishedOps]
    assert nonFinishedOpsIDs == [1, 3]

    res = fts3db.getActiveJobs(jobAssignmentTag=None)
    assert res["OK"]
    activeJobs = res["Value"]
    activeJobIDs = [op.jobID for op in activeJobs]
    assert activeJobIDs == [1, 6]


@pytest.mark.parametrize("baseTest", baseTestModule.allBaseTests)
def test_all_common_tests(fts3db, baseTest):
    """Run all the tests in the FTS3TestUtils."""
    baseTest(fts3db, fts3db)
