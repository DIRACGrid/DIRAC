""" This is a test of the chain
    FTS3Client -> FTS3ManagerHandler -> FTS3DB

    It supposes that the DB is present, and that the service is running
"""
import unittest
import time
import sys
import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

import DIRAC.DataManagementSystem.DB.test.FTS3TestUtils as baseTestModule
from DIRAC.DataManagementSystem.Client.FTS3Client import FTS3Client
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation, FTS3TransferOperation, FTS3StagingOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job

from DIRAC.DataManagementSystem.DB.FTS3DB import FTS3DB


# pylint: disable=unsubscriptable-object
@pytest.fixture
def fts3db():
    yield FTS3DB()


@pytest.fixture
def fts3Client():
    yield FTS3Client()


def test_cancelNotFoundJob(fts3db, fts3Client):
    """When a job disappears from the server, we need to cancel it
    and its files.

    The scenario is as follow. Operation has 4 files.
    Job1 is submitted for File1 and File2.
    Job2 is submitted for File3 and File4.
    File1 is finished, and then the job disappears.
    We need to cancel Job1 and File2.
    Job2, File3 and File4 are here to make sure we do not cancel wrongly other files

    Note: this test is not in the common tests because SQLte does not play nice with
    multiple table update, which is what is done in cancelNonExistingJob
    """

    op = baseTestModule.generateOperation("Transfer", 4, ["Target1"])

    job1 = FTS3Job()
    job1GUID = "05-cancelall-job1"
    job1.ftsGUID = job1GUID
    job1.ftsServer = "fts3"

    job1.username = op.username
    job1.userGroup = op.userGroup

    # assign the GUID to the files
    op.ftsFiles[0].ftsGUID = job1GUID
    op.ftsFiles[1].ftsGUID = job1GUID

    # Pretend

    op.ftsJobs.append(job1)

    job2 = FTS3Job()
    job2GUID = "05-cancelall-job2"
    job2.ftsGUID = job2GUID
    job2.ftsServer = "fts3"

    job2.username = op.username
    job2.userGroup = op.userGroup

    # assign the GUID to the files
    op.ftsFiles[2].ftsGUID = job2GUID
    op.ftsFiles[3].ftsGUID = job2GUID

    op.ftsJobs.append(job2)

    res = fts3db.persistOperation(op)
    opID = res["Value"]

    # Get back the operation to update all the IDs
    res = fts3db.getOperation(opID)
    op = res["Value"]

    fileIds = []
    for ftsFile in op.ftsFiles:
        fileIds.append(ftsFile.fileID)

    # Now we monitor Job1, and find that the first file has failed, the second is still ongoing
    # And since File1 is in an FTS final status, we set its ftsGUID to None
    file1ID = op.ftsFiles[0].fileID
    file2ID = op.ftsFiles[1].fileID
    fileStatusDict = {file1ID: {"status": "Finished", "ftsGUID": None}, file2ID: {"status": "Staging"}}

    # And when updating, take care of specifying that you are updating for a given GUID
    res = fts3db.updateFileStatus(fileStatusDict, ftsGUID=job1GUID)
    assert res["OK"]

    # Now we monitor again, job one, and find out that job1 has disappeared
    # So we cancel the job and the files
    res = fts3db.cancelNonExistingJob(opID, job1GUID)
    assert res["OK"]

    # And hopefully now File2 is Canceled, while the others are as they were
    res = fts3Client.getOperation(opID)
    op = res["Value"]

    assert op.ftsFiles[0].status == "Finished"
    assert op.ftsFiles[1].status == "Canceled"
    assert op.ftsFiles[1].ftsGUID is None
    assert op.ftsFiles[2].status == "New"
    assert op.ftsFiles[3].status == "New"


@pytest.mark.parametrize("baseTest", baseTestModule.allBaseTests)
def test_all_common_tests(baseTest, fts3db, fts3Client):
    """Run all the tests in the FTS3TestUtils."""
    baseTest(fts3db, fts3Client)
