"""
We define here some tests that are meant to be ran as unit tests
against an sqlite DB and as integration tests against a MySQL DB
"""
import random

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation, FTS3TransferOperation, FTS3StagingOperation
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job


def generateOperation(opType, nbFiles, dests, sources=None):
    """Generate one FTS3Operation object with FTS3Files in it"""
    op = None
    if opType == "Transfer":
        op = FTS3TransferOperation()
    elif opType == "Staging":
        op = FTS3StagingOperation()
    # Get the username and group from the proxy
    # if we are in integration test
    try:
        proxyInfo = getProxyInfo()["Value"]
        op.username = proxyInfo["username"]
        op.userGroup = proxyInfo["group"]
    except:
        op.username = "username"
        op.userGroup = "group"
    op.sourceSEs = str(sources)
    for _i in range(nbFiles * len(dests)):
        for dest in dests:
            ftsFile = FTS3File()
            ftsFile.lfn = f"lfn{random.randint(0,100)}"
            ftsFile.targetSE = dest
            op.ftsFiles.append(ftsFile)

    return op


def base_test_operation(fts3db, fts3Client):
    """
    Run basic operation tests

    :param fts3db: an FTS3DB
    :param fts3Client: possibly an FTS3Client (integration test) or the same fts3db
    """
    op = generateOperation("Transfer", 3, ["Target1", "Target2"], sources=["Source1", "Source2"])
    assert not op.isTotallyProcessed()

    res = fts3Client.persistOperation(op)
    assert res["OK"], res
    opID = res["Value"]

    res = fts3Client.getOperation(opID)
    assert res["OK"]

    op2 = res["Value"]

    assert isinstance(op2, FTS3TransferOperation)
    assert not op2.isTotallyProcessed()

    for attr in ["username", "userGroup", "sourceSEs"]:
        assert getattr(op, attr) == getattr(op2, attr)

    assert len(op.ftsFiles) == len(op2.ftsFiles)

    assert op2.status == FTS3Operation.INIT_STATE

    fileIds = []
    for ftsFile in op2.ftsFiles:
        fileIds.append(ftsFile.fileID)
        assert ftsFile.status == FTS3File.INIT_STATE

    # Testing updating the status and error
    fileStatusDict = {}
    for fId in fileIds:
        fileStatusDict[fId] = {
            "status": "Finished" if fId % 2 else "Failed",
            "error": "" if fId % 2 else "Tough luck",
        }

    res = fts3db.updateFileStatus(fileStatusDict)
    assert res["OK"]

    res = fts3Client.getOperation(opID)
    op3 = res["Value"]
    assert res["OK"]

    assert op3.ftsFiles
    for ftsFile in op3.ftsFiles:
        if ftsFile.fileID % 2:
            assert ftsFile.status == "Finished"
            assert not ftsFile.error
        else:
            assert ftsFile.status == "Failed"
            assert ftsFile.error == "Tough luck"

    assert not op3.isTotallyProcessed()

    # Testing updating only the status and to final states
    fileStatusDict = {}
    nbFinalStates = len(FTS3File.FINAL_STATES)
    for fId in fileIds:
        fileStatusDict[fId] = {"status": FTS3File.FINAL_STATES[fId % nbFinalStates]}

    res = fts3db.updateFileStatus(fileStatusDict)
    assert res["OK"]

    res = fts3Client.getOperation(opID)
    op4 = res["Value"]
    assert res["OK"]

    assert op4.ftsFiles
    for ftsFile in op4.ftsFiles:
        if ftsFile.fileID % 2:
            # Files to finished cannot be changed
            assert ftsFile.status == "Finished"
            assert not ftsFile.error
        else:
            assert ftsFile.status == FTS3File.FINAL_STATES[ftsFile.fileID % nbFinalStates]
            assert ftsFile.error == "Tough luck"

    # Now it should be considered as totally processed
    assert op4.isTotallyProcessed()
    res = fts3Client.persistOperation(op4)


def base_test_job(fts3db, fts3Client):
    """
    Run basic Job tests

    :param fts3db: an FTS3DB
    :param fts3Client: possibly an FTS3Client (integration test) or the same fts3db
    """
    op = generateOperation("Transfer", 3, ["Target1", "Target2"], sources=["Source1", "Source2"])

    job1 = FTS3Job()
    job1.ftsGUID = "a-random-guid"
    job1.ftsServer = "fts3"

    job1.username = op.username
    job1.userGroup = op.userGroup

    op.ftsJobs.append(job1)

    res = fts3Client.persistOperation(op)
    assert res["OK"], res
    opID = res["Value"]

    res = fts3Client.getOperation(opID)
    assert res["OK"]

    op2 = res["Value"]
    assert len(op2.ftsJobs) == 1
    job2 = op2.ftsJobs[0]
    assert job2.operationID == opID

    for attr in ["ftsGUID", "ftsServer", "username", "userGroup"]:
        assert getattr(job1, attr) == getattr(job2, attr)


def base_test_job_monitoring_racecondition(fts3db, fts3Client):
    """We used to have a race condition resulting in duplicated transfers for a file.
    This test reproduces the race condition.

    The scenario is as follow. Operation has two files File1 and File2.
    Job1 is submitted for File1 and File2.
    File1 fails, File2 is still ongoing.
    We submit Job2 for File1.
    Job1 is monitored again, and we update again File1 to failed (because it is so in Job1)
    A Job3 would be created for File1, despite Job2 still running on it.
    """
    op = generateOperation("Transfer", 2, ["Target1"])

    job1 = FTS3Job()
    job1.ftsGUID = "03-racecondition-job1"
    job1.ftsServer = "fts3"

    job1.username = op.username
    job1.userGroup = op.userGroup

    op.ftsJobs.append(job1)

    res = fts3Client.persistOperation(op)
    opID = res["Value"]

    # Get back the operation to update all the IDs
    res = fts3Client.getOperation(opID)
    op = res["Value"]

    fileIds = []
    for ftsFile in op.ftsFiles:
        fileIds.append(ftsFile.fileID)

    file1ID = min(fileIds)
    file2ID = max(fileIds)

    # Now we monitor Job1, and find that the first file has failed, the second is still ongoing
    fileStatusDict = {
        file1ID: {"status": "Failed", "error": "Someone made a boo-boo"},
        file2ID: {"status": "Staging"},
    }

    res = fts3db.updateFileStatus(fileStatusDict)
    assert res["OK"]

    # We would then submit a second job
    job2 = FTS3Job()
    job2.ftsGUID = "03-racecondition-job2"
    job2.ftsServer = "fts3"

    job2.username = op.username
    job2.userGroup = op.userGroup

    op.ftsJobs.append(job2)
    res = fts3Client.persistOperation(op)

    # Now we monitor Job2 & Job1 (in this order)
    fileStatusDictJob2 = {
        file1ID: {"status": "Staging"},
    }
    res = fts3db.updateFileStatus(fileStatusDictJob2)
    assert res["OK"]

    # And in Job1, File1 is (and will remain) failed, while File2 is still ongoing
    fileStatusDictJob1 = {
        file1ID: {"status": "Failed", "error": "Someone made a boo-boo"},
        file2ID: {"status": "Staging"},
    }
    res = fts3db.updateFileStatus(fileStatusDictJob1)
    assert res["OK"]

    # And now this is the problem, because If we check whether this operation still has
    # files to submit, it will tell me yes, while all the files are being taken care of
    res = fts3Client.getOperation(opID)
    op = res["Value"]

    # isTotallyProcessed does not return S_OK struct
    filesToSubmit = op._getFilesToSubmit()
    assert filesToSubmit == [op.ftsFiles[0]]


def base_test_job_monitoring_solve_racecondition(fts3db, fts3Client):
    """We used to have a race condition resulting in duplicated transfers for a file.
    This test reproduces the race condition to make sure it is fixed.
    This test makes sure that the update only happens on files concerned by the job

    The scenario is as follow. Operation has two files File1 and File2.
    Job1 is submitted for File1 and File2.
    File1 fails, File2 is still ongoing.
    We submit Job2 for File1.
    Job1 is monitored again, and we update again File1 to failed (because it is so in Job1)
    A Job3 would be created for File1, dispite Job2 still runing on it.
    """
    op = generateOperation("Transfer", 2, ["Target1"])

    job1 = FTS3Job()
    job1GUID = "04-racecondition-job1"
    job1.ftsGUID = job1GUID
    job1.ftsServer = "fts3"

    job1.username = op.username
    job1.userGroup = op.userGroup

    op.ftsJobs.append(job1)

    # Now, when submitting the job, we specify the ftsGUID to which files are
    # assigned
    for ftsFile in op.ftsFiles:
        ftsFile.ftsGUID = job1GUID

    res = fts3Client.persistOperation(op)
    opID = res["Value"]

    # Get back the operation to update all the IDs
    res = fts3Client.getOperation(opID)
    op = res["Value"]

    fileIds = []
    for ftsFile in op.ftsFiles:
        fileIds.append(ftsFile.fileID)

    # Arbitrarilly decide that File1 has the smalled fileID
    file1ID = min(fileIds)
    file2ID = max(fileIds)

    # Now we monitor Job1, and find that the first file has failed, the second is still ongoing
    # And since File1 is in an FTS final status, we set its ftsGUID to None
    fileStatusDict = {
        file1ID: {"status": "Failed", "error": "Someone made a boo-boo", "ftsGUID": None},
        file2ID: {"status": "Staging"},
    }

    # And when updating, take care of specifying that you are updating for a given GUID
    res = fts3db.updateFileStatus(fileStatusDict, ftsGUID=job1GUID)
    assert res["OK"]

    # We would then submit a second job
    job2 = FTS3Job()
    job2GUID = "04-racecondition-job2"
    job2.ftsGUID = job2GUID
    job2.ftsServer = "fts3"

    job2.username = op.username
    job2.userGroup = op.userGroup

    op.ftsJobs.append(job2)

    # And do not forget to add the new FTSGUID to File1
    # assigned
    for ftsFile in op.ftsFiles:
        if ftsFile.fileID == file1ID:
            ftsFile.ftsGUID = job2GUID

    res = fts3Client.persistOperation(op)

    # Now we monitor Job2 & Job1 (in this order)
    fileStatusDictJob2 = {
        file1ID: {"status": "Staging"},
    }

    # Again specify the GUID
    res = fts3db.updateFileStatus(fileStatusDictJob2, ftsGUID=job2GUID)
    assert res["OK"]

    # And in Job1, File1 is (and will remain) failed, while File2 is still ongoing
    fileStatusDictJob1 = {
        file1ID: {"status": "Failed", "error": "Someone made a boo-boo"},
        file2ID: {"status": "Staging"},
    }

    # And thanks to specifying the job GUID, File1 should not be touched !
    res = fts3db.updateFileStatus(fileStatusDictJob1, ftsGUID=job1GUID)
    assert res["OK"]

    # And hopefully now there shouldn't be any file to submit
    res = fts3Client.getOperation(opID)
    op = res["Value"]

    # isTotallyProcessed does not return S_OK struct
    filesToSubmit = op._getFilesToSubmit()
    assert not filesToSubmit


def base_test_delete_operations(fts3db, fts3Client):
    """Test operation removals"""
    op1 = generateOperation("Transfer", 2, ["Target1"])

    res = fts3Client.persistOperation(op1)
    opID1 = res["Value"]

    # Create two other operations, to test the limit feature
    op2 = generateOperation("Transfer", 2, ["Target2"])
    res = fts3Client.persistOperation(op2)
    opID2 = res["Value"]

    op3 = generateOperation("Transfer", 2, ["Target3"])
    res = fts3Client.persistOperation(op3)
    opID3 = res["Value"]

    # Now, call delete, and make sure that operation is not delete
    # Ops is not in a final state, and delay is not passed
    res = fts3db.deleteFinalOperations()
    assert res["OK"]

    res = fts3Client.getOperation(opID1)
    assert res["OK"]
    op1 = res["Value"]

    # Try again with no delay, but still not final state
    res = fts3db.deleteFinalOperations(deleteDelay=0)
    assert res["OK"]

    res = fts3Client.getOperation(opID1)
    assert res["OK"]
    op1 = res["Value"]

    # Set the final status
    op1.status = "Finished"
    res = fts3Client.persistOperation(op1)
    assert res["OK"]

    # Now try to delete again.
    # It should still not work because of the delay
    res = fts3db.deleteFinalOperations()
    assert res["OK"]

    res = fts3Client.getOperation(opID1)
    assert res["OK"]
    op1 = res["Value"]

    # Finally, it should work, with no delay and a final status
    res = fts3db.deleteFinalOperations(deleteDelay=0)
    assert res["OK"]

    res = fts3Client.getOperation(opID1)
    assert not res["OK"]

    # op2 and op3 should still be here though !

    res = fts3Client.getOperation(opID2)
    assert res["OK"]
    op2 = res["Value"]
    res = fts3Client.getOperation(opID3)
    assert res["OK"]
    op3 = res["Value"]

    # Set them both to a final status
    op2.status = "Finished"
    res = fts3Client.persistOperation(op2)
    assert res["OK"]
    op3.status = "Finished"
    res = fts3Client.persistOperation(op3)
    assert res["OK"]

    # Now try to delete, but only one
    res = fts3db.deleteFinalOperations(limit=1, deleteDelay=0)
    assert res["OK"]

    # Now only op2 or op3 should be here

    res2 = fts3Client.getOperation(opID2)
    res3 = fts3Client.getOperation(opID3)

    assert res2["OK"] ^ res3["OK"]


# All base tests defined in this module
allBaseTests = [test_func for testName, test_func in globals().items() if testName.startswith("base_test")]
