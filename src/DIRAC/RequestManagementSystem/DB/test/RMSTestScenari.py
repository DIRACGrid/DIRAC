""" This contains a bunch of tests for the RMS that can be ran
either as unit test for the DB (Test_RequestDB.py) or as
integration tests (Test_ReqDB.py)
"""

# pylint: disable=invalid-name,wrong-import-position
import time


from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

from DIRAC.RequestManagementSystem.DB import RequestDB

STRESS_REQUESTS = 10
BULK_REQUESTS = 10


def test_stress(reqDB):
    """stress test"""

    reqIDs = []
    for i in range(STRESS_REQUESTS):
        request = Request({"RequestName": "test-%d" % i})
        op = Operation({"Type": "RemoveReplica", "TargetSE": "CERN-USER"})
        op += File({"LFN": "/lhcb/user/c/cibak/foo"})
        request += op
        put = reqDB.putRequest(request)
        assert put["OK"], put
        reqIDs.append(put["Value"])

    startTime = time.time()

    for reqID in reqIDs:
        get = reqDB.getRequest(reqID)
        assert get["OK"], get

    endTime = time.time()

    print("getRequest duration %s " % (endTime - startTime))
    for reqID in reqIDs:
        delete = reqDB.deleteRequest(reqID)
        assert delete["OK"], delete


def test_stressBulk(reqDB):
    """stress test bulk"""

    reqIDs = []
    for i in range(STRESS_REQUESTS):
        request = Request({"RequestName": "test-%d" % i})
        op = Operation({"Type": "RemoveReplica", "TargetSE": "CERN-USER"})
        op += File({"LFN": "/lhcb/user/c/cibak/foo"})
        request += op
        put = reqDB.putRequest(request)
        assert put["OK"], put
        reqIDs.append(put["Value"])

    loops = STRESS_REQUESTS // BULK_REQUESTS + (1 if (STRESS_REQUESTS % BULK_REQUESTS) else 0)
    totalSuccessful = 0

    time.sleep(1)
    startTime = time.time()

    for i in range(loops):
        get = reqDB.getBulkRequests(BULK_REQUESTS, True)
        assert get["OK"], get

        totalSuccessful += len(get["Value"])

    endTime = time.time()

    print("getRequests duration %s " % (endTime - startTime))

    assert totalSuccessful == STRESS_REQUESTS, "Did not retrieve all the requests: {} instead of {}".format(
        totalSuccessful,
        STRESS_REQUESTS,
    )

    for reqID in reqIDs:
        delete = reqDB.deleteRequest(reqID)
        assert delete["OK"], delete


def test_scheduled(reqDB):
    """scheduled request r/w"""

    req = Request({"RequestName": "FTSTest"})
    op = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op += File({"LFN": "/a/b/c", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32"})
    req += op

    put = reqDB.putRequest(req)
    assert put["OK"], put
    reqID = put["Value"]

    peek = reqDB.peekRequest(reqID)
    assert peek["OK"], peek

    peek = peek["Value"]
    for op in peek:
        opId = op.OperationID

    getFTS = reqDB.getScheduledRequest(opId)
    assert getFTS["OK"], getFTS
    assert getFTS["Value"].RequestName == "FTSTest", "Wrong request name %s" % getFTS["Value"].RequestName

    delete = reqDB.deleteRequest(reqID)
    assert delete["OK"], delete


def test_dirty(reqDB):
    """dirty records

    This illustrates this bug https://github.com/sqlalchemy/sqlalchemy/discussions/6294
    """
    req = Request()
    req.RequestName = "dirty"

    op1 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op1 += File({"LFN": "/a/b/c/1", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32"})

    op2 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CNAF-USER"})
    op2 += File({"LFN": "/a/b/c/2", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32"})

    op3 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "IN2P3-USER"})
    op3 += File({"LFN": "/a/b/c/3", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32"})

    req += op1
    req += op2
    req += op3

    put = reqDB.putRequest(req)
    assert put["OK"], put
    reqID = put["Value"]

    get = reqDB.getRequest(reqID)
    assert get["OK"], get
    req = get["Value"]

    del req[0]
    assert len(req) == 2, f"Wrong number of operations ({len(req)}) {req}"

    put = reqDB.putRequest(req)
    assert put["OK"], put
    reqID = put["Value"]

    get = reqDB.getRequest(reqID)
    assert get["OK"], get
    req = get["Value"]

    assert len(req) == 2, f"Wrong number of operations ({len(req)}) {req}"

    op4 = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op4 += File({"LFN": "/a/b/c/4", "Status": "Scheduled", "Checksum": "123456", "ChecksumType": "ADLER32"})

    req[0] = op4
    put = reqDB.putRequest(req)
    assert put["OK"], put
    reqID = put["Value"]

    get = reqDB.getRequest(reqID)
    assert get["OK"], get
    req = get["Value"]

    assert len(req) == 2, f"Wrong number of operations ({len(req)}) {req}"

    delete = reqDB.deleteRequest(reqID)
    assert delete["OK"], delete
