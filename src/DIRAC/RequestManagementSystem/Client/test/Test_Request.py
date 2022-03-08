""" :mod: RequestTests
    =======================

    .. module: RequestTests
    :synopsis: test cases for Request class
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    test cases for Request class
"""

import datetime

from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.ReqClient import printRequest


def optimizeRequest(req, printOutput=None):
    from DIRAC import gLogger

    if printOutput:
        if isinstance(printOutput, str):
            gLogger.always("Request %s:" % printOutput)
        printRequest(req)
        gLogger.always("=========== Optimized ===============")
    res = req.optimize()
    if printOutput:
        printRequest(req)
        gLogger.always("")
    return res


def createRequest(reqType):
    r = Request()

    # Simple failover
    op1 = Operation()
    f = File()
    f.LFN = "/This/is/an/LFN"
    op1.addFile(f)
    op1.Type = "ReplicateAndRegister"
    op1.SourceSE = "CERN-FAILOVER"
    op1.TargetSE = "CERN-BUFFER"
    r.addOperation(op1)

    # You cannot reuse the same File object,
    # since it is a different entry in the DB
    fr = File()
    fr.LFN = "/This/is/an/LFN"
    op2 = Operation()
    op2.addFile(fr)
    op2.Type = "RemoveReplica"
    op2.TargetSE = "CERN-FAILOVER"
    r.addOperation(op2)
    if reqType == 0:
        return r

    # two files for Failover
    f1 = File()
    f1.LFN = "/This/is/a/second/LFN"
    op3 = Operation()
    op3.addFile(f1)
    op3.Type = "ReplicateAndRegister"
    op3.SourceSE = "CERN-FAILOVER"
    op3.TargetSE = "CERN-BUFFER"
    r.addOperation(op3)

    f1r = File()
    f1r.LFN = "/This/is/a/second/LFN"
    op3 = Operation()
    op3.addFile(f1r)
    op3.Type = "RemoveReplica"
    op3.TargetSE = "CERN-FAILOVER"
    r.addOperation(op3)
    if reqType == 1:
        return r

    op = Operation()
    op.Type = "ForwardDiset"
    if reqType == 2:
        r.addOperation(op)
        return r

    r.insertBefore(op, r[0])
    if reqType == 3:
        return r

    op4 = Operation()
    op4.Type = "ForwardDiset"
    r.addOperation(op4)
    if reqType == 4:
        return r

    # 2 different FAILOVER SEs: removal not optimized
    r[1].SourceSE = "RAL-FAILOVER"
    r[2].SourceSE = "RAL-FAILOVER"
    if reqType == 5:
        return r

    # 2 different destinations, same FAILOVER: replication not optimized
    r[3].SourceSE = "RAL-FAILOVER"
    r[4].SourceSE = "RAL-FAILOVER"
    r[3].TargetSE = "RAL-BUFFER"
    if reqType == 6:
        return r

    print("This should not happen, reqType =", reqType)


########################################################################


def test_CtorSerilization():
    """c'tor and serialization"""
    req = Request()
    assert isinstance(req, Request)
    assert req.JobID == 0
    assert req.Status == "Waiting"

    req = Request({"RequestName": "test", "JobID": 12345})
    assert isinstance(req, Request)
    assert req.RequestName == "test"
    assert req.JobID == 12345
    assert req.Status == "Waiting"

    req.SourceComponent = "test component"
    assert req.SourceComponent == b"test component"

    toJSON = req.toJSON()
    assert toJSON["OK"], "JSON serialization failed"

    fromJSON = toJSON["Value"]
    req = Request(fromJSON)


def test_Props():
    """props"""
    # # valid values
    req = Request()

    req.RequestID = 1
    assert req.RequestID == 1

    req.RequestName = "test"
    assert req.RequestName == "test"

    req.JobID = 1
    assert req.JobID == 1

    req.CreationTime = "1970-01-01 00:00:00"
    assert req.CreationTime == datetime.datetime(1970, 1, 1, 0, 0, 0)
    req.CreationTime = datetime.datetime(1970, 1, 1, 0, 0, 0)
    assert req.CreationTime == datetime.datetime(1970, 1, 1, 0, 0, 0)

    req.SubmitTime = "1970-01-01 00:00:00"
    assert req.SubmitTime == datetime.datetime(1970, 1, 1, 0, 0, 0)
    req.SubmitTime = datetime.datetime(1970, 1, 1, 0, 0, 0)
    assert req.SubmitTime == datetime.datetime(1970, 1, 1, 0, 0, 0)

    req.LastUpdate = "1970-01-01 00:00:00"
    assert req.LastUpdate == datetime.datetime(1970, 1, 1, 0, 0, 0)
    req.LastUpdate = datetime.datetime(1970, 1, 1, 0, 0, 0)
    assert req.LastUpdate == datetime.datetime(1970, 1, 1, 0, 0, 0)

    req.Error = ""


def test_Operations():
    """operations arithmetic and state machine"""
    req = Request()
    assert len(req) == 0

    transfer = Operation()
    transfer.Type = "ReplicateAndRegister"
    transfer.addFile(File({"LFN": "/a/b/c", "Status": "Waiting"}))

    getWaiting = req.getWaiting()
    assert getWaiting["OK"]
    assert getWaiting["Value"] is None

    req.addOperation(transfer)
    assert len(req) == 1
    assert transfer.Order == req.Order
    assert transfer.Status == "Waiting"

    getWaiting = req.getWaiting()
    assert getWaiting["OK"]
    assert getWaiting["Value"] == transfer

    removal = Operation({"Type": "RemoveFile"})
    removal.addFile(File({"LFN": "/a/b/c", "Status": "Waiting"}))

    req.insertBefore(removal, transfer)

    getWaiting = req.getWaiting()
    assert getWaiting["OK"]
    assert getWaiting["Value"] == removal

    assert len(req) == 2
    assert [op.Status for op in req] == ["Waiting", "Queued"]
    assert req.subStatusList() == ["Waiting", "Queued"]

    assert removal.Order == 0
    assert removal.Order == req.Order

    assert transfer.Order == 1

    assert removal.Status == "Waiting"
    assert transfer.Status == "Queued"

    for subFile in removal:
        subFile.Status = "Done"
    removal.Status = "Done"

    assert removal.Status == "Done"

    assert transfer.Status == "Waiting"
    assert transfer.Order == req.Order

    # len, looping
    assert len(req) == 2
    assert [op.Status for op in req] == ["Done", "Waiting"]
    assert req.subStatusList() == ["Done", "Waiting"]

    digest = req.toJSON()
    assert digest["OK"]

    getWaiting = req.getWaiting()
    assert getWaiting["OK"]
    assert getWaiting["Value"] == transfer


def test_FTS():
    """FTS state machine"""

    req = Request()
    req.RequestName = "FTSTest"

    ftsTransfer = Operation()
    ftsTransfer.Type = "ReplicateAndRegister"
    ftsTransfer.TargetSE = "CERN-USER"

    ftsFile = File()
    ftsFile.LFN = "/a/b/c"
    ftsFile.Checksum = "123456"
    ftsFile.ChecksumType = "Adler32"

    ftsTransfer.addFile(ftsFile)
    req.addOperation(ftsTransfer)

    assert req.Status == "Waiting", "1. wrong request status: %s" % req.Status
    assert ftsTransfer.Status == "Waiting", "1. wrong ftsStatus status: %s" % ftsTransfer.Status

    # # scheduled
    ftsFile.Status = "Scheduled"

    assert ftsTransfer.Status == "Scheduled", "2. wrong status for ftsTransfer: %s" % ftsTransfer.Status
    assert req.Status == "Scheduled", "2. wrong status for request: %s" % req.Status

    # # add new operation before FTS
    insertBefore = Operation()
    insertBefore.Type = "RegisterReplica"
    insertBefore.TargetSE = "CERN-USER"
    insertFile = File()
    insertFile.LFN = "/a/b/c"
    insertFile.PFN = "http://foo/bar"
    insertBefore.addFile(insertFile)
    req.insertBefore(insertBefore, ftsTransfer)

    assert insertBefore.Status == "Waiting", "3. wrong status for insertBefore: %s" % insertBefore.Status
    assert ftsTransfer.Status == "Scheduled", "3. wrong status for ftsStatus: %s" % ftsTransfer.Status
    assert req.Status == "Waiting", "3. wrong status for request: %s" % req.Status

    # # prev done
    insertFile.Status = "Done"

    assert insertBefore.Status == "Done", "4. wrong status for insertBefore: %s" % insertBefore.Status
    assert ftsTransfer.Status == "Scheduled", "4. wrong status for ftsStatus: %s" % ftsTransfer.Status
    assert req.Status == "Scheduled", "4. wrong status for request: %s" % req.Status

    # # reschedule
    ftsFile.Status = "Waiting"

    assert insertBefore.Status == "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status
    assert ftsTransfer.Status == "Waiting", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status
    assert req.Status == "Waiting", "5. wrong status for request: %s" % req.Status

    # # fts done
    ftsFile.Status = "Done"

    assert insertBefore.Status == "Done", "5. wrong status for insertBefore: %s" % insertBefore.Status
    assert ftsTransfer.Status == "Done", "5. wrong status for ftsStatus: %s" % ftsTransfer.Status
    assert req.Status == "Done", "5. wrong status for request: %s" % req.Status


def test_StateMachine():
    """state machine tests"""
    r = Request({"RequestName": "SMT"})
    assert r.Status == "Waiting", "1. wrong status %s" % r.Status

    r.addOperation(Operation({"Status": "Queued"}))
    assert r.Status == "Waiting", "2. wrong status %s" % r.Status

    r.addOperation(Operation({"Status": "Queued"}))
    assert r.Status == "Waiting", "3. wrong status %s" % r.Status

    r[0].Status = "Done"
    assert r.Status == "Waiting", "4. wrong status %s" % r.Status

    r[1].Status = "Done"
    assert r.Status == "Done", "5. wrong status %s" % r.Status

    r[0].Status = "Failed"
    assert r.Status == "Failed", "6. wrong status %s" % r.Status

    r[0].Status = "Queued"
    assert r.Status == "Waiting", "7. wrong status %s" % r.Status

    r.insertBefore(Operation({"Status": "Queued"}), r[0])
    assert r.Status == "Waiting", "8. wrong status %s" % r.Status

    r.insertBefore(Operation({"Status": "Queued"}), r[0])
    assert r.Status == "Waiting", "9. wrong status %s" % r.Status

    r.insertBefore(Operation({"Status": "Scheduled"}), r[0])
    assert r.Status == "Scheduled", "10. wrong status %s" % r.Status

    r.insertBefore(Operation({"Status": "Queued"}), r[0])
    assert r.Status == "Waiting", "11. wrong status %s" % r.Status

    r[0].Status = "Failed"
    assert r.Status == "Failed", "12. wrong status %s" % r.Status

    r[0].Status = "Done"
    assert r.Status == "Scheduled", "13. wrong status %s" % r.Status

    r[1].Status = "Failed"
    assert r.Status == "Failed", "14. wrong status %s" % r.Status

    r[1].Status = "Done"
    assert r.Status == "Waiting", "15. wrong status %s" % r.Status

    r[2].Status = "Scheduled"
    assert r.Status == "Scheduled", "16. wrong status %s" % r.Status

    r[2].Status = "Queued"
    assert r.Status == "Waiting", "17. wrong status %s" % r.Status

    r[2].Status = "Scheduled"
    assert r.Status == "Scheduled", "18. wrong status %s" % r.Status

    r = Request()
    for _ in range(5):
        r.addOperation(Operation({"Status": "Queued"}))

    r[0].Status = "Done"
    assert r.Status == "Waiting", "19. wrong status %s" % r.Status

    r[1].Status = "Done"
    assert r.Status == "Waiting", "20. wrong status %s" % r.Status

    r[2].Status = "Scheduled"
    assert r.Status == "Scheduled", "21. wrong status %s" % r.Status

    r[2].Status = "Done"
    assert r.Status == "Waiting", "22. wrong status %s" % r.Status


def test_List():
    """setitem, delitem, getitem and dirty"""

    r = Request()

    ops = [Operation() for i in range(5)]

    for op in ops:
        r.addOperation(op)

    for i, op in enumerate(ops):
        assert op == r[i], "__getitem__ failed"

    op = Operation()
    r[0] = op
    assert op == r[0], "__setitem__ failed"

    del r[0]
    assert len(r) == 4, "__delitem__ failed"


def test_Optimize():
    title = {
        0: "Simple Failover",
        1: "Double Failover",
        2: "Double Failover + ForwardDiset",
        3: "ForwardDiset + Double Failover",
        4: "ForwardDiset + Double Failover + ForwardDiset",
        5: "ForwardDiset + Double Failover (# Failover SE) + ForwardDiset",
        6: "ForwardDiset + Double Failover (# Destination SE) + ForwardDiset",
    }
    debug = False
    if debug:
        print("")
    for reqType in title:
        r = createRequest(reqType)
        res = optimizeRequest(r, printOutput=title[reqType] if (debug and debug == reqType) else False)
        assert res["OK"]
        assert res["Value"]
        if reqType in (0, 1):
            assert len(r) == 2, "Wrong number of operations: %d" % len(r)
            assert r[0].Type == "ReplicateAndRegister"
            assert r[1].Type == "RemoveReplica"
        if reqType == 1:
            assert len(r[0]) == 2, "Wrong number of files: %d" % len(r[0])
            assert len(r[1]) == 2, "Wrong number of files: %d" % len(r[1])
        elif reqType == 2:
            assert len(r) == 3, "Wrong number of operations: %d" % len(r)
            assert r[0].Type == "ReplicateAndRegister"
            assert r[1].Type == "RemoveReplica"
            assert r[2].Type == "ForwardDiset"
            assert len(r[0]) == 2, "Wrong number of files: %d" % len(r[0])
            assert len(r[1]) == 2, "Wrong number of files: %d" % len(r[1])
        elif reqType == 3:
            assert len(r) == 3, "Wrong number of operations: %d" % len(r)
            assert r[1].Type == "ReplicateAndRegister"
            assert r[2].Type == "RemoveReplica"
            assert r[0].Type == "ForwardDiset"
            assert len(r[1]) == 2, "Wrong number of files: %d" % len(r[0])
            assert len(r[2]) == 2, "Wrong number of files: %d" % len(r[1])
        elif reqType == 4:
            assert len(r) == 4, "Wrong number of operations: %d" % len(r)
            assert r[1].Type == "ReplicateAndRegister"
            assert r[2].Type == "RemoveReplica"
            assert r[0].Type == "ForwardDiset"
            assert r[3].Type == "ForwardDiset"
            assert len(r[1]) == 2, "Wrong number of files: %d" % len(r[0])
            assert len(r[2]) == 2, "Wrong number of files: %d" % len(r[1])
        elif reqType == 5:
            assert len(r) == 5, "Wrong number of operations: %d" % len(r)
            assert r[1].Type == "ReplicateAndRegister"
            assert r[2].Type == "RemoveReplica"
            assert r[3].Type == "RemoveReplica"
            assert r[0].Type == "ForwardDiset"
            assert r[4].Type == "ForwardDiset"
            assert len(r[1]) == 2, "Wrong number of files: %d" % len(r[0])
            assert len(r[2]) == 1, "Wrong number of files: %d" % len(r[1])
            assert len(r[3]) == 1, "Wrong number of files: %d" % len(r[1])
        elif reqType == 6:
            assert len(r) == 5, "Wrong number of operations: %d" % len(r)
            assert r[1].Type == "ReplicateAndRegister"
            assert r[2].Type == "ReplicateAndRegister"
            assert r[3].Type == "RemoveReplica"
            assert r[0].Type == "ForwardDiset"
            assert r[4].Type == "ForwardDiset"
            assert len(r[1]) == 1, "Wrong number of files: %d" % len(r[0])
            assert len(r[2]) == 1, "Wrong number of files: %d" % len(r[1])
            assert len(r[3]) == 2, "Wrong number of files: %d" % len(r[1])
