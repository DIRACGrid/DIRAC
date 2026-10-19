"""Tests for ReqProxyHandler"""
import json

from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Service import ReqProxyHandler as ReqProxyModule
from DIRAC.RequestManagementSystem.Service.ReqProxyHandler import ReqProxyHandler


def test_putRequestKeepsOperations(tmp_path, monkeypatch):
    """The cached request must contain the same operations as the one which was received"""
    monkeypatch.setattr(ReqProxyHandler, "cacheDir", classmethod(lambda cls: str(tmp_path)))
    monkeypatch.setattr(ReqProxyModule.RequestValidator, "setAndCheckRequestOwner", lambda request, creds: True)
    handler = object.__new__(ReqProxyHandler)
    handler.getRemoteCredentials = lambda: {}

    request = Request({"RequestName": "test", "OwnerDN": "/DC=ch/CN=user", "OwnerGroup": "group"})
    op = Operation({"Type": "ReplicateAndRegister", "TargetSE": "CERN-USER"})
    op.addFile(File({"LFN": "/a/b/c"}))
    request.addOperation(op)
    request.addOperation(Operation({"Type": "ForwardDISET", "Arguments": "x"}))

    res = handler.export_putRequest(request.toJSON()["Value"])
    assert res["OK"], res
    assert res["Value"] == {"set": False, "saved": True}

    (cachedFile,) = tmp_path.iterdir()
    cached = Request(json.loads(cachedFile.read_text()))
    assert [o.Type for o in cached] == ["ReplicateAndRegister", "ForwardDISET"]
    assert [f.LFN for f in cached[0]] == ["/a/b/c"]
    assert (cached.OwnerDN, cached.OwnerGroup) == ("/DC=ch/CN=user", "group")
