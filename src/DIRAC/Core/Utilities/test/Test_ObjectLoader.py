from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Tornado.Server.TornadoService import TornadoService


def _check(result):
    assert result["OK"], result["Message"]
    return result["Value"]


def test_load():
    _check(ObjectLoader().loadObject("Core.Utilities.List", "fromChar"))
    _check(ObjectLoader().loadObject("Core.Utilities.ObjectLoader", "ObjectLoader"))
    assert _check(ObjectLoader().loadObject("Core.Utilities.ObjectLoader")) is ObjectLoader
    dataFilter = _check(ObjectLoader().getObjects("WorkloadManagementSystem.Service", ".*Handler"))
    dataClass = _check(ObjectLoader().getObjects("WorkloadManagementSystem.Service", parentClass=RequestHandler))
    tornadoDataClass = _check(ObjectLoader().getObjects("WorkloadManagementSystem.Service", parentClass=TornadoService))

    assert sorted(dataFilter) == sorted(set(dataClass).union(set(tornadoDataClass)))
