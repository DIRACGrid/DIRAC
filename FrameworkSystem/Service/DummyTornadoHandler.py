"""
    This handler is only here to test if Tornado server starts
    It can be used for some tests (like performance test on ping)
"""


from DIRAC.TornadoServices.Server.TornadoService import TornadoService
from DIRAC import S_OK, S_ERROR

class DummyTornadoHandler(TornadoService):
  
  #LOCATION = "Framework/Dummy"

  auth_true = ['all']
  def export_true(self):
    return S_OK()

  auth_false = ['all']
  def export_false(self):
    return S_ERROR()

