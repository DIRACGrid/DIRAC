"""
    This handler is only here to test if Tornado server starts
    It can be used for some tests (like performance test on ping)
    This file must be copied in FrameworkSystem/Service to run tests
"""


from DIRAC.Core.Tornado.Server.TornadoService import TornadoService
from DIRAC import S_OK, S_ERROR


class DummyTornadoHandler(TornadoService):

  auth_true = ['all']

  def export_true(self):
    return S_OK()

  auth_false = ['all']

  def export_false(self):
    return S_ERROR()
