"""
    Test if same service work on DIRAC and TORNADO
    Testing if basic operation works on a dummy example

    These handlers provide a method who's access is always forbidden to test authorization system

    It's just normal services, entry in dirac.cfg are the same as usual.
    To start tornado use DIRAC/TornadoServices/scripts/tornado-start-all.py
    ```
    Services
      {
        User
        {
          Protocol = https
        }
        UserDirac
        {
          Port = 3424
        }
      }
    ```

    ```
    URLs
      {
        User = https://MrBoincHost:443/Framework/User
        UserDirac = dips://localhost:3424/Framework/UserDirac
      }
    ```

"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from string import printable
import datetime
import sys

from hypothesis import given, settings
from hypothesis.strategies import text

from DIRAC.Core.DISET.RPCClient import RPCClient as RPCClientDIRAC
from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient as RPCClientTornado
from DIRAC.Core.Utilities.DErrno import ENOAUTH
from DIRAC import S_ERROR

from pytest import mark
parametrize = mark.parametrize

rpc_imp = ((RPCClientTornado, 'Framework/User'), (RPCClientDIRAC, 'Framework/UserDirac'))


@parametrize('rpc', rpc_imp)
def test_authorization(rpc):
  service = rpc[0](rpc[1])

  authorisation = service.unauthorized()
  assert authorisation['OK'] is False
  assert authorisation['Message'] == S_ERROR(ENOAUTH, "Unauthorized query")['Message']


@parametrize('rpc', rpc_imp)
def test_unknown_method(rpc):
  service = rpc[0](rpc[1])

  unknownmethod = service.ThisMethodMayNotExist()
  assert unknownmethod['OK'] is False
  assert unknownmethod['Message'] == "Unknown method ThisMethodMayNotExist"


@parametrize('rpc', rpc_imp)
def test_ping(rpc):
  service = rpc[0](rpc[1])

  assert service.ping()['OK']


@parametrize('rpc', rpc_imp)
@settings(deadline=None, max_examples=42)
@given(data=text(printable, max_size=64))
def test_echo(rpc, data):
  service = rpc[0](rpc[1])

  assert service.echo(data)['Value'] == data


def test_whoami():  # Only in tornado
  credDict = RPCClientTornado('Framework/User').whoami()['Value']
  assert 'DN' in credDict
  assert 'CN' in credDict
  assert 'isProxy' in credDict
