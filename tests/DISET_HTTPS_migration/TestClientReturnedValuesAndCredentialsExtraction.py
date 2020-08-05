"""
    In this test we want to check if Tornado generate the same credentials dictionnary as DIRAC.
    It also test if the correct certificates are sended by client.

    To run this test you must have the handlers who returns credentials dictionnary.
    Handlers are diracCredDictHandler and tornadoCredDictHandler just returns these dictionnary
    and are stored in DIRAC/FrameworkSystem/Service

    Then you have to start tornado using script tornado-start-all.py in DIRAC/TornadoServices/scripts
    and diset with ``dirac-service Framework/diracCredDict`` before running test

    In configuration it have to be set as normal services, it will look like:

    ```
    # In Systems/Service/<instance>/Framework
     Services
      {
        tornadoCredDict
        {
          protocol = https
        }
        diracCredDict
        {
          Port = 3444
          DisableMonitoring = yes
        }
      }
    ```

    ```
    URLs
      {
        tornadoCredDict = https://localhost:443/Framework/tornadoCredDict
        diracCredDict = dips://MrBoincHost:3444/Framework/diracCredDict
      }
    ```

"""
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
import pytest
from DIRAC import gConfig
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from pytest import mark, fixture
parametrize = mark.parametrize


def get_RPC_returnedValue(serviceName, Client):
  """
    Get credentials extracted tornado server or Dirac server
  """
  service = Client(serviceName)
  return service.credDict()


def get_all_returnedValues():
  """
    Just code factorisation to who call server and get credential dictionnary
  """
  serviceNameTornado = 'Framework/tornadoCredDict'
  serviceNameDirac = 'Framework/diracCredDict'
  repTornado = TornadoClient(serviceNameTornado).whoami()
  repDirac = RPCClient(serviceNameDirac).whoami()
  return (repTornado, repDirac)


@parametrize('UseServerCertificate', ('true', 'false'))
def test_return_credential_are_equals(UseServerCertificate):
  """
    Check if certificates sended AND extraction have same comportement is DISET and HTTPS
  """
  gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', UseServerCertificate)

  (repTornado, repDirac) = get_all_returnedValues()

  # Service returns credentials
  assert repDirac['Value'] == repTornado['Value']


@parametrize('UseServerCertificate', ('True', 'False'))
def test_rpcStubs_are_equals(UseServerCertificate):
  """
    Test if Clients returns the same rpcStubs

    Navigating through array is a bit complicated in this test...
    repDirac and repTornado may have the same structure:

    repDirac dict{
      OK: True
      rpcStub: tuple{
        ServiceName: str
        kwargs: dict{
          *** kwargs used to instanciate client ***
        }
        methodName: str
        arguments: list
      }
      Value: dict { # NOT USED IN THIS TEST
        *** Credentials dictionnary extracted by server ***
      }
    }
  """

  gConfigurationData.setOptionInCFG('/DIRAC/Security/UseServerCertificate', UseServerCertificate)
  (repTornado, repDirac) = get_all_returnedValues()

  # Explicitly removed in Tornado
  del repDirac['rpcStub'][0][1]['keepAliveLapse']

  # rep['rpcStub'] is at form (rpcStub, method, args) where rpcStub is tuple with (serviceName, kwargs)
  assert repTornado['rpcStub'][0][0] != repDirac['rpcStub'][0][0]  # Services name are different
  assert repTornado['rpcStub'][0][1] == repDirac['rpcStub'][0][1]  # Check kwargs returned by rpcStub
  assert repTornado['rpcStub'][1:] != repDirac['rpcStub'][1:]  # Check method/args
