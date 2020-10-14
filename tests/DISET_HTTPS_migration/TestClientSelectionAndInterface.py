"""
    Unit test on client selection:
        - By default: RPCClient should be used
        - If we use Tornado service TornadoClient is used

    Should work with
        - 'Component/Service'
        - URL
        - List of URL

    Mock Config:
        - Service using HTTPS with Tornado
        - Service using Diset

    You don't need to setup anything, just run ``pytest TestClientSelection.py`` !
"""
from __future__ import print_function
import os
import re


from pytest import mark, fixture

from DIRAC.Core.Tornado.Client.ClientSelector import RPCClientSelector
from DIRAC.Core.Tornado.Client.TornadoClient import TornadoClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from diraccfg import CFG
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.DISET.private.InnerRPCClient import InnerRPCClient

parametrize = mark.parametrize


testCfgFileName = 'test.cfg'


@fixture(scope='function')
def config(request):
  """
    fixture is the pytest way to declare initalization function.
    Scope = module significate that this function will be called only time for this file.
    If no scope precised it call config for each test.

    This function can have a return value, it will be the value of 'config' argument for the tests
  """

  cfgContent = '''
  DIRAC
  {
    Setup=TestSetup
    Setups
    {
      TestSetup
      {
        WorkloadManagement=MyWM
      }
    }
  }
  Systems
  {
    WorkloadManagement
    {
      MyWM
      {
        URLs
        {
          ServiceDips = dips://$MAINSERVERS$:1234/WorkloadManagement/ServiceDips
          ServiceHttps = https://$MAINSERVERS$:1234/WorkloadManagement/ServiceHttps
        }
      }
    }
  }
  Operations{
    Defaults
    {
      MainServers = server1, server2
    }
  }
  '''
  with open(testCfgFileName, 'w') as f:
    f.write(cfgContent)
  gConfig = ConfigurationClient(fileToLoadList=[testCfgFileName])  # we replace the configuration by our own one.

  # def tearDown():
  # Wait for teardown
  yield config
  """
    This function is called at the end of the test.
  """
  try:
    os.remove(testCfgFileName)
  except OSError:
    pass
  # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
  # not to conflict with other tests that might be using a local dirac.cfg
  gConfigurationData.localCFG = CFG()
  gConfigurationData.remoteCFG = CFG()
  gConfigurationData.mergedCFG = CFG()
  gConfigurationData.generateNewVersion()
  print("TearDown")
  # request is given by @fixture decorator, addfinalizer set the function who need to be called after the tests
  # request.addfinalizer(tearDown)


# Tuple with (expectedClient, serviceName)
client_imp = (
    (TornadoClient, 'WorkloadManagement/ServiceHttps'),
    (TornadoClient, 'https://server1:1234/WorkloadManagement/ServiceHttps'),
    (TornadoClient,
     'https://server1:1234/WorkloadManagement/ServiceHttps,https://server2:1234/WorkloadManagement/ServiceHttps'),
    (RPCClient, 'WorkloadManagement/ServiceDips'),
    (RPCClient, 'dips://server1:1234/WorkloadManagement/ServiceDips'),
    (RPCClient,
     'dips://server1:1234/WorkloadManagement/ServiceDips,dips://server2:1234/WorkloadManagement/ServiceDips'),
)


@parametrize('client', client_imp)
def test_selection_when_using_RPCClientSelector(client, config):
  """
    One way to call service is to use RPCClient or TornadoClient
    If service is HTTPS, it must return client who work with tornado (TornadoClient)
    else it must return the RPCClient
  """
  clientWanted = client[0]
  component_service = client[1]
  clientSelected = RPCClientSelector(component_service)
  assert isinstance(clientSelected, clientWanted)


error_component = (
    'Too/Many/Sections',
    'JustAName',
    "InexistantComponent/InexistantService",
    "dummyProtocol://dummy/url")


@parametrize('component_service', error_component)
def test_error(component_service, config):
  """
    In any other cases (including error cases) it must return RPCClient by default
    This test is NOT testing if RPCClient handle the errors
    It just test that we get RPCClient and not Tornadoclient
  """
  clientSelected = RPCClientSelector(component_service)
  assert isinstance(clientSelected, RPCClient)


def test_interface():
  """
    Interface of TornadoClient MUST contain at least interface of RPCClient.
    BUT a __getattr__ method extends this interface with interface of InnerRPCClient.
  """
  interfaceTornadoClient = dir(TornadoClient)
  interfaceRPCClient = dir(RPCClient) + dir(InnerRPCClient)
  for element in interfaceRPCClient:
    # We don't need to test private methods / attribute
    # Private methods/attribute starts with __
    # dir also return private methods named with something like  _ClassName__PrivateMethodName
    if not element.startswith('_'):
      assert element in interfaceTornadoClient


client_imp = (
    (2, 'WorkloadManagement/ServiceHttps'),
    (1, 'https://server1:1234/WorkloadManagement/ServiceHttps')
)


@parametrize('client', client_imp)
def test_urls_used_by_TornadoClient(config, client):
  # We can't directly get url because they are randomized but we can check if we have right number of URL

  nbOfUrl = client[0]
  component_service = client[1]
  clientSelected = RPCClientSelector(component_service)
  # Little hack to get the private attribute
  assert nbOfUrl == clientSelected._TornadoBaseClient__nbOfUrls
