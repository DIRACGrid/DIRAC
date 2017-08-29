__RCSID__ = "$Id$"

import unittest
import mock
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
import pprint
import copy

gLogger.setLevel('DEBUG')
dict_cs = {
    "Resources": {
        "StorageElements": {
            "CERN-SIMPLE": {
                "BackendType": "Eos",
                "SEType": "T0D1",
                "RemoteAccessProtocol": {
                    "Host": "srm-eoslhcb.cern.ch",
                    "Port": 8443,
                    "PluginName": "GFAL2_SRM2",
                    "Protocol": "srm",
                    "Path": "/eos/lhcb/grid/prod",
                    "Access": "remote",
                    "SpaceToken": "LHCb-EOS",
                    "WSUrl": "/srm/v2/server?SFN:",
                },
                "LocalAccessProtocol": {
                    "Host": "eoslhcb.cern.ch",
                    "PluginName": "File",
                    "Protocol": "file",
                    "Path": "/eos/lhcb/grid/prod",
                    "Access": "local",
                    "SpaceToken": "LHCb-EOS",
                },
            },
            "CERN-BASE": {
                "BackendType": "Eos",
                "SEType": "T0D1",
                "AccessProtocol.1": {
                    "Host": "srm-eoslhcb.cern.ch",
                    "Port": 8443,
                    "PluginName": "GFAL2_SRM2",
                    "Protocol": "srm",
                    "Path": "/eos/lhcb/grid/prod",
                    "Access": "remote",
                    "SpaceToken": "LHCb-EOS",
                    "WSUrl": "/srm/v2/server?SFN:",
                }
            },
            # Just inherit, overwrite a SpaceToken and Path and add an option
            "CERN-USER": {
                "BaseSE": "CERN-BASE",
                "PledgedSpace": 205,
                "AccessProtocol.1": {
                    "PluginName": "GFAL2_SRM2",
                    "Path": "/eos/lhcb/grid/user",
                    "SpaceToken": "LHCb_USER",
                }
            },
            "CERN-DST": {
                "BaseSE": "CERN-BASE",
                "AccessProtocol.1": {
                    "PluginName": "GFAL2_SRM2",
                    "Path": "/eos/lhcb/grid/prod",
                }
            },
            # Does not redefine anything, it is just to give another name
            "CERN-NO-DEF": {
                "BaseSE": "CERN-BASE",
            },
            # Buggy because does not redefine PluginName
            "CERN-NO-PLUGIN-NAME": {
                "BaseSE": "CERN-BASE",
                "AccessProtocol.1": {
                    "Path": "/eos/lhcb/grid/prod",
                }
            },
            # Buggy because defines a different PluginName
            "CERN-BAD-PLUGIN-NAME": {
                "BaseSE": "CERN-BASE",
                "AccessProtocol.1": {
                    "PluginName": "AnotherPluginName",
                    "Path": "/eos/lhcb/grid/prod",
                    "Access": "local",
                }
            },
            # Buggy because uses a different name for the same plugin
            "CERN-REDEFINE-PLUGIN-NAME": {
                "BaseSE": "CERN-BASE",
                "AccessProtocol.OtherName": {
                    "PluginName": "GFAL2_SRM2",
                    "Path": "/eos/lhcb/grid/other",
                }
            },
            # More because add an extra protocol compared to the parent
            "CERN-MORE": {
                "BaseSE": "CERN-BASE",
                "AccessProtocol.1": {
                    "PluginName": "GFAL2_SRM2",
                    "Path": "/eos/lhcb/grid/user",
                },
                "AccessProtocol.More": {
                    "Host": "srm-eoslhcb.cern.ch",
                    "Port": 8443,
                    "PluginName": "Extra",
                    "Protocol": "srm",
                    "Path": "/eos/lhcb/grid/prod",
                    "Access": "remote",
                    "SpaceToken": "LHCb-EOS",
                }
            },
            # Pure abstract because no path or SpaceToken
            "CERN-ABSTRACT": {
                "BackendType": "Eos",
                "SEType": "T0D1",
                "AccessProtocol.1": {
                    "Host": "srm-eoslhcb.cern.ch",
                    "Port": 8443,
                    "PluginName": "GFAL2_SRM2",
                    "Protocol": "srm",
                    "Access": "remote",
                    "WSUrl": "/srm/v2/server?SFN:",
                },
                "AccessProtocol.2": {
                    "Host": "eoslhcb.cern.ch",
                    "PluginName": "GFAL2_XROOT",
                    "Protocol": "root",
                    "Access": "remote",
                }
            },
            # Inherits from ABSTRACT
            "CERN-CHILD": {
                "BaseSE": "CERN-ABSTRACT",
                "AccessProtocol.1": {
                    "PluginName": "GFAL2_SRM2",
                    "Path": "/eos/lhcb/grid/user",
                    "SpaceToken": "LHCb_USER",
                },
                "AccessProtocol.2": {
                    "PluginName": "GFAL2_XROOT",
                    "Path": "/eos/lhcb/grid/xrootuser",
                }
            },
        }
    }
}


class fake_gConfig(object):
  @staticmethod
  def crawlCS(path):
    # How nice :-)
    # split the path and recursively dig down the dict_cs dictionary
    return reduce(lambda d, e: d.get(e, {}), path.strip('/').split('/'), dict_cs)

  def getValue(self, path, defaultValue = ''):
    if 'StorageElements' not in path:
      return gConfig.getValue(path, defaultValue)
    csValue = self.crawlCS(path)
    if not csValue:
      csValue = defaultValue
    return csValue

  def getOptionsDict(self, path):
    """ Mock the getOptionsDict call of gConfig
      It reads from dict_cs
    """
    if 'StorageElements' not in path:
      return gConfig.getOptionsDict(path)
    csSection = self.crawlCS(path)
    options = dict((opt, val) for opt, val in csSection.iteritems() if not isinstance(val, dict))
    return S_OK(options)

  def getOptions(self, path):
    """ Mock the getOptions call of gConfig
      It reads from dict_cs
    """
    if 'StorageElements' not in path:
      return gConfig.getOptions(path)
    csSection = self.crawlCS(path)
    options = [opt for opt, val in csSection.iteritems() if not isinstance(val, dict)]
    return S_OK(options)

  def getSections(self, path):
    """ Mock the getOptions call of gConfig
      It reads from dict_cs
    """
    if 'StorageElements' not in path:
      return gConfig.getSections(path)
    csSection = self.crawlCS(path)
    sections = [opt for opt, val in csSection.iteritems() if isinstance(val, dict)]
    return S_OK(sections)


def mock_StorageFactory__generateStorageObject(*args, **kwargs):
  """ Don't really load the plugin, just create an object """
  return S_OK(object())


def mock_resourceStatus_getElementStatus(seName, elementType = 'StorageElement'):
  """ We shut up RSS
  """
  return S_OK({seName: {}})


sf_gConfig = fake_gConfig()
mandatoryProtocolOptions = {
    'Access': '',
    'Host': '',
    'Path': '',
    'Port': '',
    'Protocol': '',
    'SpaceToken': '',
    'WSUrl': ''
}


@mock.patch('DIRAC.Resources.Storage.StorageFactory.gConfig', new = sf_gConfig)
@mock.patch(
    'DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject',
    side_effect = mock_StorageFactory__generateStorageObject)
@mock.patch(
    'DIRAC.ResourceStatusSystem.Client.ResourceStatus.ResourceStatus.getElementStatus',
    side_effect = mock_resourceStatus_getElementStatus)
class StorageFactoryStandaloneTestCase(unittest.TestCase):
  """ Base class for the StorageFactory test cases
  """

  def test_standalone(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ Test loading a storage element with everything defined in itself.
        It should have two storage plugins
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-SIMPLE')

    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['LocalPlugins'], ['File'])
    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2'])

    allProtocols = []
    for protocol in ['RemoteAccessProtocol', 'LocalAccessProtocol']:
      protocolDef = copy.copy(mandatoryProtocolOptions)
      protocolDef.update(
          fake_gConfig.crawlCS('/Resources/StorageElements/CERN-SIMPLE/%s' % protocol))
      allProtocols.append(protocolDef)

    self.assertEqual(len(storages['ProtocolOptions']), len(allProtocols))
    self.assertEqual(len(storages['StorageObjects']), len(allProtocols))

    self.assertListEqual(sorted(allProtocols), sorted(storages['ProtocolOptions']))
    self.assertDictEqual(storages['StorageOptions'], {'BackendType': 'Eos', 'SEType': 'T0D1'})


@mock.patch('DIRAC.Resources.Storage.StorageFactory.gConfig', new = sf_gConfig)
@mock.patch(
    'DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject',
    side_effect = mock_StorageFactory__generateStorageObject)
@mock.patch(
    'DIRAC.ResourceStatusSystem.Client.ResourceStatus.ResourceStatus.getElementStatus',
    side_effect = mock_resourceStatus_getElementStatus)
class StorageFactorySimpleInheritance(unittest.TestCase):
  """ In this class we perform simple inheritance test, with only one
    protocol and no extra protocol definition
  """

  def test_simple_inheritance_overwrite(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-USER that inherits from CERN-BASE,
        add a storage option, redefine the path and the space token
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-USER')

    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2'])

    # There should be a single protocol
    self.assertEqual(len(storages['ProtocolOptions']), 1)
    # There should be one storage object
    self.assertEqual(len(storages['StorageObjects']), 1)

    protocolDetail = storages['ProtocolOptions'][0]
    # These are the values we expect
    self.assertEqual(protocolDetail['Access'], 'remote')
    self.assertEqual(protocolDetail['Host'], 'srm-eoslhcb.cern.ch')
    self.assertEqual(protocolDetail['Path'], '/eos/lhcb/grid/user')
    self.assertEqual(protocolDetail['PluginName'], 'GFAL2_SRM2')
    self.assertEqual(protocolDetail['Port'], 8443)
    self.assertEqual(protocolDetail['Protocol'], 'srm')
    self.assertEqual(protocolDetail['SpaceToken'], 'LHCb_USER')
    self.assertEqual(protocolDetail['WSUrl'], '/srm/v2/server?SFN:')

    self.assertDictEqual(storages['StorageOptions'], {
        'BackendType': 'Eos',
        'SEType': 'T0D1',
        'PledgedSpace': 205,
        'BaseSE': 'CERN-BASE'
    })

  def test_simple_inheritance(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-DST that inherits from CERN-BASE,
        and redefine the same value for Path and PluginName
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-DST')

    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2'])

    # There should be a single protocol
    self.assertEqual(len(storages['ProtocolOptions']), 1)
    # There should be one storage object
    self.assertEqual(len(storages['StorageObjects']), 1)

    protocolDetail = storages['ProtocolOptions'][0]
    # These are the values we expect
    self.assertEqual(protocolDetail['Access'], 'remote')
    self.assertEqual(protocolDetail['Host'], 'srm-eoslhcb.cern.ch')
    self.assertEqual(protocolDetail['Path'], '/eos/lhcb/grid/prod')
    self.assertEqual(protocolDetail['PluginName'], 'GFAL2_SRM2')
    self.assertEqual(protocolDetail['Port'], 8443)
    self.assertEqual(protocolDetail['Protocol'], 'srm')
    self.assertEqual(protocolDetail['SpaceToken'], 'LHCb-EOS')
    self.assertEqual(protocolDetail['WSUrl'], '/srm/v2/server?SFN:')

    self.assertDictEqual(storages['StorageOptions'],
                         {'BackendType': 'Eos',
                          'SEType': 'T0D1',
                          'BaseSE': 'CERN-BASE'})

  def test_pure_inheritance(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-NO-DEF that inherits from CERN-BASE,
        but does not redefine ANYTHING. We expect it to be just like the parent
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-NO-DEF')

    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2'])

    # There should be a single protocol
    self.assertEqual(len(storages['ProtocolOptions']), 1)
    # There should be one storage object
    self.assertEqual(len(storages['StorageObjects']), 1)

    protocolDetail = storages['ProtocolOptions'][0]
    # These are the values we expect
    self.assertEqual(protocolDetail['Access'], 'remote')
    self.assertEqual(protocolDetail['Host'], 'srm-eoslhcb.cern.ch')
    self.assertEqual(protocolDetail['Path'], '/eos/lhcb/grid/prod')
    self.assertEqual(protocolDetail['PluginName'], 'GFAL2_SRM2')
    self.assertEqual(protocolDetail['Port'], 8443)
    self.assertEqual(protocolDetail['Protocol'], 'srm')
    self.assertEqual(protocolDetail['SpaceToken'], 'LHCb-EOS')
    self.assertEqual(protocolDetail['WSUrl'], '/srm/v2/server?SFN:')

    self.assertDictEqual(storages['StorageOptions'],
                         {'BackendType': 'Eos',
                          'SEType': 'T0D1',
                          'BaseSE': 'CERN-BASE'})


@mock.patch('DIRAC.Resources.Storage.StorageFactory.gConfig', new = sf_gConfig)
@mock.patch(
    'DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject',
    side_effect = mock_StorageFactory__generateStorageObject)
@mock.patch(
    'DIRAC.ResourceStatusSystem.Client.ResourceStatus.ResourceStatus.getElementStatus',
    side_effect = mock_resourceStatus_getElementStatus)
class StorageFactoryWeirdDefinition(unittest.TestCase):
  """ In this class, we test error cases
  """

  def test_no_plugin_name(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-NO-PLUGIN-NAME that inherits from CERN-BASE,
        and redifine the same protocol but with no PluginName
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-NO-PLUGIN-NAME')

    self.assertFalse(storages['OK'], storages)

  def test_bad_plugin_name(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-BAD-PLUGIN-NAME that inherits from CERN-BASE,
        and redifine the same protocol but with a different PluginName.

        Currently this results in two different protocols, but this should change
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-BAD-PLUGIN-NAME')
    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2'])
    self.assertListEqual(storages['LocalPlugins'], ['AnotherPluginName'])

    expectedProtocols = [{
        'Access': 'remote',
        'Host': 'srm-eoslhcb.cern.ch',
        'Path': '/eos/lhcb/grid/prod',
        'PluginName': 'GFAL2_SRM2',
        'Port': 8443,
        'Protocol': 'srm',
        'SpaceToken': 'LHCb-EOS',
        'WSUrl': '/srm/v2/server?SFN:'
    }, {
        'Access': 'local',
        'Host': '',
        'Path': '/eos/lhcb/grid/prod',
        'PluginName': 'AnotherPluginName',
        'Port': '',
        'Protocol': '',
        'SpaceToken': '',
        'WSUrl': ''
    }]

    self.assertListEqual(storages['ProtocolOptions'], expectedProtocols)

  def test_redefine_plugin_name(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-REDEFINE-PLUGIN-NAME that inherits from CERN-BASE,
        and uses the same Plugin with a different section.

        Currently this results in a single protocol, but this should change
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-REDEFINE-PLUGIN-NAME')
    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2'])

    expectedProtocols = [{
        'Access': 'remote',
        'Host': 'srm-eoslhcb.cern.ch',
        'Path': '/eos/lhcb/grid/other',
        'PluginName': 'GFAL2_SRM2',
        'Port': 8443,
        'Protocol': 'srm',
        'SpaceToken': 'LHCb-EOS',
        'WSUrl': '/srm/v2/server?SFN:'
    }]

    self.assertListEqual(storages['ProtocolOptions'], expectedProtocols)

  def test_more_protocol(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-MORE that inherits from CERN-BASE,
        and adds an extra protocol
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-MORE')
    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2', 'Extra'])

    expectedProtocols = [{
        'Access': 'remote',
        'Host': 'srm-eoslhcb.cern.ch',
        'Path': '/eos/lhcb/grid/user',
        'PluginName': 'GFAL2_SRM2',
        'Port': 8443,
        'Protocol': 'srm',
        'SpaceToken': 'LHCb-EOS',
        'WSUrl': '/srm/v2/server?SFN:'
    }, {
        'Access': 'remote',
        'Host': 'srm-eoslhcb.cern.ch',
        'Path': '/eos/lhcb/grid/prod',
        'PluginName': 'Extra',
        'Port': 8443,
        'Protocol': 'srm',
        'SpaceToken': 'LHCb-EOS',
        'WSUrl': ''
    }]

    self.assertListEqual(storages['ProtocolOptions'], expectedProtocols)


@mock.patch('DIRAC.Resources.Storage.StorageFactory.gConfig', new = sf_gConfig)
@mock.patch(
    'DIRAC.Resources.Storage.StorageFactory.StorageFactory._StorageFactory__generateStorageObject',
    side_effect = mock_StorageFactory__generateStorageObject)
@mock.patch(
    'DIRAC.ResourceStatusSystem.Client.ResourceStatus.ResourceStatus.getElementStatus',
    side_effect = mock_resourceStatus_getElementStatus)
class StorageFactoryPureAbstract(unittest.TestCase):
  """ In this class, we test pure abstract inheritance
  """

  def test_pure_abstract(self, _sf_generateStorageObject, _rss_getSEStatus):
    """ In this test, we load a storage element CERN-CHILD that inherits from CERN-ABSTRACT.
        CERN-ABSTRACT has two uncomplete protocols, and CERN-CHILD defines them
    """

    sf = StorageFactory(vo = 'lhcb')
    storages = sf.getStorages('CERN-CHILD')
    self.assertTrue(storages['OK'], storages)
    storages = storages['Value']

    self.assertListEqual(storages['RemotePlugins'], ['GFAL2_SRM2', 'GFAL2_XROOT'])

    expectedProtocols = [{
        'Access': 'remote',
        'Host': 'srm-eoslhcb.cern.ch',
        'Path': '/eos/lhcb/grid/user',
        'PluginName': 'GFAL2_SRM2',
        'Port': 8443,
        'Protocol': 'srm',
        'SpaceToken': 'LHCb_USER',
        'WSUrl': '/srm/v2/server?SFN:'
    }, {
        'Access': 'remote',
        'Host': 'eoslhcb.cern.ch',
        'Path': '/eos/lhcb/grid/xrootuser',
        'PluginName': 'GFAL2_XROOT',
        'Port': '',
        'Protocol': 'root',
        'SpaceToken': '',
        'WSUrl': ''
    }]

    self.assertListEqual(storages['ProtocolOptions'], expectedProtocols)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(StorageFactoryStandaloneTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StorageFactorySimpleInheritance))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StorageFactoryWeirdDefinition))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StorageFactoryPureAbstract))
  testResult = unittest.TextTestRunner(verbosity = 3).run(suite)
