from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pytest

from diraccfg import CFG

from DIRAC.ConfigurationSystem.Client.Helpers import Operations
from DIRAC.ConfigurationSystem.private import ConfigurationClient
from DIRAC.ConfigurationSystem.private.ConfigurationData import ConfigurationData

localCFGData = ConfigurationData(False)
mergedCFG = CFG()
mergedCFG.loadFromBuffer("""
specSection
{
  option = specVal
  section
  {
    secOptA = specSecOptA
    secOptB = specSecOptB
  }
}
Operations
{
  Defaults
  {
    option = defVal
    section
    {
      secOptA = defSecOptA
      secOptB = defSecOptB
      defOpt = defOptVal
    }
    specSection
    {
      option = defVal
      section
      {
        secOptB = defSecOptB
        defOpt = defOptVal
      }
    }
  }
  proSetup
  {
    option = proVal
    section
    {
      secOptB = proSecOptB
      proOpt = proOptVal
    }
    specSection
    {
      option = proVal
      section
      {
        secOptB = proSecOptB
        proOpt = proOptVal
      }
    }
  }
  testSetup
  {
    option = testVal
    section
    {
      secOptB = testSecOptB
      testOpt = testOptVal
    }
    specSection
    {
      option = testVal
      section
      {
        secOptB = testSecOptB
        testOpt = testOptVal
      }
    }
  }
  aVO
  {
    Defaults
    {
      option = adefVal
      section
      {
        secOptB = adefSecOptB
        adefOpt = adefOptVal
      }
      specSection
      {
        option = adefVal
        section
        {
          secOptB = adefSecOptB
          adefOpt = adefOptVal
        }
      }
    }
    proSetup
    {
      option = aproVal
      section
      {
        secOptB = aproSecOptB
        aproOpt = aproOptVal
      }
      specSection
      {
        option = aproVal
        section
        {
          secOptB = aproSecOptB
          aproOpt = aproOptVal
        }
      }
    }
    testSetup
    {
      option = atestVal
      section
      {
        secOptB = atestSecOptB
        atestOpt = atestOptVal
      }
      specSection
      {
        option = atestVal
        section
        {
          secOptB = atestSecOptB
          atestOpt = atestOptVal
        }
      }
    }
  }
  bVO
  {
    Defaults
    {
      option = bdefVal
      section
      {
        secOptB = bdefSecOptB
        bdefOpt = bdefOptVal
      }
      specSection
      {
        option = bdefVal
        section
        {
          secOptB = bdefSecOptB
          bdefOpt = bdefOptVal
        }
      }
    }
    proSetup
    {
      option = bproVal
      section
      {
        secOptB = bproSecOptB
        bproOpt = bproOptVal
      }
      specSection
      {
        option = bproVal
        section
        {
          secOptB = bproSecOptB
          bproOpt = bproOptVal
        }
      }
    }
    testSetup
    {
      option = btestVal
      section
      {
        secOptB = btestSecOptB
        btestOpt = btestOptVal
      }
      specSection
      {
        option = btestVal
        section
        {
          secOptB = btestSecOptB
          btestOpt = btestOptVal
        }
      }
    }
  }
}
""")
localCFGData.localCFG = mergedCFG
localCFGData.remoteCFG = mergedCFG
localCFGData.mergedCFG = mergedCFG
localCFGData.generateNewVersion()


@pytest.fixture
def ops(monkeypatch):
  monkeypatch.setattr(ConfigurationClient, 'gConfigurationData', localCFGData)
  monkeypatch.setattr(Operations, "gConfig", ConfigurationClient.ConfigurationClient())
  monkeypatch.setattr(Operations, 'getSetup', lambda: 'proSetup')
  monkeypatch.setattr(Operations, 'getVOfromProxyGroup', lambda: {})
  monkeypatch.setattr(Operations, "gConfigurationData", localCFGData)
  return Operations


@pytest.mark.parametrize("vo, setup, mainSection, cfg, optionPath, sectionPath", [
    (None, None, None, {'option': 'proVal', 'section': {'secOptA': 'defSecOptA',
                                                        'secOptB': 'proSecOptB',
                                                        'proOpt': 'proOptVal',
                                                        'defOpt': 'defOptVal'},
                                            'specSection': {'option': 'proVal',
                                                            'section': {'secOptB': 'proSecOptB',
                                                                        'proOpt': 'proOptVal',
                                                                        'defOpt': 'defOptVal'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    ('aVO', None, None, {'option': 'aproVal', 'section': {'proOpt': 'proOptVal',
                                                          'aproOpt': 'aproOptVal',
                                                          'adefOpt': 'adefOptVal',
                                                          'defOpt': 'defOptVal',
                                                          'secOptA': 'defSecOptA',
                                                          'secOptB': 'aproSecOptB'},
                                              'specSection': {'option': 'aproVal',
                                                              'section': {'proOpt': 'proOptVal',
                                                                          'secOptB': 'aproSecOptB',
                                                                          'proOpt': 'proOptVal',
                                                                          'aproOpt': 'aproOptVal',
                                                                          'adefOpt': 'adefOptVal',
                                                                          'defOpt': 'defOptVal'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    ('bVO', None, None, {'option': 'bproVal', 'section': {'bproOpt': 'bproOptVal',
                                                          'bdefOpt': 'bdefOptVal',
                                                          'proOpt': 'proOptVal',
                                                          'defOpt': 'defOptVal',
                                                          'secOptA': 'defSecOptA',
                                                          'secOptB': 'bproSecOptB'},
                                              'specSection': {'option': 'bproVal',
                                                              'section': {'bproOpt': 'bproOptVal',
                                                                          'bdefOpt': 'bdefOptVal',
                                                                          'proOpt': 'proOptVal',
                                                                          'defOpt': 'defOptVal',
                                                                          'secOptB': 'bproSecOptB'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    ('aVO', 'proSetup', None, {'option': 'aproVal', 'section': {'proOpt': 'proOptVal',
                                                                'aproOpt': 'aproOptVal',
                                                                'adefOpt': 'adefOptVal',
                                                                'defOpt': 'defOptVal',
                                                                'secOptA': 'defSecOptA',
                                                                'secOptB': 'aproSecOptB'},
                                                    'specSection': {'option': 'aproVal',
                                                                    'section': {'proOpt': 'proOptVal',
                                                                                'aproOpt': 'aproOptVal',
                                                                                'adefOpt': 'adefOptVal',
                                                                                'defOpt': 'defOptVal',
                                                                                'secOptB': 'aproSecOptB'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    ('aVO', 'testSetup', None, {'option': 'atestVal', 'section': {'atestOpt': 'atestOptVal',
                                                                  'testOpt': 'testOptVal',
                                                                  'adefOpt': 'adefOptVal',
                                                                  'defOpt': 'defOptVal',
                                                                  'secOptA': 'defSecOptA',
                                                                  'secOptB': 'atestSecOptB'},
                                                      'specSection': {'option': 'atestVal',
                                                                      'section': {'atestOpt': 'atestOptVal',
                                                                                  'testOpt': 'testOptVal',
                                                                                  'adefOpt': 'adefOptVal',
                                                                                  'defOpt': 'defOptVal',
                                                                                  'secOptB': 'atestSecOptB'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    ('bVO', 'proSetup', None, {'option': 'bproVal', 'section': {'bproOpt': 'bproOptVal',
                                                                'bdefOpt': 'bdefOptVal',
                                                                'proOpt': 'proOptVal',
                                                                'defOpt': 'defOptVal',
                                                                'secOptA': 'defSecOptA',
                                                                'secOptB': 'bproSecOptB'},
                                                    'specSection': {'option': 'bproVal',
                                                                    'section': {'bproOpt': 'bproOptVal',
                                                                                'bdefOpt': 'bdefOptVal',
                                                                                'proOpt': 'proOptVal',
                                                                                'defOpt': 'defOptVal',
                                                                                'secOptB': 'bproSecOptB'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    ('bVO', 'testSetup', None, {'option': 'btestVal', 'section': {'testOpt': 'testOptVal',
                                                                  'bdefOpt': 'bdefOptVal',
                                                                  'btestOpt': 'btestOptVal',
                                                                  'defOpt': 'defOptVal',
                                                                  'secOptA': 'defSecOptA',
                                                                  'secOptB': 'btestSecOptB'},
                                                      'specSection': {'option': 'btestVal',
                                                                      'section': {'testOpt': 'testOptVal',
                                                                                  'bdefOpt': 'bdefOptVal',
                                                                                  'btestOpt': 'btestOptVal',
                                                                                  'defOpt': 'defOptVal',
                                                                                  'secOptB': 'btestSecOptB'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    (None, 'proSetup', None, {'option': 'proVal', 'section': {'defOpt': 'defOptVal',
                                                              'secOptA': 'defSecOptA',
                                                              'proOpt': 'proOptVal',
                                                              'secOptB': 'proSecOptB'},
                                                  'specSection': {'option': 'proVal',
                                                                  'section': {'defOpt': 'defOptVal',
                                                                              'proOpt': 'proOptVal',
                                                                              'secOptB': 'proSecOptB'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    (None, 'testSetup', None, {'option': 'testVal', 'section': {'testOpt': 'testOptVal',
                                                                'defOpt': 'defOptVal',
                                                                'secOptA': 'defSecOptA',
                                                                'secOptB': 'testSecOptB'},
                                                    'specSection': {'option': 'testVal',
                                                                    'section': {'testOpt': 'testOptVal',
                                                                                'defOpt': 'defOptVal',
                                                                                'secOptB': 'testSecOptB'}}},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),

    # Test specSection
    ('notExist', None, None, {'specSection': {'section': {'defOpt': 'defOptVal',
                                                          'proOpt': 'proOptVal',
                                                          'secOptB': 'proSecOptB'},
                                              'option': 'proVal'},
                              'section': {'defOpt': 'defOptVal',
                                          'secOptA': 'defSecOptA',
                                          'proOpt': 'proOptVal',
                                          'secOptB': 'proSecOptB'},
                              'option': 'proVal'},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    (None, 'notExist', None, {'specSection': {'section': {'defOpt': 'defOptVal',
                                                          'secOptB': 'defSecOptB'},
                                              'option': 'defVal'},
                              'section': {'defOpt': 'defOptVal',
                                          'secOptA': 'defSecOptA',
                                          'secOptB': 'defSecOptB'},
                              'option': 'defVal'},
     '/Operations/Defaults/option', '/Operations/Defaults/section'),
    (None, None, 'specSection', {'section': {'secOptA': 'specSecOptA',
                                             'proOpt': 'proOptVal',
                                             'secOptB': 'proSecOptB'},
                                 'option': 'proVal'},
     '/specSection/option', '/specSection/section'),
    ('aVO', None, 'specSection', {'section': {'aproOpt': 'aproOptVal',
                                              'secOptA': 'specSecOptA',
                                              'adefOpt': 'adefOptVal',
                                              'proOpt': 'proOptVal',
                                              'secOptB': 'aproSecOptB'},
                                  'option': 'aproVal'},
     '/specSection/option', '/specSection/section'),
    ('aVO', 'proSetup', 'specSection', {'section': {'aproOpt': 'aproOptVal',
                                                    'secOptA': 'specSecOptA',
                                                    'adefOpt': 'adefOptVal',
                                                    'proOpt': 'proOptVal',
                                                    'secOptB': 'aproSecOptB'},
                                        'option': 'aproVal'},
     '/specSection/option', '/specSection/section'),
    ('aVO', 'testSetup', 'specSection', {'section': {'testOpt': 'testOptVal',
                                                     'atestOpt': 'atestOptVal',
                                                     'secOptA': 'specSecOptA',
                                                     'adefOpt': 'adefOptVal',
                                                     'secOptB': 'atestSecOptB'},
                                         'option': 'atestVal'},
     '/specSection/option', '/specSection/section'),
    ('aVO', 'notExist', 'specSection', {'section': {'secOptA': 'specSecOptA',
                                                    'adefOpt': 'adefOptVal',
                                                    'secOptB': 'adefSecOptB'},
                                        'option': 'adefVal'},
     '/specSection/option', '/specSection/section'),
    ('bVO', None, 'specSection', {'section': {'bdefOpt': 'bdefOptVal',
                                              'bproOpt': 'bproOptVal',
                                              'secOptA': 'specSecOptA',
                                              'proOpt': 'proOptVal',
                                              'secOptB': 'bproSecOptB'},
                                  'option': 'bproVal'},
     '/specSection/option', '/specSection/section'),
    ('bVO', 'proSetup', 'specSection', {'section': {'bdefOpt': 'bdefOptVal',
                                                    'bproOpt': 'bproOptVal',
                                                    'secOptA': 'specSecOptA',
                                                    'proOpt': 'proOptVal',
                                                    'secOptB': 'bproSecOptB'},
                                        'option': 'bproVal'},
     '/specSection/option', '/specSection/section'),
    ('bVO', 'testSetup', 'specSection', {'section': {'testOpt': 'testOptVal',
                                                     'bdefOpt': 'bdefOptVal',
                                                     'secOptA': 'specSecOptA',
                                                     'btestOpt': 'btestOptVal',
                                                     'secOptB': 'btestSecOptB'},
                                         'option': 'btestVal'},
     '/specSection/option', '/specSection/section'),
    ('bVO', 'notExist', 'specSection', {'section': {'bdefOpt': 'bdefOptVal',
                                                    'secOptA': 'specSecOptA',
                                                    'secOptB': 'bdefSecOptB'},
                                        'option': 'bdefVal'},
     '/specSection/option', '/specSection/section'),
    (None, 'proSetup', 'specSection', {'section': {'secOptA': 'specSecOptA',
                                                   'proOpt': 'proOptVal',
                                                   'secOptB': 'proSecOptB'},
                                       'option': 'proVal'},
     '/specSection/option', '/specSection/section'),
    (None, 'testSetup', 'specSection', {'section': {'testOpt': 'testOptVal',
                                                    'secOptA': 'specSecOptA',
                                                    'secOptB': 'testSecOptB'},
                                        'option': 'testVal'},
     '/specSection/option', '/specSection/section'),
    (None, 'notExist', 'specSection', {'section': {'secOptA': 'specSecOptA',
                                                   'secOptB': 'specSecOptB'},
                                       'option': 'specVal'},
     '/specSection/option', '/specSection/section'),
    ('notExist', 'notExist', 'specSection', {'section': {'secOptA': 'specSecOptA',
                                                         'secOptB': 'specSecOptB'},
                                             'option': 'specVal'},
     '/specSection/option', '/specSection/section'),
    (None, None, 'notExist', {}, '', '')])
def test_Operations(ops, vo, setup, mainSection, cfg, optionPath, sectionPath):
  """ Test Operations """
  oper = ops.Operations(vo=vo, setup=setup, mainSection=mainSection)

  def checkValue(data, path=''):
    """ Helper method """
    # Test getSections
    result = oper.getSections(path)
    assert result['OK'], result['Message']
    assert set(result['Value']) == set([k for k in data if isinstance(data[k], dict)])

    # Test getOptions
    result = oper.getOptions(path)
    assert result['OK'], result['Message']
    assert set(result['Value']) == set([k for k in data if not isinstance(data[k], dict)])

    # Test getOptionsDict
    result = oper.getOptionsDict(path)
    assert result['OK'], result['Message']
    assert result['Value'] == {k: data[k] for k in data if not isinstance(data[k], dict)}

    for key in data:

      if isinstance(data[key], dict):
        # Check next section
        checkValue(data[key], os.path.join(path, key))

      else:
        # Test getValue
        assert oper.getValue(os.path.join(path, key)) == data[key]

  checkValue(cfg)

  # getPath
  assert oper.getPath('option') == optionPath
  assert oper.getPath('section') == sectionPath

  # getOptionPath
  assert oper.getOptionPath('option') == optionPath

  # getSectionPath
  assert oper.getSectionPath('section') == sectionPath


def test_expiresCache(ops):
  """ Test cache version """
  oldVertion = ops.gConfigurationData.getVersion()
  ops.gConfigurationData.generateNewVersion()
  assert ops.Operations()._cacheExpired(), 'oldVersion(%s), newVersion(%s)' % (oldVertion, ops.gConfigurationData.getVersion())
