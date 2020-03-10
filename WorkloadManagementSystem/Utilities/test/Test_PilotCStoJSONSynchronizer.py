""" This is a test of the creation of the json dump file
"""

import unittest
import os
from mock import patch, MagicMock as Mock

from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Utilities.CFG import CFG

# pylint: disable=protected-access


class PilotCStoJSONSynchronizerTestCase(unittest.TestCase):
  """ Base class for the PilotCStoJSONSynchronizer test cases
  """

  def setUp(self):
    # Creating test configuration file
    self.testCfgFileName = 'test.cfg'
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
            Service1 = dips://server1:1234/WorkloadManagement/Service1
            Service2 = dips://$MAINSERVERS$:5678/WorkloadManagement/Service2
          }
          FailoverURLs
          {
            Service2 = dips://failover1:5678/WorkloadManagement/Service2
          }
        }
      }
    }
    Operations
    {
      Defaults
      {
        Pilot
        {
          Project = LHCb
          GenericPilotDN = /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=doe/CN=111213/CN=Joe Doe
          GenericPilotGroup = xxx_pilot
        }

        MainServers = gw1, gw2
      }
    }
    Registry
    {
      Users
      {
        ttester
        {
          DN = /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=ttester/CN=696969/CN=Thomas Tester
          CA = /DC=ch/DC=cern/CN=CERN Grid Certification Authority
          Email = thomas.tester@cern.ch
        }
        franekbolek
        {
          DN = /DC=ch/DC=voodo/OU=Organic Units/OU=Users/CN=franekbolek/CN=111122/CN=Franek Bolek
          CA = /DC=ch/DC=voodo/CN=Voodo Grid Certification Authority
          Email = franek.bolek@voodo.pl
        }
      }
      Groups
      {
        lhcb_pilot
        {
          #@@-host - /DC=ch/DC=voodo/OU=computers/CN=brabra.voodo.pl
          Users = franekbolek
          Users += ttester
          Properties = GenericPilot
          Properties += LimitedDelegation
          VOMSRole = /lhcb/Role=pilot
          #@@-ggg@diracAdmin - 2015-07-07 13:40:55
          VO = lhcb
        }
      }
    }
    Resources
    {
      Sites
      {
        Tests
        {
           Tests.Testing.tst
           {
             CEs
             {
                test1.Testing.tst
                {
                  CEType = Tester
                }
             }
           }
        }
      }
    }
    '''
    with open(self.testCfgFileName, 'w') as f:
      f.write(cfgContent)
    # we replace the configuration by our own one.
    gConfig = ConfigurationClient(fileToLoadList=[self.testCfgFileName])
    self.setup = gConfig.getValue('/DIRAC/Setup', '')
    self.wm = gConfig.getValue('DIRAC/Setups/' + self.setup + '/WorkloadManagement', '')

  def tearDown(self):
    for aFile in [self.testCfgFileName, 'checksums.sha512', 'pilot.json']:
      try:
        os.remove(aFile)
      except OSError:
        pass
    # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
    # not to conflict with other tests that might be using a local dirac.cfg
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()


class Test_PilotCStoJSONSynchronizer_sync(PilotCStoJSONSynchronizerTestCase):

  @patch('DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer.requests', new=Mock())
  def test_success(self):
    synchroniser = PilotCStoJSONSynchronizer()
    synchroniser.pilotFileServer = 'value'
    res = synchroniser._syncJSONFile()
    self.assertTrue(res is None)
    # ensure pilot.json was "uploaded"
    assert 'pilot.json' in synchroniser._checksumDict

  @patch('DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer.requests', new=Mock())
  def test_syncchecksum(self):
    expectedHash = '00e67a2d45e2c2508a935500a4765e1a5f1ce661f23c1fb329987c8211bde754ed' + \
                   '79f6b02cdeabd429979a82014c474c5ce2f46a879f17e2a6ce4bcac683e2e4'
    synchroniser = PilotCStoJSONSynchronizer()
    synchroniser.pilotFileServer = 'value'
    synchroniser._checksumFile(self.testCfgFileName)
    res = synchroniser._syncJSONFile()
    assert res is None
    synchroniser._syncChecksum()
    assert self.testCfgFileName in synchroniser._checksumDict
    assert synchroniser._checksumDict[self.testCfgFileName] == expectedHash
    assert open('checksums.sha512', 'rb').read().split('\n')[1] == '%s: %s' % (expectedHash, self.testCfgFileName)
    # this tests if the checksums file was also "uploaded"
    assert 'checksums.sha512' in list(synchroniser._checksumDict)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PilotCStoJSONSynchronizerTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_PilotCStoJSONSynchronizer_sync))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
