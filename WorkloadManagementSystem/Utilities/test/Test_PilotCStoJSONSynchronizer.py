""" This is a test of the creation of the json dump file
"""

import unittest
import os

from DIRAC.WorkloadManagementSystem.Utilities.PilotCStoJSONSynchronizer import PilotCStoJSONSynchronizer
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.Core.Utilities.CFG import CFG


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
    Operations{
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
    '''
    with open(self.testCfgFileName, 'w') as f:
      f.write(cfgContent)
    gConfig = ConfigurationClient(fileToLoadList=[self.testCfgFileName])  # we replace the configuration by our own one.
    self.setup = gConfig.getValue('/DIRAC/Setup', '')
    self.wm = gConfig.getValue('DIRAC/Setups/' + self.setup + '/WorkloadManagement', '')

  def tearDown(self):
    try:
      os.remove(self.testCfgFileName)
    except OSError:
      pass
    # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
    # not to conflict with other tests that might be using a local dirac.cfg
    gConfigurationData.localCFG = CFG()
    gConfigurationData.remoteCFG = CFG()
    gConfigurationData.mergedCFG = CFG()
    gConfigurationData.generateNewVersion()


class Test_PilotCStoJSONSynchronizer_sync(PilotCStoJSONSynchronizerTestCase):

  def test_success(self):
    synchroniser = PilotCStoJSONSynchronizer()
    res = synchroniser._syncJSONFile()
    self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(PilotCStoJSONSynchronizerTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(Test_PilotCStoJSONSynchronizer_sync))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
