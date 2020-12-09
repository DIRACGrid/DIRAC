""" Unit tests for PathFinder only for functions that I added
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import unittest
from diraccfg import CFG
from DIRAC.ConfigurationSystem.Client.PathFinder import getComponentSection, getServiceFailoverURL, getServiceURL
from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData


class TestPathFinder( unittest.TestCase ):
  def setUp( self ):
    #Creating test configuration file
    self.testCfgFileName = 'test.cfg'
    cfgContent='''
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
        MainServers = gw1, gw2
      }
    }
    '''
    with open(self.testCfgFileName, 'w') as f:
      f.write(cfgContent)
    gConfig = ConfigurationClient(fileToLoadList = [self.testCfgFileName])  #we replace the configuration by our own one.
    self.setup = gConfig.getValue( '/DIRAC/Setup', '' )
    self.wm = gConfig.getValue('DIRAC/Setups/' + self.setup +'/WorkloadManagement', '')
  def tearDown( self ):
    try:
      os.remove(self.testCfgFileName)
    except OSError:
      pass
    # SUPER UGLY: one must recreate the CFG objects of gConfigurationData
    # not to conflict with other tests that might be using a local dirac.cfg
    gConfigurationData.localCFG=CFG()
    gConfigurationData.remoteCFG=CFG()
    gConfigurationData.mergedCFG=CFG()
    gConfigurationData.generateNewVersion()

class TestGetComponentSection( TestPathFinder ):

  def test_success( self ):
    result = getComponentSection('WorkloadManagement/SandboxStoreHandler',False, False,'Services')
    correctResult = '/Systems/WorkloadManagement/' + self.wm + '/Services/SandboxStoreHandler'
    self.assertEqual(result, correctResult)

  def test_sucessComponentStringDoesNotExist( self ):
    """ tricky case one could expect that if entity string is wrong
        than some kind of error will be returned, but it is not the case
    """
    result = getComponentSection('WorkloadManagement/SimpleLogConsumer',False, False,'NonRonsumersNon')
    correctResult = '/Systems/WorkloadManagement/' + self.wm + '/NonRonsumersNon/SimpleLogConsumer'
    self.assertEqual(result, correctResult)

class TestURLs( TestPathFinder ):

  def test_getServiceURLSimple( self ):
    """Fetching a URL defined normally"""
    result = getServiceURL('WorkloadManagement/Service1')
    correctResult = 'dips://server1:1234/WorkloadManagement/Service1'

    self.assertEqual(result, correctResult)

  def test_getServiceMainURL( self ):
    """Fetching a URL referencing the MainServers"""
    result = getServiceURL('WorkloadManagement/Service2')
    correctResult = 'dips://gw1:5678/WorkloadManagement/Service2,dips://gw2:5678/WorkloadManagement/Service2'
    self.assertEqual(result, correctResult)

  def test_getServiceFailoverURLNonExisting( self ):
    """Fetching a FailoverURL not defined"""
    result = getServiceFailoverURL('WorkloadManagement/Service1')
    correctResult = ''

    self.assertEqual(result, correctResult)

  def test_getServiceFailoverURL( self ):
    """Fetching a FailoverURL"""
    result = getServiceFailoverURL('WorkloadManagement/Service2')
    correctResult = 'dips://failover1:5678/WorkloadManagement/Service2'
    self.assertEqual(result, correctResult)

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestPathFinder )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestGetComponentSection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestURLs ) )

  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
