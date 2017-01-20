""" Unit tests for PathFinder only for functions that I added
"""

import unittest
from DIRAC.ConfigurationSystem.Client.PathFinder import getComponentSection
from DIRAC import gConfig
import os

from DIRAC.ConfigurationSystem.private.ConfigurationClient import ConfigurationClient

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
        }
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

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestPathFinder )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestGetComponentSection ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
