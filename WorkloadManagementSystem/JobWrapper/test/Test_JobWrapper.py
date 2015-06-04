""" Test class for JobWrapper
"""

# imports
import unittest
import importlib

from mock import MagicMock

from DIRAC import gLogger, S_OK
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper

class JobWrapperTestCase( unittest.TestCase ):
  """ Base class for the JobWrapper test cases
  """
  def setUp( self ):
    gLogger.setLevel( 'DEBUG' )

    self.mockDM = MagicMock()
    self.mockDM.getReplicas.return_value = S_OK( {'Successful': {'/a/lfn/1.txt':{'SE1':'/a/lfn/at/SE1.1.txt',
                                                                                 'SE2':'/a/lfn/at/SE2.1.txt'},
                                                                 '/a/lfn/2.txt':{'SE1':'/a/lfn/at/SE1.1.txt'}},
                                                  'Failed':{}} )
    self.mockFC = MagicMock()
    self.mockFC.getFileMetadata.return_value = S_OK( {'Successful': {'/a/lfn/1.txt':{'GUID':'AABB11'},
                                                                     '/a/lfn/2.txt':{'GUID':'AABB22'}},
                                                      'Failed':{}} )
  def tearDown( self ):
    pass


class JobWrapperTestCaseSuccess( JobWrapperTestCase ):

  def test_InputData( self ):
    myJW = importlib.import_module( 'DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper' )
    myJW.getSystemSection = MagicMock()
    myJW.ModuleFactory = MagicMock()

    jw = JobWrapper()

    jw.jobArgs['InputData'] = ''
    res = jw.resolveInputData()
    self.assertFalse( res['OK'] )

    jw = JobWrapper()
    jw.jobArgs['InputData'] = 'pippo'
    jw.dm = self.mockDM
    jw.fc = self.mockFC
    res = jw.resolveInputData()
    self.assert_( res['OK'] )

    jw = JobWrapper()
    jw.jobArgs['InputData'] = 'pippo'
    jw.jobArgs['LocalSE'] = 'mySE'
    jw.jobArgs['InputDataModule'] = 'aa.bb'
    jw.dm = self.mockDM
    jw.fc = self.mockFC
    res = jw.resolveInputData()
    self.assert_( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( JobWrapperTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobWrapperTestCaseSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
