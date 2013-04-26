import mock

import sys
if sys.version_info < ( 2, 7 ):
  import unittest2 as unittest
else:
  import unittest

import DIRAC.WorkloadManagementSystem.Executor.JobScheduling as sut
from DIRAC.FrameworkSystem.private.logging.Logger import Logger

from DIRAC import gLogger

class ExecutorTestCase( unittest.TestCase ):

  def setUp( self ):

    # reload SoftwareUnderTest to drop old patchers
    reload( sut )

    patcher = mock.patch( 'DIRAC.WorkloadManagementSystem.Executor.Base.OptimizerExecutor', autospec = True )
    pStarted = patcher.start()
    class OptimizerMocked():
      def __init__( self, *args, **kwargs ):
        for k, v in pStarted.__dict__.iteritems():
          setattr( self, k, v )
#    OptimizerMocked.JobLog = Logger
#    OptimizerMocked.__jobData = mock.Mock()
#    OptimizerMocked.__jobData.jobLog = gLogger
    sut.OptimizerExecutor = OptimizerMocked
    sut.JobScheduling.__super__ = ( OptimizerMocked, )

    self.js = sut.JobScheduling()
#    def retF( f ):
#      print "AAAAAAA"
#      return {'LCG.CERN.ch':0, 'LCG.CSCS.ch':1}
#    self.js.getSiteTier = retF
#    self.js.jobLog = gLogger

  def tearDown( self ):

    # Stop patchers
    mock.patch.stopall()

class JobSchedulingSuccess( ExecutorTestCase ):
  def test__getJobSite( self ):
    res = self.js._getJobSite( ['CNAF'] )
    self.assertEqual( res, 'CNAF' )
    res = self.js._getJobSite( [] )
    self.assertEqual( res, 'ANY' )
    res = self.js._getJobSite( [''] )
    self.assertEqual( res, 'ANY' )

    res = self.js._getJobSite( ['CNAF', 'CERN'] )
    self.assertEqual( res, 'Multiple' )


    def retF( f ):
      return {'LCG.CERN.ch':0, 'LCG.CSCS.ch':2}

    self.js._getSiteTiers = retF
    res = self.js._getJobSite( ['CNAF', 'CSCS'] )
    self.assertEqual( res, 'Group.CERN.ch' )

    def retF2( f ):
      return {'LCG.CERN.ch':0, 'LCG.CNAF.it':1}

    self.js._getSiteTiers = retF2
    res = self.js._getJobSite( ['CNAF', 'CSCS'] )
    self.assertEqual( res, 'Group.CERN.ch' )

    def retF3( f ):
      return {'LCG.CERN.ch':1, 'LCG.CNAF.it':1}

    self.js._getSiteTiers = retF3
    res = self.js._getJobSite( ['CNAF', 'CSCS'] )
    self.assertEqual( res, 'Multiple' )

    def retF4( f ):
      return {'LCG.CERN.ch':0, 'LCG.CNAF.it':0}

    self.js._getSiteTiers = retF4
    res = self.js._getJobSite( ['CNAF', 'CSCS'] )
    self.assertEqual( res, 'Multiple' )

    def retF5( f ):
      return {'LCG.CERN.ch':0, 'LCG.CNAF.it':0, 'LCG.CSCS.ch':2}

    self.js._getSiteTiers = retF5
    res = self.js._getJobSite( ['CNAF', 'CSCS'] )
    self.assertEqual( res, 'Multiple' )

    def retF6( f ):
      return {'LCG.CERN.ch':0, 'LCG.CNAF.it':1, 'LCG.CSCS.ch':2}

    self.js._getSiteTiers = retF6
    res = self.js._getJobSite( ['CNAF', 'CSCS'] )
    self.assertEqual( res, 'Group.CERN.ch' )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ExecutorTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobSchedulingSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
