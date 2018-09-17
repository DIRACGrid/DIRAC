#!/bin/env python
"""
tests for PoolComputingElement module
"""

import unittest
import os
import time

from DIRAC.Resources.Computing.PoolComputingElement import PoolComputingElement

jobScript = """#!/usr/bin/env python

import time
import os

jobNumber = %s
stopFile = 'stop_job_' + str( jobNumber )
start = time.time()

print "Start job", jobNumber
while True:
  time.sleep( 0.1 )
  if os.path.isfile( stopFile ):
    os.unlink( stopFile )
    break
  if (time.time() - start) > 10:
    break
print "End job", jobNumber
"""

class PoolCETests( unittest.TestCase ):
  """ tests for the PoolComputingElement Module """

  def setUp( self ):
    ceParameters = { 'WholeNode': True,
                     'NumberOfProcessors': 4 }
    self.ce = PoolComputingElement('TestPoolCE')
    self.ce.setParameters( ceParameters )
    for i in range(4):
      with open( 'testPoolCEJob_%s.py' % i, 'w' ) as execFile:
        execFile.write( jobScript % i )
      os.chmod( 'testPoolCEJob_%s.py' % i, 0755  )

  def tearDown( self ):

    # Stop all the jobs if any, cleanup tmp files
    for i in range(4):
      self.__stopJob( i )
      for ff in [ 'testPoolCEJob_%s.py' % i, 'stop_job_%s' % i ]:
        if os.path.isfile( ff ):
          os.unlink( ff )

  def __stopJob( self, nJob ):
    with open( 'stop_job_%s' % nJob, 'w' ) as stopFile:
      stopFile.write( 'Stop' )
    time.sleep( 0.2 )
    if os.path.isfile( 'stop_job_%s' % nJob ):
      os.unlink( 'stop_job_%s' % nJob )

  def test_executeJob( self ):

    # Test that max 4 processors can be used at a time
    result = self.ce.submitJob( 'testPoolCEJob_0.py', None )
    self.assertTrue( result['OK'] )
    result = self.ce.getCEStatus()
    self.assertEqual( 1, result['UsedProcessors'] )

    jobParams = { 'numberOfProcessors': 2 }
    result = self.ce.submitJob( 'testPoolCEJob_1.py', None, **jobParams )
    self.assertTrue( result['OK'] )
    result = self.ce.getCEStatus()
    self.assertEqual( 3, result['UsedProcessors'] )

    jobParams = { 'numberOfProcessors': 2 }
    result = self.ce.submitJob( 'testPoolCEJob_2.py', None, **jobParams )
    self.assertTrue( not result['OK'] )
    self.assertIn( "Not enough slots", result['Message'] )

    self.__stopJob( 0 )
    jobParams = { 'numberOfProcessors': 2 }
    ce = PoolComputingElement('TestPoolCE')
    ceParameters = { 'WholeNode': False,
                     'NumberOfProcessors': 4 }
    ce.setParameters( ceParameters )
    result = ce.submitJob( 'testPoolCEJob_2.py', None, **jobParams )
    self.assertTrue( result['OK'] )
    result = ce.getCEStatus()
    self.assertEqual( 2, result['UsedProcessors'] )

    for i in range(4):
      self.__stopJob( i )
    time.sleep(1)
    result = self.ce.getCEStatus()
    self.assertEqual( 0, result['UsedProcessors'] )

    # Whole node jobs
    result = self.ce.submitJob( 'testPoolCEJob_0.py', None )
    self.assertTrue( result['OK'] )
    result = self.ce.getCEStatus()
    self.assertEqual( 1, result['UsedProcessors'] )

    jobParams = { 'wholeNode': True }
    result = self.ce.submitJob( 'testPoolCEJob_1.py', None, **jobParams )
    self.assertTrue( not result['OK'] )
    self.assertIn( "Can not take WholeNode job", result['Message'] )

    self.__stopJob( 0 )
    time.sleep(1)

    jobParams = { 'wholeNode': True }
    result = self.ce.submitJob( 'testPoolCEJob_1.py', None, **jobParams )
    self.assertTrue( result['OK'] )


if __name__ == '__main__':
  test = unittest.defaultTestLoader.loadTestsFromTestCase( PoolCETests )
  unittest.TextTestRunner( verbosity = 2 ).run( test )
