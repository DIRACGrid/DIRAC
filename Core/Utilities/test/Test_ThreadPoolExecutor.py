"""
.. module:: Test_ThreadPoolExecutor
Unit test for DIRAC.Core.Utilities.ThreadPoolExecutor

"""

import unittest
import random
import time
import functools
import os

from DIRAC import gLogger
from DIRAC.Core.Utilities.ThreadPoolExecutor import ThreadPoolExecutor

def testFunc( timeWait ):
  """ function to be executed by a future """
  print "pid=%s will sleep for %s s" % ( os.getpid(), timeWait )
  time.sleep( timeWait )
  return timeWait

def testCallback( timeWait, futures ):
  print "callback: pid=%s will sleep for %s s" % ( os.getpid(), timeWait )
    
class ThreadPoolExecutorTests( unittest.TestCase ):
  
  def setUp( self ):
    gLogger.showHeaders( True )
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.threadPool = ThreadPoolExecutor( 4 ) 
    
  def test_generateJobAndQueueItI( self ):  
    for _ in xrange( 20 ):
      timetoWait = random.randint( 0, 10 )
      self.threadPool.generateJobAndQueueIt( testFunc, args = ( timetoWait, ) )

    self.assertEqual( 4, self.threadPool.numWorkingThreads() )
    self.assert_( not self.threadPool.numWaitingThreads () < 4 )
    self.assertEqual( self.threadPool.getMaxThreads(), 4 )
  
  def test_generateJobAndQueueItICallback( self ):
    for i in xrange( 20 ):
      timetoWait = random.randint( 0, 10 )
      self.threadPool.generateJobAndQueueIt( testFunc, args = ( timetoWait, ) )
      if i % 2:
        self.threadPool.generateJobAndQueueIt( testFunc, args = ( timetoWait, ), oCallback = functools.partial( testCallback, "Future:%s" % timetoWait ) )
      else:
        self.threadPool.generateJobAndQueueIt( testFunc, args = ( timetoWait, ) )

    self.assertEqual( 4, self.threadPool.numWorkingThreads() )
    self.assert_( not self.threadPool.numWaitingThreads () < 4 )
    self.assertEqual( self.threadPool.getMaxThreads(), 4 )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ThreadPoolExecutorTests )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
    
