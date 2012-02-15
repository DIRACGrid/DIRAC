########################################################################
# $HeadURL $
# File: ProcessPoolTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2012/02/13 07:55:31
########################################################################

""" :mod: ProcessPoolTests 
    =======================
 
    .. module: ProcessPoolTests
    :synopsis: unit tests for ProcessPool
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unit tests for ProcessPool
"""

__RCSID__ = "$Id $"

##
# @file ProcessPoolTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/02/13 07:55:46
# @brief Definition of ProcessPoolTests class.

## imports 
import unittest
import random
import time

## from DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine()
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
## SUT
from DIRAC.Core.Utilities.ProcessPool import ProcessPool

def ResultCallback( task, taskResult ):
  """ dummy result callback """
  print "results callback for task %s" % task.getTaskID()

def ExceptionCallback( task, exec_info ):
  """ dummy exception callback """
  print "exception callback for task %s" % task.getTaskID()

def CallableFunc( timeWait, raiseException = False ):
  """ global function to be executed in task """
  time.sleep( timeWait )
  if raiseException:
    raise Exception( "testException" )
  return timeWait

class CallableClass( object ):
  """ callable class to be executed in task """

  def __init__( self, timeWait, raiseException=False ):
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.timeWait = timeWait
    self.raiseException = raiseException
  def __call__( self ):
    import time
    self.log.always( "will sleep for %s s" % self.timeWait )
    time.sleep( self.timeWait )
    if self.raiseException:
      raise Exception("testException")
    return self.timeWait
  
########################################################################
class TaskCallbacksTests(unittest.TestCase):
  """
  .. class:: TaskCallbacksTests
  test case for ProcessPool
  """

  def setUp( self ):
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    gLogger.showHeaders( True )
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.processPool = ProcessPool() 
    self.processPool.daemonize()

  def testCallableClass( self ):
    """ CallableClass and task callbacks test """
    i = 0
    while True:
      if self.processPool.getFreeSlots() > 0:
        timeWait = random.randint(0, 5)
        raiseException = False
        if not timeWait:
          raiseException = True 
        result = self.processPool.createAndQueueTask( CallableClass,
                                                      taskID = i,
                                                      args = ( timeWait, raiseException ),  
                                                      callback = ResultCallback,
                                                      exceptionCallback = ExceptionCallback,
                                                      blocking = True )    
        if result["OK"]:
          i += 1
          self.log.always("CallableClass enqueued to task %s" % i )
        else:
          continue
      if i == 10:
        break
    self.processPool.processAllResults() 
    self.processPool.finalize()

  def testCallableFunc( self ):
    """ CallableFunc and task callbacks test """
    i = 0
    while True:
      if self.processPool.getFreeSlots() > 0:
        timeWait = random.randint(0, 5)
        raiseException = False
        if not timeWait:
          raiseException = True 
        result = self.processPool.createAndQueueTask( CallableFunc,
                                                      taskID = i,
                                                      args = ( timeWait, raiseException ),  
                                                      callback = ResultCallback,
                                                      exceptionCallback = ExceptionCallback,
                                                      blocking = True )    
        if result["OK"]:
          i += 1
          self.log.always("CallableClass enqueued to task %s" % i )
        else:
          continue
      if i == 10:
        break
    self.processPool.processAllResults() 
    self.processPool.finalize()


########################################################################
class ProcessPoolCallbacksTests( unittest.TestCase ):
  """
  .. class:: ProcessPoolCallbacksTests
  test case for ProcessPool
  """

  def setUp( self ):
    """c'tor

    :param self: self reference
    """
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    gLogger.showHeaders( True )
    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.processPool = ProcessPool( poolCallback = self.poolCallback, 
                                    poolExceptionCallback = self.poolExceptionCallback )
    self.processPool.daemonize()
    
  def poolCallback( self, task ):
    self.log.always( "result callback executed for task %s" % task.getTaskID() )
  
  def poolExceptionCallback( self, task ):
    self.log.always( "exception callback executed for task %s" % task.getTaskID() )
  
  def testCallableClass( self ):
    """ CallableClass and pool callbacks test """
    i = 0
    while True:
      if self.processPool.getFreeSlots() > 0:
        timeWait = random.randint(0, 5)
        raiseException = False
        if not timeWait:
          raiseException = True 
        result = self.processPool.createAndQueueTask( CallableClass,
                                                      taskID = i,
                                                      args = ( timeWait, raiseException ),  
                                                      usePoolCallbacks = True,
                                                      blocking = True )    
        if result["OK"]:
          i += 1
          self.log.always("CallableClass enqueued to task %s" % i )
        else:
          continue
      if i == 10:
        break
    self.processPool.processAllResults() 
    self.processPool.finalize()


  def testCallableFunc( self ):
    """ CallableFunc and pool callbacks test """
    i = 0
    while True:
      if self.processPool.getFreeSlots() > 0:
        timeWait = random.randint(0, 5)
        raiseException = False
        if not timeWait:
          raiseException = True 
        result = self.processPool.createAndQueueTask( CallableFunc,
                                                      taskID = i,
                                                      args = ( timeWait, raiseException ),  
                                                      usePoolCallbacks = True,
                                                      blocking = True )    
        if result["OK"]:
          i += 1
          self.log.always("CallableClass enqueued to task %s" % i )
        else:
          continue
      if i == 10:
        break
    self.processPool.processAllResults() 
    self.processPool.finalize()


## SUT suite execution
if __name__ == "__main__":

  testLoader = unittest.TestLoader()
  suitePPCT = testLoader.loadTestsFromTestCase( ProcessPoolCallbacksTests )  
  suiteTCT = testLoader.loadTestsFromTestCase( TaskCallbacksTests )
  suite = unittest.TestSuite( [ suitePPCT, suiteTCT ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

