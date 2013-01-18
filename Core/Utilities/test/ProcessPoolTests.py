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
import os
import unittest
import random
import time
import threading

## from DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine()
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
## SUT
from DIRAC.Core.Utilities.ProcessPool import ProcessPool
import threading

def ResultCallback( task, taskResult ):
  """ dummy result callback """
  print "callback for %s result is %s" % ( task.getTaskID(), taskResult )

def ExceptionCallback( task, exec_info ):
  """ dummy exception callback """
  print "callback for %s exception is %s" % ( task.getTaskID(), exec_info )

def CallableFunc( taskID, timeWait, raiseException = False ):
  """ global function to be executed in task """
  print "pid=%s task=%s will sleep for %s s" % ( os.getpid(), taskID, timeWait )
  time.sleep( timeWait )
  if raiseException:
    raise Exception( "testException" )
  return timeWait

class CallableClass( object ):
  """ callable class to be executed in task """

  def __init__( self, taskID, timeWait, raiseException=False ):
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    self.log = gLogger.getSubLogger( self.__class__.__name__ + "/%s" % taskID )
    self.taskID = taskID
    self.timeWait = timeWait
    self.raiseException = raiseException
    
  def __call__( self ):
    import time, os
    self.log.always( "pid=%s task=%s will sleep for %s s" % ( os.getpid(), self.taskID, self.timeWait ) )
    time.sleep( self.timeWait )
    if self.raiseException:
      raise Exception("testException")
    return self.timeWait

## global locked lock 
gLock = threading.Lock()
# make sure it is locked
gLock.acquire()

## dummy callable locked class
class LockedCallableClass( object ):
  """ callable and locked class """
  def __init__( self, taskID, timeWait, raiseException=False ):
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    from DIRAC.FrameworkSystem.Client.Logger import gLogger
    self.log = gLogger.getSubLogger( self.__class__.__name__ + "/%s" % taskID )
    self.taskID = taskID
    self.log.always( "pid=%s task=%s I'm locked" % ( os.getpid(), self.taskID ) )
    gLock.acquire()
    self.log.always("you can't see that line, object is stuck by gLock" )
    self.timeWait = timeWait 
    self.raiseException = raiseException
    gLock.release()

  def __call__( self ):
    self.log.always("If you see this line, miracle had happened!")
    import time, os
    self.log.always("will sleep for %s" % self.timeWait )
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
    self.processPool = ProcessPool( 4, 8, 8 ) 
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
                                                      args = ( i, timeWait, raiseException ),  
                                                      callback = ResultCallback,
                                                      exceptionCallback = ExceptionCallback,
                                                      blocking = True )    
        if result["OK"]:
          self.log.always("CallableClass enqueued to task %s" % i )
          i += 1
        else:
          continue
      if i == 10:
        break
    self.processPool.finalize( 2 )

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
                                                      args = ( i, timeWait, raiseException ),  
                                                      callback = ResultCallback,
                                                      exceptionCallback = ExceptionCallback,
                                                      blocking = True )    
        if result["OK"]:
          self.log.always("CallableClass enqueued to task %s" % i )
          i += 1          
        else:
          continue
      if i == 10:
        break
    self.processPool.finalize( 2 )
    

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
    self.processPool = ProcessPool( 4, 8, 8,
                                    poolCallback = self.poolCallback, 
                                    poolExceptionCallback = self.poolExceptionCallback )
    self.processPool.daemonize()

  def poolCallback( self, taskID, taskResult ):
    self.log.always( "callback for %s result is %s" % ( taskID, taskResult ) ) 

  def poolExceptionCallback( self, taskID, taskException ):
    self.log.always( "callback for %s exception is %s" % ( taskID, taskException ) )

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
                                                      args = ( i, timeWait, raiseException ),  
                                                      usePoolCallbacks = True,
                                                      blocking = True )    
        if result["OK"]:
          self.log.always("CallableClass enqueued to task %s" % i )
          i += 1          
        else:
          continue
      if i == 10:
        break
    self.processPool.finalize( 2 )


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
                                                      args = ( i, timeWait, raiseException ),  
                                                      usePoolCallbacks = True,
                                                      blocking = True )    
        if result["OK"]:
          self.log.always("CallableFunc enqueued to task %s" % i )
          i += 1          
        else:
          continue
      if i == 10:
        break
    self.processPool.finalize( 2 )


########################################################################
class TaskTimeOutTests( unittest.TestCase ):
  """
  .. class:: TaskTimeOutTests

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
    self.processPool = ProcessPool( 2,
                                    4, 
                                    8,
                                    poolCallback = self.poolCallback, 
                                    poolExceptionCallback = self.poolExceptionCallback )
    self.processPool.daemonize()
    
  def poolCallback( self, taskID, taskResult ):
    self.log.always( "callback result for %s is %s" % ( taskID, taskResult )  ) 

  def poolExceptionCallback( self, taskID, taskException ): 
    self.log.always( "callback exception for %s is %s" % ( taskID, taskException ) )


  def testCallableClass( self ):
    """ CallableClass and task time out test """
    i = 0
    while True:
      if self.processPool.getFreeSlots() > 0:
        timeWait = random.randint( 0, 5 ) * 10
        raiseException = False
        if not timeWait:
          raiseException = True 
        result = self.processPool.createAndQueueTask( CallableClass,
                                                      taskID = i,
                                                      args = ( i, timeWait, raiseException ), 
                                                      timeOut = 15,
                                                      usePoolCallbacks = True,
                                                      blocking = True )    
        if result["OK"]:
          self.log.always("CallableClass enqueued to task %s timeWait=%s exception=%s" % ( i, timeWait, raiseException ) )
          i += 1
        else:
          continue
      if i == 16:
        break
    self.processPool.finalize( 2 )
    
  def testCallableFunc( self ):
    """ CallableFunc and task timeout test """
    i = 0
    while True:
      if self.processPool.getFreeSlots() > 0:
        timeWait = random.randint(0, 5) * 5
        raiseException = False
        if not timeWait:
          raiseException = True 
        result = self.processPool.createAndQueueTask( CallableFunc,
                                                      taskID = i,
                                                      args = ( i, timeWait, raiseException ),  
                                                      timeOut = 15,
                                                      usePoolCallbacks = True,
                                                      blocking = True )    
        if result["OK"]:
          self.log.always("CallableFunc enqueued to task %s timeWait=%s exception=%s" % ( i, timeWait, raiseException ) )
          i += 1
        else:
          continue
      if i == 16:
        break
    self.processPool.finalize( 2 )

  def testLockedClass( self ):
    """ LockedCallableClass and task time out test """

    for loop in range(2):
      self.log.always( "loop %s" % loop )
      i = 0
      while i < 16:
        if self.processPool.getFreeSlots() > 0:
          timeWait = random.randint(0, 5) * 5
          raiseException = False
          if timeWait == 5:
            raiseException = True
          klass = CallableClass
          if timeWait >= 20:
            klass = LockedCallableClass
          result = self.processPool.createAndQueueTask( klass,
                                                        taskID = i,
                                                        args = ( i, timeWait, raiseException ), 
                                                        timeOut = 15,
                                                        usePoolCallbacks = True,
                                                        blocking = True )    
          if result["OK"]:
            self.log.always("%s enqueued to task %s timeWait=%s exception=%s" % ( klass.__name__ , i, timeWait, raiseException ) )
            i += 1
          else:
            continue
      self.log.always("being idle for a while")
      for i in range(100000):
        for j in range(1000):
          pass

    self.log.always("finalizing...")
    self.processPool.finalize( 10 )
    ## unlock
    gLock.release()


## SUT suite execution
if __name__ == "__main__":

  testLoader = unittest.TestLoader()
  suitePPCT = testLoader.loadTestsFromTestCase( ProcessPoolCallbacksTests )  
  suiteTCT = testLoader.loadTestsFromTestCase( TaskCallbacksTests )
  suiteTTOT = testLoader.loadTestsFromTestCase( TaskTimeOutTests )
  suite = unittest.TestSuite( [ suitePPCT, suiteTCT, suiteTTOT ] )
  unittest.TextTestRunner(verbosity=3).run(suite)

