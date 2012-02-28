########################################################################
# $HeadURL $
# File: RequestTaskTests.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2011/10/12 14:23:12
########################################################################

""" :mod: RequestTaskTests 
    =======================
 
    .. module: RequestTaskTests
    :synopsis: unittets for RequestTask
    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    unittets for RequestTask
"""

__RCSID__ = "$Id $"

##
# @file RequestTaskTests.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2011/10/12 14:23:23
# @brief Definition of RequestTaskTests class.

## imports 
import unittest
import pickle
import multiprocessing
import Queue
import sys
import time
import threading

import string 
from mock import *

## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR, rootPath
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.Base.AgentModule import AgentModule 
from DIRAC.DataManagementSystem.private.RequestAgentBase import RequestAgentBase
from DIRAC.Core.Utilities.ProcessPool import ProcessTask, ProcessPool
from DIRAC.DataManagementSystem.private.RequestTask import RequestTask
from DIRAC.Core.DISET.RPCClient import RPCClient

from DIRAC.DataManagementSystem.test.InheritedTask import InheritedTask

## test agent name
AGENT_NAME = "DataManagement/InheritedAgent"

class InheritedTask( RequestTask ):

  def __init__( self, *args, **kwargs ):
    RequestTask.__init__( self, args, *kwargs )
  
  def __call__( self ):
    self.always( "in call of %s" % str(self ) )
    self.addMark( "akey", 10 )
    return S_OK( self.monitor() )

class InheritedAgent( RequestAgentBase ):
  
  def initialize( self ):
    self.setRequestTask( InheritedTask )
    gLogger.always( self.configPath() )
    return S_OK()

########################################################################
class RequestTaskTests( unittest.TestCase ):
  """
  .. class:: RequestTaskTests
  
  """

  __processPool = None

  def setUp( self ):
    """ setup

    :param self: self reference
    """
    if not self.__processPool:
      self.__processPool = ProcessPool(2, 5, 5, strictLimits = True)
    self.__processPool.daemonize()
    self.kwargs = { "configPath" : "/a/b/c",
                    "requestName" : "fake name",
                    "requestString" : "",
                    "jobID" : 0,
                    #"executionOrder" : [],
                    "sourceServer" : "" }
    print "IDLE: ", self.__processPool.getNumIdleProcesses()

  def tearDown( self ):
    if self.__processPool:
      while self.__processPool.hasPendingTasks() or self.__processPool.isWorking():
        self.__processPool.processResults()
        time.sleep( 5 )
      if not self.__processPool.isWorking():
        self.__processPool = None

  def test__01_createTask( self ):
    """ test InheritedTask c'tor

    """
    self.assertEqual( isinstance( InheritedTask( **self.kwargs ), InheritedTask ), True, "c'tor not working" )
    self.assertEqual( issubclass( InheritedTask, RequestTask ), True, "bad inheritance" )
    
  def test__02_queueTask( self ):
    """ ProcessPool.queueTask test

    """
    processTask = ProcessTask( InheritedTask, kwargs = self.kwargs )
    self.assertEqual( isinstance( processTask, ProcessTask ), True )
    self.__processPool.queueTask( processTask, blocking  = True )
    self.__processPool.processResults()

  def test__03_createAndQueueTask( self ):
    """ ProcessPool.createAndQueueTask test

    """
    self.__processPool.createAndQueueTask( InheritedTask, 
                                           kwargs = self.kwargs, 
                                           blocking = True )
    self.__processPool.processResults()

class RequestAgentBaseTests( unittest.TestCase ):
  """
  .. class:: RequestAgentBaseTests

  """
  def setUp( self ):
    """ setup
    :param self: self reference
    """
    self.agent = InheritedAgent( AGENT_NAME )
    self.agent.getRequest = Mock()
    self.agent.getRequest.return_value = { "OK" : True, "Value" :  { "configPath" : "/a/b/c",
                                                                     "requestName" : "fake name",
                                                                     "requestString" : "",
                                                                     "jobID" : 0,
                                                                     "executionOrder" : 0,
                                                                     "sourceServer" : "" } }

  def test__01_execute( self ):
    self.agent.initialize()
    print "init done"
    self.agent.execute()
    print "execute done"
    self.agent.finalize()
    print "finalize done"


## tests execution
if __name__ == "__main__":
  
  testLoader = unittest.TestLoader()
  suiteRABT = testLoader.loadTestsFromTestCase( RequestAgentBaseTests )     
  suiteRT = testLoader.loadTestsFromTestCase( RequestTaskTests )
  suite = unittest.TestSuite( [ suiteRT ] )
  unittest.TextTestRunner(verbosity=3).run(suite)
