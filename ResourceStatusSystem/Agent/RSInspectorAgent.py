########################################################################
# $HeadURL:  $
########################################################################

import threading
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

__RCSID__ = "$Id:  $"

AGENT_NAME = 'ResourceStatus/RSInspectorAgent'

class RSInspectorAgent(AgentModule):
  """ Class RSInspectorAgent is in charge of going through Resources
      table, and pass Resource and Status to the PEP
  """

#############################################################################

  def initialize(self):
    """ Standard constructor
    """
    
    try:
      try:
        self.rsDB = ResourceStatusDB()
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
      
      self.am_setOption( "PollingTime", 60 )
      self.ResToBeChecked = []
      self.ResNamesInCheck = []
      
      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
  
      #vedi taskQueueDirector
      self.threadPool = ThreadPool( self.am_getOption('minThreadsInPool', 1),
                                    self.maxNumberOfThreads )
      if not self.threadPool:
        self.log.error('Can not create Thread Pool')
        return S_ERROR('Can not create Thread Pool')
      
      self.lockObj = threading.RLock()
      
      self.setup = gConfig.getValue("DIRAC/Setup")
      
      return S_OK()
    
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr,'',x)
      return S_ERROR(errorStr)

#############################################################################

  def execute(self):
    """ The main RSInspectorAgent execution method
    """
    
    try:

      self._getResourcesToCheck()

      for i in range(self.maxNumberOfThreads):
        self.lockObj.acquire()
        try:
          toBeChecked = self.ResourcesToBeChecked.pop()
        except Exception:
          break
        finally:
          self.lockObj.release()
        
        self.threadPool.generateJobAndQueueIt(self._executeCheck, args = (toBeChecked, ) )
        
      
      self.threadPool.processAllResults()
      return S_OK()

    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)
      
#############################################################################

  def _getResourcesToCheck(self):
    """ 
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getResourcesToCheck` 
    and put result in a list
    """

    try:
    
      try:
        res = self.rsDB.getStuffToCheck('Resources', Configurations.Resources_check_freq) 
      except RSSDBException, x:
        gLogger.error(whoRaised(x))
      except RSSException, x:
        gLogger.error(whoRaised(x))
   
      for resourceTuple in res:
        if resourceTuple[0] in self.ResNamesInCheck:
          break
        resourceL = ['Resource']
        for x in resourceTuple:
          resourceL.append(x)
        self.lockObj.acquire()
        try:
          self.ResNamesInCheck.insert(0, resourceL[1])
          self.ResToBeChecked.insert(0, resourceL)
        finally:
          self.lockObj.release()

    except Exception, x:
      gLogger.exception(whoRaised(x),'',x)

        
#############################################################################

  def _executeCheck(self):
    """ 
    Create instance of a PEP, instantiated popping a resource from lists.
    """
    
    try:
    
      while True:
      
        granularity = toBeChecked[0]
        resourceName = toBeChecked[1]
        status = toBeChecked[2]
        formerStatus = toBeChecked[3]
        siteType = toBeChecked[4]
        resourceType = toBeChecked[5]
        
        gLogger.info("Checking Resource %s, with status %s" % (resourceName, status))
        newPEP = PEP(granularity = granularity, name = resourceName, status = status, 
                     formerStatus = formerStatus, siteType = siteType, 
                     resourceType = resourceType)
        newPEP.enforce(rsDBIn = self.rsDB, setupIn = self.setup)
    
        # remove from InCheck list
        self.lockObj.acquire()
        try:
          self.ResourceNamesInCheck.remove(toBeChecked[1])
        finally:
          self.lockObj.release()

        # get new site to be checked 
        self.lockObj.acquire()
        try:
          toBeChecked = self.ResourcesToBeChecked.pop()
        except Exception:
          break
        finally:
          self.lockObj.release()
        
    
    except Exception, x:
      gLogger.exception(whoRaised(x),'',x)
      self.lockObj.acquire()
      try:
        self.ResourceNamesInCheck.remove(resourceeName)
      except IndexError:
        pass
      finally:
        self.lockObj.release()

#############################################################################    