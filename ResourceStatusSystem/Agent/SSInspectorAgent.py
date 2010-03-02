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

AGENT_NAME = 'ResourceStatus/SSInspectorAgent'

class SSInspectorAgent(AgentModule):
  """ Class SSInspectorAgent is in charge of going through Sites
      table, and pass Site and Status to the PEP
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
      
      self.SitesToBeChecked = []
      self.SiteNamesInCheck = []
      
      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )

      self.threadPool = ThreadPool( self.am_getOption('minThreadsInPool', 1),
                                    self.maxNumberOfThreads )
      if not self.threadPool:
        self.log.error('Can not create Thread Pool:')
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
    """ The main SSInspectorAgent execution method
    """
    
    try:

      self._getSitesToCheck()

      for i in range(self.maxNumberOfThreads):
        self.lockObj.acquire()
        try:
          toBeChecked = self.SitesToBeChecked.pop()
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

  def _getSitesToCheck(self):
    """ 
    Call :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getSitesToCheck` 
    and put result in list
    """
    
    try:
      try:
        res = self.rsDB.getStuffToCheck('Sites', Configurations.Sites_check_freq)
      except RSSDBException, x:
        gLogger.exception(whoRaised(x))
      except RSSException, x:
        gLogger.exception(whoRaised(x))
  
      for siteTuple in res:
        if siteTuple[0] in self.SiteNamesInCheck:
          break
        siteL = ['Site']
        for x in siteTuple:
          siteL.append(x)
        self.lockObj.acquire()
        try:
          self.SiteNamesInCheck.insert(0, siteL[1])
          self.SitesToBeChecked.insert(0, siteL)
        finally:
          self.lockObj.release()
    
    except Exception, x:
      gLogger.exception(whoRaised(x),'',x)

#############################################################################

  def _executeCheck(self, toBeChecked):
    """ 
    Create istance of a PEP, instantiated popping a site from lists.
    """
    
    try:
    
      while True:
      
        granularity = toBeChecked[0]
        siteName = toBeChecked[1]
        status = toBeChecked[2]
        formerStatus = toBeChecked[3]
        siteType = toBeChecked[4]
        
        gLogger.info("Checking Site %s, with status %s" % (siteName, status))
        newPEP = PEP(granularity = granularity, name = siteName, status = status, 
                     formerStatus = formerStatus, siteType = siteType)
        newPEP.enforce(rsDBIn = self.rsDB, setupIn = self.setup)
    
        # remove from InCheck list
        self.lockObj.acquire()
        try:
          self.SiteNamesInCheck.remove(toBeChecked[1])
        finally:
          self.lockObj.release()

        # get new site to be checked 
        self.lockObj.acquire()
        try:
          toBeChecked = self.SitesToBeChecked.pop()
        except Exception:
          break
        finally:
          self.lockObj.release()
        
    
    except Exception, x:
      gLogger.exception(whoRaised(x),'',x)
      self.lockObj.acquire()
      try:
        self.SiteNamesInCheck.remove(siteName)
      except IndexError:
        pass
      finally:
        self.lockObj.release()

#############################################################################
