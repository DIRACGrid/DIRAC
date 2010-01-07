########################################################################
# $HeadURL:  $
########################################################################

import threading
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.Utilities.Utils import *
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *
from DIRAC.ResourceStatusSystem.Policy import Configurations

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/SeSInspectorAgent'

class SeSInspectorAgent(AgentModule):
  """ Class SeSInspectorAgent is in charge of going through Services
      table, and pass Service and Status to the PEP
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
      
      self.am_setOption( "PollingTime", 180 )
      self.ServicesToBeChecked = []
      self.ServiceNamesInCheck = []
      #self.maxNumberOfThreads = gConfig.getValue(self.section+'/NumberOfThreads',1)
      #self.threadPoolDepth = gConfig.getValue(self.section+'/ThreadPoolDepth',1)
      
      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
      #self.threadPool = ThreadPool(1,self.maxNumberOfThreads)
  
      #vedi taskQueueDirector
      self.threadPool = ThreadPool( self.am_getOption('minThreadsInPool'),
                         self.am_getOption('maxThreadsInPool'),
                         self.am_getOption('totalThreadsInPool') )
      if not self.threadPool:
        self.log.error('Can not create Thread Pool:')
        return
      
      self.lockObj = threading.RLock()
      
      return S_OK()
    
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)

#############################################################################

  def execute(self):
    """ The main SSInspectorAgent execution method
    """
    
    try:
      servicesGetter = ThreadedJob(self._getServicesToCheck)
      self.threadPool.queueJob(servicesGetter)
      
      for i in range(self.maxNumberOfThreads - 1):
        checkExecutor = ThreadedJob(self._executeCheck)
        self.threadPool.queueJob(checkExecutor)
    
      self.threadPool.processAllResults()
      return S_OK()

    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)
      
#############################################################################

  def _getServicesToCheck(self):
    """ 
    Call :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getServicesToCheck` 
    and put result in list
    """
    
    try:
      res = self.rsDB.getStuffToCheck('Services', maxN = self.maxNumberOfThreads - 1)
    except RSSDBException, x:
      gLogger.error(whoRaised(x))
    except RSSException, x:
      gLogger.error(whoRaised(x))

    for serviceTuple in res:
      if serviceTuple[0] in self.ServiceNamesInCheck:
        break
      serviceL = ['Service']
      for x in serviceTuple:
        serviceL.append(x)
      self.lockObj.acquire()
      try:
        self.ServiceNamesInCheck.insert(0, serviceL[1])
        self.ServicesToBeChecked.insert(0, serviceL)
      finally:
        self.lockObj.release()

#############################################################################

  def _executeCheck(self):
    """ 
    Create instance of a PEP, instantiated popping a service from lists.
    """
    
    if len(self.ServicesToBeChecked) > 0:
        
      self.lockObj.acquire()
      try:
        toBeChecked = self.ServicesToBeChecked.pop()
      finally:
        self.lockObj.release()
      
      granularity = toBeChecked[0]
      serviceName = toBeChecked[1]
      status = toBeChecked[2]
      formerStatus = toBeChecked[3]
      siteType = toBeChecked[4]
      serviceType = toBeChecked[5]
      
      gLogger.info("Checking Service %s, with status %s" % (serviceName, status))
      newPEP = PEP(granularity = granularity, name = serviceName, status = status, 
                   formerStatus = formerStatus, siteType = siteType, 
                   serviceType = serviceType)
      newPEP.enforce()

      self.lockObj.acquire()
      try:
        self.ServiceNamesInCheck.remove(serviceName)
      finally:
        self.lockObj.release()

#############################################################################