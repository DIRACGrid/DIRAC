########################################################################
# $HeadURL:  $
########################################################################

import threading
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool,ThreadedJob
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

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
      
      self.ServicesToBeChecked = []
      self.ServiceNamesInCheck = []
      
      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
  
      self.threadPool = ThreadPool( self.am_getOption('minThreadsInPool', 1),
                                    self.maxNumberOfThreads )
      if not self.threadPool:
        self.log.error('Can not create Thread Pool:')
        return S_ERROR('Can not create Thread Pool')
      
      
      self.lockObj = threading.RLock()

      self.setup = gConfig.getValue("DIRAC/Setup")
      
      self.nc = NotificationClient()

      self.diracAdmin = DiracAdmin()

      self.csAPI = CSAPI()      
    
      return S_OK()
    
#    except Exception, x:
#      errorStr = where(self, self.execute)
#      gLogger.exception(errorStr,'',x)
#      return S_ERROR(errorStr)

    except Exception:
      errorStr = "SeSInspectorAgent initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)


#############################################################################

  def execute(self):
    """ The main SSInspectorAgent execution method
    """
    
    try:

      self._getServicesToCheck()

      for i in range(self.maxNumberOfThreads):
        self.lockObj.acquire()
        try:
          toBeChecked = self.ServicesToBeChecked.pop()
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

  def _getServicesToCheck(self):
    """ 
    Call :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getServicesToCheck` 
    and put result in list
    """
    
    try:
      try:
        res = self.rsDB.getStuffToCheck('Services', Configurations.Services_check_freq)
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

#    except Exception, x:
#      gLogger.exception(whoRaised(x),'',x)
    except Exception:
      errorStr = "SeSInspectorAgent _getResourcesToCheck"
      gLogger.exception(errorStr)

#############################################################################

  def _executeCheck(self, toBeChecked):
    """ 
    Create instance of a PEP, instantiated popping a service from lists.
    """
    
    try:
    
      while True:
      
        granularity = toBeChecked[0]
        serviceName = toBeChecked[1]
        status = toBeChecked[2]
        formerStatus = toBeChecked[3]
        siteType = toBeChecked[4]
        serviceType = toBeChecked[5]
        operatorCode = toBeChecked[6]
        
        gLogger.info("Checking Service %s, with status %s" % (serviceName, status))

        newPEP = PEP(granularity = granularity, name = serviceName, status = status, 
                     formerStatus = formerStatus, siteType = siteType, 
                     serviceType = serviceType, operatorCode = operatorCode)
        
        newPEP.enforce(rsDBIn = self.rsDB, setupIn = self.setup, ncIn = self.nc, 
                       daIn = self.diracAdmin, csAPIIn = self.csAPI)
    

        # remove from InCheck list
        self.lockObj.acquire()
        try:
          self.ServiceNamesInCheck.remove(toBeChecked[1])
        finally:
          self.lockObj.release()

        # get new service to be checked 
        self.lockObj.acquire()
        try:
          toBeChecked = self.ServicesToBeChecked.pop()
        except Exception:
          break
        finally:
          self.lockObj.release()
        
    
    except Exception, x:
      gLogger.exception('SeSInspector._executeCheck')
      self.lockObj.acquire()
      try:
        self.ServiceNamesInCheck.remove(serviceName)
      except IndexError:
        pass
      finally:
        self.lockObj.release()

#############################################################################