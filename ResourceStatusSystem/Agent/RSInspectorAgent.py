########################################################################
# $HeadURL:  $
########################################################################

import copy
import Queue
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient

from DIRAC.ResourceStatusSystem.Utilities.Exceptions import *
from DIRAC.ResourceStatusSystem.PolicySystem.PEP import PEP
from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import *

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
      self.rsDB = ResourceStatusDB()
      
      self.ResourcesToBeChecked = Queue.Queue()
      self.ResourceNamesInCheck = []
      
      self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
      self.threadPool = ThreadPool( self.maxNumberOfThreads,
                                    self.maxNumberOfThreads )

      if not self.threadPool:
        self.log.error('Can not create Thread Pool')
        return S_ERROR('Can not create Thread Pool')
      
      self.setup = gConfig.getValue("DIRAC/Setup")
      
      self.VOExtension = gConfig.getValue("DIRAC/Extensions")

      if 'LHCb' in self.VOExtension:
        self.VOExtension = 'LHCb'

      configModule = __import__(self.VOExtension+"DIRAC.ResourceStatusSystem.Policy.Configurations", 
                                globals(), locals(), ['*'])
      
      self.Resources_check_freq = copy.deepcopy(configModule.Resources_check_freq)
      
      self.nc = NotificationClient()

      self.diracAdmin = DiracAdmin()

      self.csAPI = CSAPI()      
      
      for i in range(self.maxNumberOfThreads):
        self.threadPool.generateJobAndQueueIt(self._executeCheck, args = (None, ) )  
        
      return S_OK()

    except Exception:
      errorStr = "RSInspectorAgent initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  def execute(self):
    """ 
    The main RSInspectorAgent execution method.
    Calls :meth:`DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.getResourcesToCheck` and 
    put result in self.ResourcesToBeChecked (a Queue) and in self.ResourceNamesInCheck (a list)
    """
    
    try:

      res = self.rsDB.getStuffToCheck('Resources', self.Resources_check_freq) 
   
      for resourceTuple in res:
        if resourceTuple[0] in self.ResourceNamesInCheck:
          break
        resourceL = ['Resource']
        for x in resourceTuple:
          resourceL.append(x)
        self.ResourceNamesInCheck.insert(0, resourceL[1])
        self.ResourcesToBeChecked.put(resourceL)

      return S_OK()

    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr,lException=x)
      return S_ERROR(errorStr)
      
        
#############################################################################

  def _executeCheck(self, arg):
    """ 
    Create instance of a PEP, instantiated popping a resource from lists.
    """
    
    
    while True:
      
      try:
      
        toBeChecked = self.ResourcesToBeChecked.get()
      
        granularity = toBeChecked[0]
        resourceName = toBeChecked[1]
        status = toBeChecked[2]
        formerStatus = toBeChecked[3]
        siteType = toBeChecked[4]
        resourceType = toBeChecked[5]
        operatorCode = toBeChecked[6]
        
        gLogger.info("Checking Resource %s, with status %s" % (resourceName, status))
        
        newPEP = PEP(self.VOExtension, granularity = granularity, name = resourceName, 
                     status = status, formerStatus = formerStatus, siteType = siteType, 
                     resourceType = resourceType, operatorCode = operatorCode)
        
        newPEP.enforce(rsDBIn = self.rsDB, setupIn = self.setup, ncIn = self.nc, 
                       daIn = self.diracAdmin, csAPIIn = self.csAPI)
    
        # remove from InCheck list
        self.ResourceNamesInCheck.remove(toBeChecked[1])

      except Exception:
        gLogger.exception('RSInspector._executeCheck')
        try:
          self.ResourceNamesInCheck.remove(resourceName)
        except IndexError:
          pass

#############################################################################    
