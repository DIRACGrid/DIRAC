########################################################################
# $HeadURL$
########################################################################

"""  Zuzia picks out the requests that she finds then tries to put them in the central server.

  OBSOLETE
  K.C.
"""

from DIRAC  import gLogger, gConfig, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.ConfigurationSystem.Client import PathFinder

import time,os,re
from types import *

__RCSID__ = "$Id$"

AGENT_NAME = 'RequestManagement/ZuziaAgent'

class ZuziaAgent(AgentModule):

  def initialize(self):

    self.RequestDBClient = RequestClient()

    gMonitor.registerActivity("Iteration",          "Agent Loops",                  "ZuziaAgent",      "Loops/min",      gMonitor.OP_SUM)
    gMonitor.registerActivity("Attempted",          "Request Processed",            "ZuziaRAgent",     "Requests/min",   gMonitor.OP_SUM)
    gMonitor.registerActivity("Successful",         "Request Forward Successful",   "ZuziaAgent",      "Requests/min",   gMonitor.OP_SUM)
    gMonitor.registerActivity("Failed",             "Request Forward Failed",       "ZuziaAgent",      "Requests/min",   gMonitor.OP_SUM)

    self.local = PathFinder.getServiceURL("RequestManagement/localURL")
    if not self.local:
      self.local = AgentModule.am_getOption(self,'localURL','')
    if not self.local:
      errStr = 'The RequestManagement/localURL option must be defined.'
      gLogger.fatal(errStr)
      return S_ERROR(errStr)

    self.central = PathFinder.getServiceURL("RequestManagement/centralURL")
    if not self.central:
      errStr = 'The RequestManagement/centralURL option must be defined.'
      gLogger.fatal(errStr)
      return S_ERROR(errStr)
    return S_OK()

  def execute(self):
    """ This agent is the smallest and (cutest) of all the DIRAC agents in existence.
    """
    gMonitor.addMark("Iteration",1)

    central = PathFinder.getServiceURL("RequestManagement/centralURL")
    if central:
      self.central = central
    local = PathFinder.getServiceURL("RequestManagement/localURL")
    if local:
      self.local = local

    res = self.RequestDBClient.serveRequest(url=self.local)
    if not res['OK']:
      gLogger.error("ZuziaAgent.execute: Failed to get request from database.",self.local)
      return S_OK()
    elif not res['Value']:
      gLogger.info("ZuziaAgent.execute: No requests to be executed found.")
      return S_OK()

    gMonitor.addMark("Attempted",1)

    requestString = res['Value']['RequestString']
    requestName = res['Value']['RequestName']
    gLogger.info("ZuziaAgent.execute: Obtained request %s" % requestName)

    gLogger.info("ZuziaAgent.execute: Attempting to set request to %s." % self.central)
    res = self.RequestDBClient.setRequest(requestName,requestString,self.central)
    if res['OK']:
      gMonitor.addMark("Successful",1)
      gLogger.info("ZuziaAgent.execute: Successfully put request.")
    else:
      gMonitor.addMark("Failed",1)
      gLogger.error("ZuziaAgent.execute: Failed to set request to", self.central)
      gLogger.info("ZuziaAgent.execute: Attempting to set request to %s." % self.local)
      res = self.RequestDBClient.setRequest(requestName,requestString,self.local)
      if res['OK']:
        gLogger.info("ZuziaAgent.execute: Successfully put request.")
      else:
        gLogger.error("ZuziaAgent.execute: Failed to set request to", self.local)
        while not res['OK']:
          gLogger.info("ZuziaAgent.execute: Attempting to set request to anywhere.")
          res = self.RequestDBClient.setRequest(requestName,requestString)
        gLogger.info("ZuziaAgent.execute: Successfully put request to %s." % res["Server"])
    return S_OK()
