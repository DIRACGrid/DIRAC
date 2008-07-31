########################################################################
# $Id: WMSAdministratorHandler.py,v 1.34 2008/07/31 10:47:40 rgracian Exp $
########################################################################
"""
This is a DIRAC WMS administrator interface.
It exposes the following methods:

Site mask related methods:
    setMask(<site mask>)
    getMask()

Access to the pilot data:
    getWMSStats()

"""

__RCSID__ = "$Id: WMSAdministratorHandler.py,v 1.34 2008/07/31 10:47:40 rgracian Exp $"

import os, sys, string, uu, shutil
from types import *

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.Service.WMSUtilities import *
import DIRAC.Core.Utilities.Time as Time

import threading

# This is a global instance of the database classes
jobDB = False
pilotDB = False


def initializeWMSAdministratorHandler( serviceInfo ):
  """  WMS AdministratorService initialization
  """

  global jobDB
  global pilotDB

  jobDB = JobDB()
  pilotDB = PilotAgentsDB()
  return S_OK()

class WMSAdministratorHandler(RequestHandler):

###########################################################################
  types_setMask = [StringType]
  def export_setSiteMask(self, siteList):
    """ Set the site mask for matching. The mask is given in a form of Classad
        string.
    """
    result = self.getRemoteCredentials()
    dn = result['DN']

    maskList = [ (site,'Active') for site in siteList ]
    result = jobDB.setSiteMask(maskList,dn)
    return result

##############################################################################
  types_getSiteMask = []
  def export_getSiteMask(self):
    """ Get the site mask
    """

    result = jobDB.getSiteMask('Active')
    return result

    if result['Status'] == "OK":
      active_list = result['Value']
      mask = []
      for i in range(1,len(tmp_list),2):
        mask.append(tmp_list[i])

      return S_OK(mask)
    else:
      return S_ERROR('Failed to get the mask from the Job DB')

##############################################################################
  types_banSite = [StringType]
  def export_banSite(self, site,comment='No comment'):
    """ Ban the given site in the site mask
    """

    result = self.getRemoteCredentials()
    dn = result['DN']
    result = jobDB.banSiteInMask(site,dn,comment)
    return result

##############################################################################
  types_allowSite = [StringType]
  def export_allowSite(self,site,comment='No comment'):
    """ Allow the given site in the site mask
    """

    result = self.getRemoteCredentials()
    dn = result['DN']
    result = jobDB.allowSiteInMask(site,dn,comment)
    return result

##############################################################################
  types_clearMask = []
  def export_clearMask(self):
    """ Clear up the entire site mask
    """

    return jobDB.removeSiteFromMask("All")

##############################################################################
  types_getPilotOutput = [StringType]
  def export_getPilotOutput(self,pilotReference):
    """ Get the pilot job standard output and standard error files for the Grid
        job reference
    """

    return self.__getGridJobOutput(pilotReference)

  ##############################################################################
  types_getJobPilotOutput = [IntType]
  def export_getJobPilotOutput(self,jobID):
    """ Get the pilot job standard output and standard error files for the DIRAC
        job reference
    """

    # Get the pilot grid reference first
    result = jobDB.getJobParameter(jobID,'Pilot_Reference')
    if not result['OK']:
      return result

    pilotReference = result['Value']
    if pilotReference:
      return self.__getGridJobOutput(pilotReference)
    else:
      return S_ERROR('No pilot job reference found')

  ##############################################################################
  def __getGridJobOutput(self,pilotReference):
    """ Get the pilot job standard output and standard error files for the Grid
        job reference
    """

    result = pilotDB.getPilotInfo(pilotReference)
    if not result['OK'] or not result[ 'Value' ]:
      return S_ERROR('Failed to determine owner for pilot ' + pilotReference)

    pilotDict = result['Value'][pilotReference]
    owner = pilotDict['OwnerDN']
    group = pilotDict['OwnerGroup']

    # FIXME: What if the OutputSandBox is not StdOut and StdErr
    result = pilotDB.getPilotOutput(pilotReference)
    if result['OK']:
      stdout = result['Value']['StdOut']
      error = result['Value']['StdError']
      if stdout or error:
        resultDict = {}
        resultDict['StdOut'] = stdout
        resultDict['StdError'] = error
        resultDict['OwnerDN'] = owner
        resultDict['OwnerGroup'] = group
        resultDict['FileList'] = []
        return S_OK(resultDict)

    ret = gProxyManager.getPilotProxyFromVOMSGroup( owner, group )
    if not ret['OK']:
      gLogger.error( ret['Message'] )
      gLogger.error( 'Could not get proxy:', 'User "%s", Group "%s"' % ( owner, group ) )
      return S_ERROR("Failed to get the pilot's owner proxy")
    proxy = ret['Value']

    gridType = pilotDict['GridType']

    result = getPilotOutput( proxy, gridType, pilotReference )

    if not result['OK']:
      return S_ERROR('Failed to get pilot output: '+result['Message'])
    # FIXME: What if the OutputSandBox is not StdOut and StdErr
    stdout = result['StdOut']
    error = result['StdError']
    fileList = result['FileList']
    result = pilotDB.storePilotOutput(pilotReference,stdout,error)
    if not result['OK']:
      gLogger.error('Failed to store pilot output:',result['Message'])

    resultDict = {}
    resultDict['StdOut'] = stdout
    resultDict['StdError'] = error
    resultDict['OwnerDN'] = owner
    resultDict['OwnerGroup'] = group
    resultDict['FileList'] = fileList
    return S_OK(resultDict)

  ##############################################################################
  types_getPilotSummary = []
  def export_getPilotSummary(self,startdate='',enddate=''):
    """ Get summary of the status of the LCG Pilot Jobs
    """

    result = pilotDB.getPilotSummary(startdate,enddate)
    return result

  ##############################################################################
  types_getPilots = [IntType]
  def export_getPilots(self,jobID):
    """ Get pilot references and their statuses for those submitted for the given job
    """

    result = pilotDB.getPilotsForJob(jobID)
    if not result['OK']:
      return S_ERROR('Failed to get pilots: '+result['Message'])

    pilots = result['Value']
    result = pilotDB.getPilotInfo(pilots)
    return result

  ##############################################################################
  types_setJobForPilot = [IntType, StringType]
  def export_setJobForPilot(self,jobID,pilotRef):
    """ Report the DIRAC job ID which is executed by the given pilot job
    """

    result = pilotDB.setJobForPilot(jobID,pilotRef)
    if not result['OK']:
      return result
    result = pilotDB.setCurrentJobID(pilotRef,jobID)
    return result

  ##########################################################################################
  types_setPilotBenchmark = [StringType, FloatType]
  def export_setPilotBenchmark(self,pilotRef,mark):
    """ Set the pilot agent benchmark
    """
    result = pilotDB.setPilotBenchmark(pilotRef,mark)
    return result

