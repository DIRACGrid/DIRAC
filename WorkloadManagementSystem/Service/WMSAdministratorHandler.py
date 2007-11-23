########################################################################
# $Id: WMSAdministratorHandler.py,v 1.2 2007/11/23 16:39:43 atsareg Exp $
########################################################################
"""
This is a DIRAC WMS administrator interface

usage: WMSAdministrator <ini-file> ...

This starts an XMLRPC service exporting the following methods:

    setMask(<site mask>)
    getMask()
    getLCGOutput(jobid)
    getProxy(DN)
    getWMSStats()

"""

__RCSID__ = "$Id: WMSAdministratorHandler.py,v 1.2 2007/11/23 16:39:43 atsareg Exp $"

import os, sys, string, uu, shutil
from types import *

from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.ProxyRepositoryDB import ProxyRepositoryDB

# This is a global instance of the JobDB class
jobDB = False
proxyRepository = False

def initializeWMSAdministratorHandler( serviceInfo ):
  """  WMS AdministratorService initialization
  """

  global jobDB
  global proxyRepository

  jobDB = JobDB()
  proxyRepository = ProxyRepositoryDB()
  return S_OK()

class WMSAdministratorHandler(RequestHandler):

  def __init__(self,*args,**kargs):

    self.servercert = gConfig.getValue('/DIRAC/Security/CertFile',
                              '/opt/dirac/etc/grid-security/hostcert.pem')
    self.serverkey = gConfig.getValue('/DIRAC/Security/KeyFile',
                              '/opt/dirac/etc/grid-security/hostkey.pem')
    if os.environ.has_key('X509_USER_CERT'):
      self.servercert = os.environ['X509_USER_CERT']
    if os.environ.has_key('X509_USER_KEY'):
      self.serverkey = os.environ['X509_USER_KEY']

    RequestHandler.__init__(self,*args,**kargs)

###########################################################################
  types_setMask = [StringType]
  def export_setMask(self, mask):
    """ Set the site mask for matching. The mask is given in a form of Classad
        string.
    """

    result = jobDB.setMask(mask)
    return result

##############################################################################
  types_getMask = []
  def export_getMask(self):
    """ Get the site mask
    """

    result = jobDB.getMask()
    if result['Status'] == "OK":
      tmp_list = result['Value'].split('"')
      mask = []
      for i in range(1,len(tmp_list),2):
        mask.append(tmp_list[i])

      return S_OK(mask)
    else:
      return S_ERROR('Failed to get the mask from the Job DB')

##############################################################################
  types_banSite = [StringType]
  def export_banSite(self, site):
    """ Ban the given site in the site mask
    """

    result = jobDB.getMask()
    if  result['OK']:
      mask = result ['Value']
      ClassadMask  = Classad(mask)
      Requirements = ClassadMask.get_expression("Requirements")

      if string.find(Requirements, '|| Other.Site == "' + site+'"') >= 0:
        Requirements = string.replace (Requirements, '|| Other.Site == "' + site +'"', '')

        return jobDB.setMask('[Requirements='+Requirements +']')
      else:
        return S_OK()
    else:
      result = S_ERROR('Can not get the mask from the Job DB')


##############################################################################
  types_allowSite = [StringType]
  def export_allowSite(self,site):
    """ Allow the given site in the site mask
    """

    result = jobDB.getMask()
    if  result['OK']:
      mask = result ['Value']
      ClassadMask  = Classad(mask)
      Requirements = ClassadMask.get_expression("Requirements")

      if string.find(Requirements, '&& Other.Site != "' + site+'"') >= 0:
        Requirements = string.replace (Requirements, '&& Other.Site != "' + site +'"', '')

      elif string.find(Requirements, 'Other.Site != "' + site+'"') >= 0:
        Requirements = string.replace (Requirements, 'Other.Site != "' + site +'"', '')

      if string.find(Requirements, 'Other.Site == "' + site+'"') < 0:
        Requirements = Requirements + '|| Other.Site == "' + site +'"'

      return jobDB.setMask('[Requirements='+Requirements +']')

#            conditions   = string.split(ClassadMask.get_expression("Requirements"),'&&')
#            conjonction  = ''
#            Requirements = ''
#            for condition in conditions:
#                #condition  = string.split(condition ,'||') [0]
#                if string.find(condition, Site)<0:
#                     Requirements = conjonction + condition
#                     conjonction  = " && "
    return S_ERROR('Failed to access site mask')


##############################################################################
  types_clearMask = []
  def export_clearMask(self):
    """ Clear up the entire site mask
    """

    return jobDB.setMask("[Requirements = false]")

##############################################################################
  types_getProxy = [StringType,IntType]
  def export_getProxy(self,ownerDN,validity=1):
    """ Get a short user proxy from the central WMS repository for the user
        with DN ownerDN with the valididty period of <valididty> hours
    """
    result = jobDB.getTicketForDN(ownerDN)
    if not result['OK']:
      return result
    new_proxy = None
    user_proxy = result['Value']
    result = renewProxy(user_proxy)
    if result["OK"]:
      proxy = result["Value"]
      result = setupProxy(proxy)
      if result["OK"]:
        new_proxy,old_proxy = result["Value"]
        cmd = "voms-proxy-info --file %s -timeleft" % new_proxy
      else:
        restoreProxy(new_proxy,proxy)
        return S_ERROR("Can not set up proxy")
    if not new_proxy:
      result = S_OK(user_proxy)
      result['Message'] = 'Could not get myproxy delegation'
      return result
    else:
      return S_OK(new_proxy)

##############################################################################
  types_getLCGOutput = [IntType]
  def export_getLCGOutput(self,jobID):
    """ Get the LCG pilot standard output and standard error files for the DIRAC
        job identified by the jobID
    """

    result = jobDB.getLCGPilotOwnerDNForJob(jobID)
    if result['Status'] != "OK":
      return S_ERROR('Can not get the LCG job reference')

    lcg_reference = result['LCGJobReference']
    pilotDN = result['Value']

    result = jobDB.getTicketForDN(pilotDN)
    if result['Status'] != "OK":
      result = S_ERROR('Can not get the LCG job owner proxy')
      result['PilotOwnerDN'] = pilotDN
      return result

    proxy = result['Value']
    proxy = setupProxy(proxy)
    if proxy["Status"] == "OK":
      proxy_file = str(proxy["Value"][0])
      old_proxy_file = str(proxy["Value"][1])
    else:
      self.log.error("Can not setup owner proxy, proxy is not valid")

#    proxy_file,old_proxy_file = setupProxy(proxy)

    if not os.path.exists(str(jobID)):
      os.makedirs(str(jobID))
    result = getLCGOutput(lcg_reference,str(jobID))

    if old_proxy_file:
      os.environ['X509_USER_PROXY'] = old_proxy_file
    os.remove(proxy_file)

    if result['Status'] != "OK":
      result['PilotOwnerDN'] = pilotDN
      return result

    outputdir = result['Value']
    # Tar the output directory
    comm = "tar czvf %s.tar.gz -C %s %s" % (str(jobID),os.path.dirname(outputdir),os.path.basename(outputdir))
    #print comm
    status,out,error,pythonError = exeCommand(comm)
    if status != 0:
      return S_ERROR('Can not get the LCG output directory')

    uu.encode(str(jobID)+'.tar.gz',str(jobID)+'.tar.gz.bin')
    outfile = open(str(jobID)+'.tar.gz.bin','r')
    contents = outfile.read()
    outfile.close()
    os.remove(str(jobID)+'.tar.gz')
    os.remove(str(jobID)+'.tar.gz.bin')
    shutil.rmtree(str(jobID))
    result = S_OK(contents)
    result['PilotOwnerDN'] = pilotDN
    return result

  ##############################################################################
  types_getLCGSummary = [StringType,StringType]
  def export_getLCGSummary(self,startdate='',enddate=''):
    """ Get summary of the status of the LCG Pilot Jobs
    """

    print "----------------------- Inside getLCGSummary "

    result = jobDB.getLCGPilotSummary(startdate,enddate)
    return result


