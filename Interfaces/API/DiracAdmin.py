########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/DiracAdmin.py,v 1.8 2008/04/18 11:38:00 paterson Exp $
# File :   DiracAdmin.py
# Author : Stuart Paterson
########################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

"""DIRAC Administrator API Class

All administrative functionality is exposed through the DIRAC Admin API.  Examples include
site banning and unbanning, WMS proxy uploading etc.

"""

__RCSID__ = "$Id: DiracAdmin.py,v 1.8 2008/04/18 11:38:00 paterson Exp $"

import DIRAC
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.Core.Utilities.GridCredentials                import getGridProxy,getCurrentDN,setDIRACGroup
from DIRAC                                               import gConfig, gLogger, S_OK, S_ERROR

import re, os, sys, string, time, shutil, types
import pprint


COMPONENT_NAME='/Interfaces/API/DiracAdmin'

class DiracAdmin:

  #############################################################################
  def __init__(self):
    """Internal initialization of the DIRAC Admin API.
    """
    self.log = gLogger.getSubLogger('DIRACAdminAPI')
    self.site       = gConfig.getValue('/LocalSite/Site','Unknown')
    self.setup      = gConfig.getValue('/DIRAC/Setup','Unknown')
    self.section    = COMPONENT_NAME
    self.cvsVersion = 'CVS version '+__RCSID__
    self.diracInfo  = 'DIRAC version v%dr%d build %d' \
                       %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel)

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','DEBUG') == 'DEBUG':
      self.dbg = True

    self.scratchDir = gConfig.getValue(self.section+'/ScratchDir','/tmp')
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
#    diracAdmin = 'diracAdmin'
#    self.log.verbose('Setting DIRAC role for current proxy to %s' %diracAdmin)
#    setDIRACGroup(diracAdmin)
    self.currentDir = os.getcwd()
    self.pPrint = pprint.PrettyPrinter()

  #############################################################################
  def uploadProxy(self,group,permanent=True):
    """Upload a proxy to the DIRAC WMS.  This method

       Example usage:

       >>> print diracAdmin.uploadProxy('lhcb_pilot')
       {'OK': True, 'Value': 0L}

       @param group: DIRAC Group
       @type job: string
       @return: S_OK,S_ERROR

       @param permanent: Indefinitely update proxy
       @type permanent: boolean

    """
    proxy  = getGridProxy()
    proxy = open(proxy,'r').read()
    activeDN = getCurrentDN()
    dn = activeDN['Value']
    result = self.wmsAdmin.uploadProxy(proxy,dn,group)
    if not result['OK']:
      self.log.warn('Uploading proxy failed')
      self.log.warn(result)
      return result

    result = self.wmsAdmin.setProxyPersistencyFlag(permanent,dn,group)
    if not result['OK']:
      self.log.warn('Setting proxy update flag failed')
      self.log.warn(result)
    return result

  #############################################################################
  def getSiteMask(self):
    """Retrieve current site mask from WMS Administrator service.

       Example usage:

       >>> print diracAdmin.getSiteMask()
       {'OK': True, 'Value': 0L}

       @return: S_OK,S_ERROR

    """
    result = self.wmsAdmin.getSiteMask()
    if result['OK']:
      print self.pPrint.pformat(result['Value'])
    return result

  #############################################################################
  def getCSDict(self,sectionPath):
    """Retrieve a dictionary from the CS for the specified path.

       Example usage:

       >>> print diracAdmin.getCSPathDict('Resources/Computing/OSCompatibility')
       {'OK': True, 'Value': {'slc4_amd64_gcc34': 'slc4_ia32_gcc34,slc4_amd64_gcc34', 'slc4_ia32_gcc34': 'slc4_ia32_gcc34'}}

       @return: S_OK,S_ERROR

    """
    result = gConfig.getOptionsDict(sectionPath)
    return result

  #############################################################################
  def addSiteInMask(self,site):
    """Adds the site to the site mask.

       Example usage:

       >>> print diracAdmin.addSiteInMask()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    self.log.info('Allowing %s in site mask' % site)
    result = self.wmsAdmin.allowSite(site)
    return result

  #############################################################################
  def banSiteFromMask(self,site):
    """Removes the site from the site mask.

       Example usage:

       >>> print diracAdmin.banSiteFromMask()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    self.log.info('Removing %s from site mask' % site)
    result = self.wmsAdmin.banSite(site)
    return result

  #############################################################################
  def clearMask(self):
    """Removes all sites from the site mask.  Should be used with care.

       Example usage:

       >>> print diracAdmin.clearMask()
       {'OK': True, 'Value':''}

       @return: S_OK,S_ERROR

    """
    result = self.wmsAdmin.clearMask()
    return result

  #############################################################################
  def getProxy(self,ownerDN,ownerGroup,directory='',validity=12):
    """Retrieves a proxy with default 12hr validity from the WMS and stores
       this in a file in the local directory by default.  For scripting in python
       with this function, the X509_USER_PROXY environment variable is also set up.

       Example usage:

       >>> print diracAdmin.getProxy()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    if not directory:
      directory = self.currentDir

    if not os.path.exists(directory):
      self.__report('Directory %s does not exist' % directory)

    result = self.wmsAdmin.getProxy(ownerDN,ownerGroup,validity)
    if not result['OK']:
      self.log.warn('Problem retrieving proxy from WMS')
      self.log.warn(result['Message'])
      return result

    proxy = result['Value']
    if not proxy:
      self.log.warn('Null proxy returned from WMS Administrator')
      return result

    name = string.split(ownerDN,'=')[-1].replace(' ','').replace('/','')
    if not name:
      name = 'tempProxy'

    proxyPath = '%s/proxy%s' %(directory,name)
    if os.path.exists(proxyPath):
      os.remove(proxyPath)

    fopen = open(proxyPath,'w')
    fopen.write(proxy)
    fopen.close()

    os.putenv('X509_USER_PROXY',proxyPath)
    self.log.info('Proxy written to %s' %(proxyPath))
    self.log.info('Setting X509_USER_PROXY=%s' %(proxyPath))
    self.log.info('Adding DIRAC role %s to downloaded proxy for later use' %(ownerGroup))
    fd = file( proxyPath, "r" )
    contents = fd.readlines()
    fd.close()
    groupLine = ":::diracgroup=%s\n" % ownerGroup
    if contents[0].find( ":::diracgroup=" ) == 0:
      contents[0] = groupLine
    else:
      contents.insert( 0, groupLine )
    fd = file( proxyPath, "w" )
    fd.write( "".join( contents ) )
    fd.close()
    result = S_OK(proxyPath)
    return result

  #############################################################################
  def resetJob(self,jobID):
    """Reset a job or list of jobs in the WMS.  This operation resets the reschedule
       counter for a job or list of jobs and allows them to run as new.

       >>> print dirac.reset(12345)
       {'OK': True, 'Value': [12345]}

       @param job: JobID
       @type job: integer or list of integers
       @return: S_OK,S_ERROR

    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or convertible integer for existing jobID')
    elif type(jobID)==type([]):
      try:
        jobID = [int(job) for job in jobID]
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or convertible integer for existing jobIDs')

    result = self.wmsAdmin.resetJob(jobID)
    return result

  #############################################################################
  def getJobJDL(self,jobID):
    """Retrieve the JDL of an existing job in the WMS.

       >>> print dirac.getJobJDL(12345)
       {'OK': True, 'Value': [12345]}

       @param job: JobID
       @type job: integer or string
       @return: S_OK,S_ERROR
    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or convertible integer for existing jobID')

    result = self.monitoring.getJobJDL(jobID)
    return result

  #############################################################################
  def getJobPilotOutput(self,jobID,directory=''):
    """Retrieve the pilot output for an existing job in the WMS.
       The output will be retrieved in a local directory unless
       otherwise specified.

       >>> print dirac.getJobPilotOutput(12345)
       {'OK': True, StdOut:'',StdError:''}

       @param job: JobID
       @type job: integer or string
       @return: S_OK,S_ERROR
    """
    if not directory:
      directory = self.currentDir

    if not os.path.exists(directory):
      self.__report('Directory %s does not exist' % directory)

    result = self.wmsAdmin.getJobPilotOutput(jobID)
    if not result['OK']:
      return result

    outputPath = '%s/pilot_%s' %(directory,jobID)
    if os.path.exists(outputPath):
      self.log.info('Remove %s and retry to continue' %outputPath)
      return S_ERROR('Remove %s and retry to continue' %outputPath)

    if not os.path.exists(outputPath):
      self.log.verbose('Creating directory %s' %outputPath)
      os.mkdir(outputPath)

    outputs = result['Value']
    if outputs.has_key('StdOut'):
      stdout = '%s/std.out' %(outputPath)
      fopen = open(stdout,'w')
      fopen.write(outputs['StdOut'])
      fopen.close()
      self.log.verbose('Standard output written to %s' %(stdout))
    else:
      self.log.warn('No standard output returned')

    if outputs.has_key('StdError'):
      stderr = '%s/std.err' %(outputPath)
      fopen = open(stderr,'w')
      fopen.write(outputs['StdError'])
      fopen.close()
      self.log.verbose('Standard error written to %s' %(stderr))
    else:
      self.log.warn('No standard error returned')

    self.log.info('Outputs retrieved in %s' %outputPath)
    return result

  #############################################################################
  def getPilotOutput(self,gridReference,directory=''):
    """Retrieve the pilot output  (std.out and std.err) for an existing job in the WMS.

       >>> print dirac.getJobPilotOutput(12345)
       {'OK': True, 'Value': {}}

       @param job: JobID
       @type job: integer or string
       @return: S_OK,S_ERROR
    """
    if not type(gridReference)==type(" "):
      return self.__errorReport(str(x),'Expected string for pilot reference')

    if not directory:
      directory = self.currentDir

    if not os.path.exists(directory):
      self.__report('Directory %s does not exist' % directory)

    result = self.wmsAdmin.getPilotOutput(gridReference)
    if not result['OK']:
      return result

    gridReferenceSmall = string.split(gridReference,'/')[-1]
    if not gridReferenceSmall:
      gridReferenceSmall='reference'
    outputPath = '%s/pilot_%s' %(directory,gridReferenceSmall)

    if os.path.exists(outputPath):
      self.log.info('Remove %s and retry to continue' %outputPath)
      return S_ERROR('Remove %s and retry to continue' %outputPath)

    if not os.path.exists(outputPath):
      self.log.verbose('Creating directory %s' %outputPath)
      os.mkdir(outputPath)

    outputs = result['Value']
    if outputs.has_key('StdOut'):
      stdout = '%s/std.out' %(outputPath)
      fopen = open(stdout,'w')
      fopen.write(outputs['StdOut'])
      fopen.close()
      self.log.verbose('Standard output written to %s' %(stdout))
    else:
      self.log.warn('No standard output returned')

    if outputs.has_key('StdError'):
      stderr = '%s/std.err' %(outputPath)
      fopen = open(stderr,'w')
      fopen.write(outputs['StdError'])
      fopen.close()
      self.log.verbose('Standard error written to %s' %(stderr))
    else:
      self.log.warn('No standard error returned')

    self.log.info('Outputs retrieved in %s' %outputPath)
    return result

  #############################################################################
  def getJobPilots(self,jobID):
    """Extract the list of submitted pilots and their status for a given
       jobID from the WMS.  Useful information is printed to the screen.

       >>> print dirac.getJobPilots()
       {'OK': True, 'Value': {PilotID:{StatusDict}}}

       @param job: JobID
       @type job: integer or string
       @return: S_OK,S_ERROR

    """
    if type(jobID)==type(" "):
      try:
        jobID = int(jobID)
      except Exception,x:
        return self.__errorReport(str(x),'Expected integer or string for existing jobID')

    result = self.wmsAdmin.getPilots(jobID)
    if result['OK']:
      print self.pPrint.pformat(result['Value'])
    return result

  #############################################################################
  def getPilotSummary(self,startDate='',endDate=''):
    """Retrieve the pilot output for an existing job in the WMS.  Summary is
       printed at INFO level, full dictionary of results also returned.

       >>> print dirac.getPilotSummary()
       {'OK': True, 'Value': {CE:{Status:Count}}}

       @param job: JobID
       @type job: integer or string
       @return: S_OK,S_ERROR
    """
    result = self.wmsAdmin.getPilotSummary(startDate,endDate)
    if not result['OK']:
      return result

    ceDict = result['Value']
    headers = 'CE'.ljust(28)
    i = 0
    for ce,summary in ceDict.items():
      states = summary.keys()
      if len(states)>i:
        i = len(states)

    for i in xrange(i):
      headers += 'Status'.ljust(12)+'Count'.ljust(12)
    print headers

    for ce,summary in ceDict.items():
      line = ce.ljust(28)
      states = summary.keys()
      states.sort()
      for state in states:
        count = str(summary[state])
        line += state.ljust(12)+count.ljust(12)
      print line

    return result

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn(error)
    return S_ERROR(message)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#