########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/DiracAdmin.py,v 1.24 2008/08/25 18:46:54 paterson Exp $
# File :   DiracAdmin.py
# Author : Stuart Paterson
########################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

"""DIRAC Administrator API Class

All administrative functionality is exposed through the DIRAC Admin API.  Examples include
site banning and unbanning, WMS proxy uploading etc.

"""

__RCSID__ = "$Id: DiracAdmin.py,v 1.24 2008/08/25 18:46:54 paterson Exp $"

import DIRAC
from DIRAC.ConfigurationSystem.Client.CSAPI              import CSAPI
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.Core.Utilities.SiteCEMapping                  import getCEsForSite,getSiteCEMapping
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
    self.csAPI      = CSAPI()

    self.dbg = False
    if gConfig.getValue(self.section+'/LogLevel','DEBUG') == 'DEBUG':
      self.dbg = True

    self.scratchDir = gConfig.getValue(self.section+'/ScratchDir','/tmp')
    self.currentDir = os.getcwd()
    self.pPrint = pprint.PrettyPrinter()

  #############################################################################
  def setProxyPersistency( self, userDN, userGroup, persistent = True ):
    """Set the persistence of a proxy in the Proxy Manager

       Example usage:

       >>> print diracAdmin.setProxyPersistency( 'some DN', 'dirac group', True )
       {'OK': True }

       @param userDN: User DN
       @type userDN: string
       @param userGroup: DIRAC Group
       @type userGroup: string
       @param persistent: Persistent flag
       @type persistent: boolean
       @return: S_OK,S_ERROR
    """
    return gProxyManager.setPersistency( userDN, userGroup, persistent )

  #############################################################################
  def checkProxyUploaded( self, userDN, userGroup, requiredTime ):
    """Set the persistence of a proxy in the Proxy Manager

       Example usage:

       >>> print diracAdmin.setProxyPersistency( 'some DN', 'dirac group', True )
       {'OK': True, 'Value' : True/False }

       @param userDN: User DN
       @type userDN: string
       @param userGroup: DIRAC Group
       @type userGroup: string
       @param requiredTime: Required life time of the uploaded proxy
       @type requiredTime: boolean
       @return: S_OK,S_ERROR
    """
    return gProxyManager.userHasProxy( userDN, userGroup, requiredTime )

  #############################################################################
  def getSiteMask(self):
    """Retrieve current site mask from WMS Administrator service.

       Example usage:

       >>> print diracAdmin.getSiteMask()
       {'OK': True, 'Value': 0L}

       @return: S_OK,S_ERROR

    """
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getSiteMask()
    if result['OK']:
      sites = result['Value']
      sites.sort()
      for site in sites:
        print site
    #TODO, add printOutput flag
    return result

  #############################################################################
  def getBannedSites(self,gridType='LCG',printOutput=False):
    """Retrieve current list of banned sites.

       Example usage:

       >>> print diracAdmin.getBannedSites()
       {'OK': True, 'Value': []}

       @return: S_OK,S_ERROR

    """
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getSiteMask()
    bannedSites = []
    if not result['OK']:
      self.log.warn(result['Message'])
      return result

    totalList = gConfig.getSections('/Resources/Sites/%s' %gridType)['Value']
    sites = result['Value']
    for site in totalList:
      if not site in sites:
        bannedSites.append(site)
    bannedSites.sort()
    if printOutput:
      print string.join(bannedSites,'\n')
    return S_OK(bannedSites)

  #############################################################################
  def getSiteSection(self,site,printOutput=False):
    """Simple utility to get the list of CEs for DIRAC site name.

       Example usage:

       >>> print diracAdmin.getSiteSection('LCG.CERN.ch')
       {'OK': True, 'Value':}

       @return: S_OK,S_ERROR
    """
    gridType = site.split('.')[0]
    if not gConfig.getSections('/Resources/Sites/%s' %(gridType))['OK']:
      return S_ERROR('/Resources/Sites/%s is not a valid site section' %(gridType))

    result = self.getCSDict('/Resources/Sites/%s/%s' %(gridType,site))
    if printOutput and result['OK']:
      print self.pPrint.pformat(result['Value'])
    return result

  #############################################################################
  def getCSDict(self,sectionPath):
    """Retrieve a dictionary from the CS for the specified path.

       Example usage:

       >>> print diracAdmin.getCSDict('Resources/Computing/OSCompatibility')
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
    result = self.__checkSiteIsValid(site)
    if not result['OK']:
      return result
    self.log.info('Allowing %s in site mask' % site)
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.allowSite(site)
    return result

  #############################################################################
  def banSiteFromMask(self,site):
    """Removes the site from the site mask.

       Example usage:

       >>> print diracAdmin.banSiteFromMask()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    result = self.__checkSiteIsValid(site)
    if not result['OK']:
      return result
    self.log.info('Removing %s from site mask' % site)
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.banSite(site)
    return result

  #############################################################################
  def __checkSiteIsValid(self,site):
    """Internal function to check that a site name is valid.
    """
    sites = getSiteCEMapping()
    if not sites['OK']:
      return S_ERROR('Could not get site CE mapping')
    siteList = sites['Value'].keys()
    if not site in siteList:
      return S_ERROR('Specified site %s is not in list of defined sites' %site)

    return S_OK('%s is valid' %site)

  #############################################################################
  def clearMask(self):
    """Removes all sites from the site mask.  Should be used with care.

       Example usage:

       >>> print diracAdmin.clearMask()
       {'OK': True, 'Value':''}

       @return: S_OK,S_ERROR

    """
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.clearMask()
    return result

  #############################################################################
  def getServicePorts(self,setup='',printOutput=False):
    """Checks the service ports for the specified setup.  If not given this is
       taken from the current installation (/DIRAC/Setup)

       Example usage:

       >>> print diracAdmin.getServicePorts()
       {'OK': True, 'Value':''}

       @return: S_OK,S_ERROR

    """
    if not setup:
      setup = gConfig.getValue('/DIRAC/Setup','')

    setupList = gConfig.getSections('/DIRAC/Setups',[])
    if not setupList['OK']:
      return S_ERROR('Could not get /DIRAC/Setups sections')
    setupList = setupList['Value']
    if not setup in setupList:
      return S_ERROR('Setup %s is not in allowed list: %s' %(setup,string.join(setupList,', ')))

    serviceSetups = gConfig.getOptionsDict('/DIRAC/Setups/%s' %setup)
    if not serviceSetups['OK']:
      return S_ERROR('Could not get /DIRAC/Setups/%s options' %setup)
    serviceSetups = serviceSetups['Value'] #dict
    systemList = gConfig.getSections('/Systems')
    if not systemList['OK']:
      return S_ERROR('Could not get Systems sections')
    systemList = systemList['Value']
    result = {}
    for system in systemList:
      if serviceSetups.has_key(system):
        path = '/Systems/%s/%s/Services' %(system,serviceSetups[system])
        servicesList = gConfig.getSections(path)
        if not servicesList['OK']:
          self.log.warn('Could not get sections in %s' %path)
        else:
          servicesList = servicesList['Value']
          if not servicesList:
            servicesList=[]
          self.log.verbose('System: %s ServicesList: %s' %(system,string.join(servicesList,', ')))
          for service in servicesList:
            spath = '%s/%s/Port' %(path,service)
            servicePort = gConfig.getValue(spath,0)
            if servicePort:
              self.log.verbose('Found port for %s/%s = %s' %(system,service,servicePort))
              result['%s/%s' %(system,service)] = servicePort
            else:
              self.log.warn('No port found for %s' %spath)
      else:
        self.log.warn('%s is not defined in /DIRAC/Setups/%s' %(system,setup))

    if printOutput:
      print self.pPrint.pformat(result)

    return S_OK(result)

  #############################################################################
  def getProxy( self, userDN, userGroup, validity=43200, limited = False ):
    """Retrieves a proxy with default 12hr validity and stores
       this in a file in the local directory by default.

       Example usage:

       >>> print diracAdmin.getProxy()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    return gProxyManager.downloadProxy( userDN, userGroup, limited = limited,
                                        requiredTimeLeft = validity )

  #############################################################################
  def getVOMSProxy( self, userDN, userGroup, vomsAttr = False, validity=43200, limited = False ):
    """Retrieves a proxy with default 12hr validity and VOMS extensions and stores
       this in a file in the local directory by default.

       Example usage:

       >>> print diracAdmin.getVOMSProxy()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """
    return gProxyManager.downloadVOMSProxy( userDN, userGroup, limited = limited,
                                            requiredVOMSAttribute = vomsAttr,
                                            requiredTimeLeft = validity )

  #############################################################################
  def getPilotProxy( self, userDN, userGroup, validity=43200 ):
    """Retrieves a pilot proxy with default 12hr validity and stores
       this in a file in the local directory by default.

       Example usage:

       >>> print diracAdmin.getVOMSProxy()
       {'OK': True, 'Value': }

       @return: S_OK,S_ERROR

    """

    return gProxyManager.getPilotProxyFromDIRACGroup( userDN, userGroup, requiredTimeLeft = validity )

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

    jobManager = RPCClient('WorkloadManagement/JobManager',useCertificates=False)
    result = jobManager.resetJob(jobID)
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

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getJobPilotOutput(jobID)
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

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getPilotOutput(gridReference)
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

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getPilots(jobID)
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
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getPilotSummary(startDate,endDate)
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
  def selectRequests(self,JobID=None,RequestID=None,RequestName=None,RequestType=None,Status=None,Operation=None,OwnerDN=None,OwnerGroup=None,RequestStart=0,Limit=100,printOutput=False):
    """ Select requests from the request management system. A few notes on the selection criteria:
        - RequestID is assigned during submission of the request
        - JobID is the WMS JobID for the request (if applicable)
        - RequestName is the corresponding XML file name
        - RequestType e.g. 'transfer'
        - Status e.g. Done
        - Operation e.g. replicateAndRegister
        - RequestStart e.g. the first request to consider (start from 0 by default)
        - Limit e.g. selection limit (default 100)

       >>> dirac.selectRequests(JobID='4894')
       {'OK': True, 'Value': [[<Requests>]]}
    """
    options = {'RequestID':RequestID,'RequestName':RequestName,'JobID':JobID,'OwnerDN':OwnerDN,
               'OwnerGroup':OwnerGroup,'RequestType':RequestType,'Status':Status,'Operation':Operation}

    conditions = {}
    for n,v in options.items():
      if v:
        try:
          conditions[n] = str(v)
        except Exception,x:
          return self.__errorReport(str(x),'Expected string for %s field' %n)

    try:
      RequestStart = int(RequestStart)
      Limit = int(Limit)
    except Exception,x:
      return self.__errorReport(str(x),'Expected integer for %s field' %n)

    self.log.verbose('Will select requests with the following conditions')
    self.log.verbose(self.pPrint.pformat(conditions))
    requestClient = RPCClient("RequestManagement/centralURL")
    result = requestClient.getRequestSummaryWeb(conditions,[],RequestStart,Limit)
    if not result['OK']:
      self.log.warn(result['Message'])
      return result

    requestIDs = result['Value']
    conds = []
    for n,v in conditions.items():
      if v:
        conds.append('%s = %s' %(n,v))
    self.log.verbose('%s request(s) selected with conditions %s and limit %s' %(len(requestIDs['Records']),string.join(conds,', '),Limit))
    if printOutput:
      requests = []
      if len(requestIDs['Records'])>Limit:
        requestList = requestIDs['Records']
        requests = requestList[:Limit]
      else:
        requests = requestIDs['Records']
      print '%s request(s) selected with conditions %s and limit %s' %(len(requestIDs['Records']),string.join(conds,', '),Limit)
      print requestIDs['ParameterNames']
      for request in requests:
        print request
    if not requestIDs:
      return S_ERROR('No requests selected for conditions: %s' %conditions)
    else:
      return result

  #############################################################################
  def getRequestSummary(self,printOutput=False):
    """ Get a summary of the requests in the request DB.
    """
    requestClient = RPCClient("RequestManagement/centralURL")
    result = requestClient.getDBSummary()
    if not result['OK']:
      self.log.warn(result['Message'])
      return result

    if printOutput:
      print self.pPrint.pformat(result['Value'])

    return result

  #############################################################################
  def getExternalPackageVersions(self):
    """ Simple function that attempts to obtain the external versions for
        the local DIRAC installation (frequently needed for debugging purposes).
    """
    gLogger.info('DIRAC version v%dr%d build %d' %(DIRAC.majorVersion,DIRAC.minorVersion,DIRAC.patchLevel))
    try:
      import lcg_util
      infoStr = 'Using lcg_util from: \n%s' % lcg_util.__file__
      gLogger.info(infoStr)
      infoStr = "The version of lcg_utils is %s" % lcg_util.lcg_util_version()
      gLogger.info(infoStr)
    except Exception,x:
      errStr = "SRM2Storage.__init__: Failed to import lcg_util: %s" % (x)
      gLogger.exception(errStr)

    try:
      import gfalthr as gfal
      infoStr = "Using gfalthr from: \n%s" % gfal.__file__
      gLogger.info(infoStr)
      infoStr = "The version of gfalthr is %s" % gfal.gfal_version()
      gLogger.info(infoStr)
    except Exception,x:
      errStr = "SRM2Storage.__init__: Failed to import gfalthr: %s." % (x)
      gLogger.warn(errStr)
      try:
        import gfal
        infoStr = "Using gfal from: %s" % gfal.__file__
        gLogger.info(infoStr)
        infoStr = "The version of gfal is %s" % gfal.gfal_version()
        gLogger.info(infoStr)
      except Exception,x:
        errStr = "SRM2Storage.__init__: Failed to import gfal: %s" % (x)
        gLogger.exception(errStr)


    defaultProtocols = gConfig.getValue('/Resources/StorageElements/DefaultProtocols',[])
    gLogger.info('Default list of protocols are: %s' %(string.join(defaultProtocols,', ')))
    return S_OK()

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn(error)
    return S_ERROR(message)

  #############################################################################
  def csModifyValue(self,optionPath,newValue):
    """Function to modify an existing value in the CS.
    """
    return self.csAPI.modifyValue(optionPath,newValue)

  #############################################################################
  def csRegisterUser( self, username, properties ):
    """Registers a user in the CS.
        - username: Username of the user (easy;)
        - properties: Dict containing:
            - DN
            - groups : list/tuple of groups the user belongs to
            - <others> : More properties of the user, like mail
    """
    return self.csAPI.addUser( username, properties )

  #############################################################################
  def csDeleteUser( self, user ):
    """Deletes a user from the CS. Can take a list of users
    """
    return self.csAPI.deleteUsers( user )

  #############################################################################
  def csModifyUser( self, username, properties, createIfNonExistant = False ):
    """Modify a user in the CS. Takes the same params as in addUser and applies
      the changes
    """
    return self.csAPI.modifyUser( username, properties, createIfNonExistant )

  #############################################################################
  def csListUsers( self, group = False ):
    """Lists the users in the CS. If no group is specified return all users.
    """
    return self.csAPI.listUsers( group )

  #############################################################################
  def csDescribeUsers( self, mask = False ):
    """List users and their properties in the CS.
        If a mask is given, only users in the mask will be returned
    """
    return self.csAPI.describeUsers( mask )

  #############################################################################
  def csListHosts( self ):
    """Lists the hosts in the CS
    """
    return self.csAPI.listHosts()

  #############################################################################
  def csDescribeHosts( self, mask = False ):
    """Gets extended info for the hosts in the CS
    """
    return self.csAPI.describeHosts( mask )

  #############################################################################
  def csListGroups( self ):
    """Lists groups in the CS
    """
    return self.csAPI.listGroups()

  #############################################################################
  def csDescribeGroups( self, mask = False ):
    """List groups and their properties in the CS.
        If a mask is given, only groups in the mask will be returned
    """
    return self.csAPI.describeGroups( mask )

  #############################################################################
  def csSyncUsersWithCFG( self, usersCFG ):
    """Synchronize users in cfg with its contents
    """
    return self.csAPI.syncUsersWithCFG( usersCFG )

  #############################################################################
  def csCommitChanges(self,sortUsers=True):
    """Commit the changes in the CS
    """
    return self.csAPI.commitChanges(sortUsers=False)

  #############################################################################
  def _promptUser(self,message):
    """Internal function to pretty print an object.
    """
    self.log.verbose('%s %s' %(message,'[yes/no] : '))
    response = raw_input('%s %s' %(message,'[yes/no] : '))
    responses = ['yes','y','n','no']
    if not response.strip() or response=='\n':
      self.log.info('Possible responses are: %s' %(string.join(responses,', ')))
      response = raw_input('%s %s' %(message,'[yes/no] : '))

    if not response.strip().lower() in responses:
      self.log.info('Problem interpreting input "%s", assuming negative response.' %(response))
      return S_ERROR(response)

    if response.strip().lower()=='y' or response.strip().lower()=='yes':
      return S_OK(response)
    else:
      return S_ERROR(response)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#