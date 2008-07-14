########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Interfaces/API/DiracAdmin.py,v 1.19 2008/07/14 18:16:48 acasajus Exp $
# File :   DiracAdmin.py
# Author : Stuart Paterson
########################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

"""DIRAC Administrator API Class

All administrative functionality is exposed through the DIRAC Admin API.  Examples include
site banning and unbanning, WMS proxy uploading etc.

"""

__RCSID__ = "$Id: DiracAdmin.py,v 1.19 2008/07/14 18:16:48 acasajus Exp $"

import DIRAC
from DIRAC.ConfigurationSystem.Client.CSAPI              import CSAPI
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
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
    self.log.info('Removing %s from site mask' % site)
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.banSite(site)
    return result

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

    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getProxy(ownerDN,ownerGroup,validity)
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
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn(error)
    return S_ERROR(message)


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
  def csCommitChanges( self ):
    """Commit the changes in the CS
    """
    return self.csAPI.commitChanges()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#