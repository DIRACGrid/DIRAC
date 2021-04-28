"""DIRAC Administrator API Class

All administrative functionality is exposed through the DIRAC Admin API.  Examples include
site banning and unbanning, WMS proxy uploading etc.

"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "$Id$"

import six
import os

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC.Core.Base.API import API
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.WMSAdministratorClient import WMSAdministratorClient
from DIRAC.WorkloadManagementSystem.Client.PilotManagerClient import PilotManagerClient

voName = ''
ret = getProxyInfo(disableVOMS=True)
if ret['OK'] and 'group' in ret['Value']:
  voName = getVOForGroup(ret['Value']['group'])

COMPONENT_NAME = '/Interfaces/API/DiracAdmin'


class DiracAdmin(API):
  """ Administrative functionalities
  """

  #############################################################################
  def __init__(self):
    """Internal initialization of the DIRAC Admin API.
    """
    super(DiracAdmin, self).__init__()

    self.csAPI = CSAPI()

    self.dbg = False
    if gConfig.getValue(self.section + '/LogLevel', 'DEBUG') == 'DEBUG':
      self.dbg = True

    self.scratchDir = gConfig.getValue(self.section + '/ScratchDir', '/tmp')
    self.currentDir = os.getcwd()
    self.rssFlag = ResourceStatus().rssFlag
    self.sitestatus = SiteStatus()

  #############################################################################
  def uploadProxy(self):
    """Upload a proxy to the DIRAC WMS.  This method

       Example usage:

         >>> print diracAdmin.uploadProxy('dteam_pilot')
         {'OK': True, 'Value': 0L}

       :return: S_OK,S_ERROR

       :param permanent: Indefinitely update proxy
       :type permanent: boolean

    """
    return gProxyManager.uploadProxy()

  #############################################################################
  def setProxyPersistency(self, userDN, userGroup, persistent=True):
    """Set the persistence of a proxy in the Proxy Manager

       Example usage:

         >>> gLogger.notice(diracAdmin.setProxyPersistency( 'some DN', 'dirac group', True ))
         {'OK': True }

       :param userDN: User DN
       :type userDN: string
       :param userGroup: DIRAC Group
       :type userGroup: string
       :param persistent: Persistent flag
       :type persistent: boolean
       :return: S_OK,S_ERROR
    """
    return gProxyManager.setPersistency(userDN, userGroup, persistent)

  #############################################################################
  def checkProxyUploaded(self, userDN, userGroup, requiredTime):
    """Set the persistence of a proxy in the Proxy Manager

       Example usage:

         >>> gLogger.notice(diracAdmin.setProxyPersistency( 'some DN', 'dirac group', True ))
         {'OK': True, 'Value' : True/False }

       :param userDN: User DN
       :type userDN: string
       :param userGroup: DIRAC Group
       :type userGroup: string
       :param requiredTime: Required life time of the uploaded proxy
       :type requiredTime: boolean
       :return: S_OK,S_ERROR
    """
    return gProxyManager.userHasProxy(userDN, userGroup, requiredTime)

  #############################################################################
  def getSiteMask(self, printOutput=False, status='Active'):
    """Retrieve current site mask from WMS Administrator service.

       Example usage:

         >>> gLogger.notice(diracAdmin.getSiteMask())
         {'OK': True, 'Value': 0L}

       :return: S_OK,S_ERROR

    """

    result = self.sitestatus.getSites(siteState=status)
    if result['OK']:
      sites = result['Value']
      if printOutput:
        sites.sort()
        for site in sites:
          gLogger.notice(site)

    return result

  #############################################################################
  def getBannedSites(self, printOutput=False):
    """Retrieve current list of banned  and probing sites.

       Example usage:

         >>> gLogger.notice(diracAdmin.getBannedSites())
         {'OK': True, 'Value': []}

       :return: S_OK,S_ERROR

    """

    bannedSites = self.sitestatus.getSites(siteState='Banned')
    if not bannedSites['OK']:
      return bannedSites

    probingSites = self.sitestatus.getSites(siteState='Probing')
    if not probingSites['OK']:
      return probingSites

    mergedList = sorted(bannedSites['Value'] + probingSites['Value'])

    if printOutput:
      gLogger.notice('\n'.join(mergedList))

    return S_OK(mergedList)

  #############################################################################
  def getSiteSection(self, site, printOutput=False):
    """Simple utility to get the list of CEs for DIRAC site name.

       Example usage:

         >>> gLogger.notice(diracAdmin.getSiteSection('LCG.CERN.ch'))
         {'OK': True, 'Value':}

       :return: S_OK,S_ERROR
    """
    gridType = site.split('.')[0]
    if not gConfig.getSections('/Resources/Sites/%s' % (gridType))['OK']:
      return S_ERROR('/Resources/Sites/%s is not a valid site section' % (gridType))

    result = gConfig.getOptionsDict('/Resources/Sites/%s/%s' % (gridType, site))
    if printOutput and result['OK']:
      gLogger.notice(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def allowSite(self, site, comment, printOutput=False):
    """Adds the site to the site mask.

       Example usage:

         >>> gLogger.notice(diracAdmin.allowSite())
         {'OK': True, 'Value': }

       :return: S_OK,S_ERROR

    """
    result = self._checkSiteIsValid(site)
    if not result['OK']:
      return result

    result = self.getSiteMask(status='Active')
    if not result['OK']:
      return result
    siteMask = result['Value']
    if site in siteMask:
      if printOutput:
        gLogger.notice('Site %s is already Active' % site)
      return S_OK('Site %s is already Active' % site)

    if self.rssFlag:
      result = self.sitestatus.setSiteStatus(site, 'Active', comment)
    else:
      result = WMSAdministratorClient().allowSite(site, comment)
    if not result['OK']:
      return result

    if printOutput:
      gLogger.notice('Site %s status is set to Active' % site)

    return result

  #############################################################################
  def getSiteMaskLogging(self, site=None, printOutput=False):
    """Retrieves site mask logging information.

       Example usage:

         >>> gLogger.notice(diracAdmin.getSiteMaskLogging('LCG.AUVER.fr'))
         {'OK': True, 'Value': }

       :return: S_OK,S_ERROR
    """
    result = self._checkSiteIsValid(site)
    if not result['OK']:
      return result

    if self.rssFlag:
      result = ResourceStatusClient().selectStatusElement('Site', 'History', name=site)
    else:
      result = WMSAdministratorClient().getSiteMaskLogging(site)

    if not result['OK']:
      return result

    if printOutput:
      if site:
        gLogger.notice('\nSite Mask Logging Info for %s\n' % site)
      else:
        gLogger.notice('\nAll Site Mask Logging Info\n')

      sitesLogging = result['Value']
      if isinstance(sitesLogging, dict):
        for siteName, tupleList in sitesLogging.items():  # can be an iterator
          if not siteName:
            gLogger.notice('\n===> %s\n' % siteName)
          for tup in tupleList:
            stup = str(tup[0]).ljust(8) + str(tup[1]).ljust(20)
            stup += '( ' + str(tup[2]).ljust(len(str(tup[2]))) + ' )  "' + str(tup[3]) + '"'
            gLogger.notice(stup)
          gLogger.notice(' ')
      elif isinstance(sitesLogging, list):
        sitesLoggingList = [(sl[1], sl[3], sl[4]) for sl in sitesLogging]
        for siteLog in sitesLoggingList:
          gLogger.notice(siteLog)

    return S_OK()

  #############################################################################
  def banSite(self, site, comment, printOutput=False):
    """Removes the site from the site mask.

       Example usage:

         >>> gLogger.notice(diracAdmin.banSite())
         {'OK': True, 'Value': }

       :return: S_OK,S_ERROR

    """
    result = self._checkSiteIsValid(site)
    if not result['OK']:
      return result

    mask = self.getSiteMask(status='Banned')
    if not mask['OK']:
      return mask
    siteMask = mask['Value']
    if site in siteMask:
      if printOutput:
        gLogger.notice('Site %s is already Banned' % site)
      return S_OK('Site %s is already Banned' % site)

    if self.rssFlag:
      result = self.sitestatus.setSiteStatus(site, 'Banned', comment)
    else:
      result = WMSAdministratorClient().banSite(site, comment)
    if not result['OK']:
      return result

    if printOutput:
      gLogger.notice('Site %s status is set to Banned' % site)

    return result

  #############################################################################
  def getServicePorts(self, setup='', printOutput=False):
    """Checks the service ports for the specified setup.  If not given this is
       taken from the current installation (/DIRAC/Setup)

       Example usage:

         >>> gLogger.notice(diracAdmin.getServicePorts())
         {'OK': True, 'Value':''}

       :return: S_OK,S_ERROR

    """
    if not setup:
      setup = gConfig.getValue('/DIRAC/Setup', '')

    setupList = gConfig.getSections('/DIRAC/Setups', [])
    if not setupList['OK']:
      return S_ERROR('Could not get /DIRAC/Setups sections')
    setupList = setupList['Value']
    if setup not in setupList:
      return S_ERROR('Setup %s is not in allowed list: %s' % (setup, ', '.join(setupList)))

    serviceSetups = gConfig.getOptionsDict('/DIRAC/Setups/%s' % setup)
    if not serviceSetups['OK']:
      return S_ERROR('Could not get /DIRAC/Setups/%s options' % setup)
    serviceSetups = serviceSetups['Value']  # dict
    systemList = gConfig.getSections('/Systems')
    if not systemList['OK']:
      return S_ERROR('Could not get Systems sections')
    systemList = systemList['Value']
    result = {}
    for system in systemList:
      if system in serviceSetups:
        path = '/Systems/%s/%s/Services' % (system, serviceSetups[system])
        servicesList = gConfig.getSections(path)
        if not servicesList['OK']:
          self.log.warn('Could not get sections in %s' % path)
        else:
          servicesList = servicesList['Value']
          if not servicesList:
            servicesList = []
          self.log.verbose('System: %s ServicesList: %s' % (system, ', '.join(servicesList)))
          for service in servicesList:
            spath = '%s/%s/Port' % (path, service)
            servicePort = gConfig.getValue(spath, 0)
            if servicePort:
              self.log.verbose('Found port for %s/%s = %s' % (system, service, servicePort))
              result['%s/%s' % (system, service)] = servicePort
            else:
              self.log.warn('No port found for %s' % spath)
      else:
        self.log.warn('%s is not defined in /DIRAC/Setups/%s' % (system, setup))

    if printOutput:
      gLogger.notice(self.pPrint.pformat(result))

    return S_OK(result)

  #############################################################################
  def getProxy(self, userDN, userGroup, validity=43200, limited=False):
    """Retrieves a proxy with default 12hr validity and stores
       this in a file in the local directory by default.

       Example usage:

         >>> gLogger.notice(diracAdmin.getProxy())
         {'OK': True, 'Value': }

       :return: S_OK,S_ERROR

    """
    return gProxyManager.downloadProxy(userDN, userGroup, limited=limited,
                                       requiredTimeLeft=validity)

  #############################################################################
  def getVOMSProxy(self, userDN, userGroup, vomsAttr=False, validity=43200, limited=False):
    """Retrieves a proxy with default 12hr validity and VOMS extensions and stores
       this in a file in the local directory by default.

       Example usage:

         >>> gLogger.notice(diracAdmin.getVOMSProxy())
         {'OK': True, 'Value': }

       :return: S_OK,S_ERROR

    """
    return gProxyManager.downloadVOMSProxy(userDN, userGroup, limited=limited,
                                           requiredVOMSAttribute=vomsAttr,
                                           requiredTimeLeft=validity)

  #############################################################################
  def getPilotProxy(self, userDN, userGroup, validity=43200):
    """Retrieves a pilot proxy with default 12hr validity and stores
       this in a file in the local directory by default.

       Example usage:

         >>> gLogger.notice(diracAdmin.getVOMSProxy())
         {'OK': True, 'Value': }

       :return: S_OK,S_ERROR

    """

    return gProxyManager.getPilotProxyFromDIRACGroup(userDN, userGroup, requiredTimeLeft=validity)

  #############################################################################
  def resetJob(self, jobID):
    """Reset a job or list of jobs in the WMS.  This operation resets the reschedule
       counter for a job or list of jobs and allows them to run as new.

       Example::

         >>> gLogger.notice(dirac.reset(12345))
         {'OK': True, 'Value': [12345]}

       :param job: JobID
       :type job: integer or list of integers
       :return: S_OK,S_ERROR

    """
    if isinstance(jobID, six.string_types):
      try:
        jobID = int(jobID)
      except Exception as x:
        return self._errorReport(str(x), 'Expected integer or convertible integer for existing jobID')
    elif isinstance(jobID, list):
      try:
        jobID = [int(job) for job in jobID]
      except Exception as x:
        return self._errorReport(str(x), 'Expected integer or convertible integer for existing jobIDs')

    result = JobManagerClient(useCertificates=False).resetJob(jobID)
    return result

  #############################################################################
  def getJobPilotOutput(self, jobID, directory=''):
    """Retrieve the pilot output for an existing job in the WMS.
       The output will be retrieved in a local directory unless
       otherwise specified.

         >>> gLogger.notice(dirac.getJobPilotOutput(12345))
         {'OK': True, StdOut:'',StdError:''}

       :param job: JobID
       :type job: integer or string
       :return: S_OK,S_ERROR
    """
    if not directory:
      directory = self.currentDir

    if not os.path.exists(directory):
      return self._errorReport('Directory %s does not exist' % directory)

    result = WMSAdministratorClient().getJobPilotOutput(jobID)
    if not result['OK']:
      return result

    outputPath = '%s/pilot_%s' % (directory, jobID)
    if os.path.exists(outputPath):
      self.log.info('Remove %s and retry to continue' % outputPath)
      return S_ERROR('Remove %s and retry to continue' % outputPath)

    if not os.path.exists(outputPath):
      self.log.verbose('Creating directory %s' % outputPath)
      os.mkdir(outputPath)

    outputs = result['Value']
    if 'StdOut' in outputs:
      stdout = '%s/std.out' % (outputPath)
      with open(stdout, 'w') as fopen:
        fopen.write(outputs['StdOut'])
      self.log.verbose('Standard output written to %s' % (stdout))
    else:
      self.log.warn('No standard output returned')

    if 'StdError' in outputs:
      stderr = '%s/std.err' % (outputPath)
      with open(stderr, 'w') as fopen:
        fopen.write(outputs['StdError'])
      self.log.verbose('Standard error written to %s' % (stderr))
    else:
      self.log.warn('No standard error returned')

    self.log.always('Outputs retrieved in %s' % outputPath)
    return result

  #############################################################################
  def getPilotOutput(self, gridReference, directory=''):
    """Retrieve the pilot output  (std.out and std.err) for an existing job in the WMS.

         >>> gLogger.notice(dirac.getJobPilotOutput(12345))
         {'OK': True, 'Value': {}}

       :param job: JobID
       :type job: integer or string
       :return: S_OK,S_ERROR
    """
    if not isinstance(gridReference, six.string_types):
      return self._errorReport('Expected string for pilot reference')

    if not directory:
      directory = self.currentDir

    if not os.path.exists(directory):
      return self._errorReport('Directory %s does not exist' % directory)

    result = PilotManagerClient().getPilotOutput(gridReference)
    if not result['OK']:
      return result

    gridReferenceSmall = gridReference.split('/')[-1]
    if not gridReferenceSmall:
      gridReferenceSmall = 'reference'
    outputPath = '%s/pilot_%s' % (directory, gridReferenceSmall)

    if os.path.exists(outputPath):
      self.log.info('Remove %s and retry to continue' % outputPath)
      return S_ERROR('Remove %s and retry to continue' % outputPath)

    if not os.path.exists(outputPath):
      self.log.verbose('Creating directory %s' % outputPath)
      os.mkdir(outputPath)

    outputs = result['Value']
    if 'StdOut' in outputs:
      stdout = '%s/std.out' % (outputPath)
      with open(stdout, 'w') as fopen:
        fopen.write(outputs['StdOut'])
      self.log.info('Standard output written to %s' % (stdout))
    else:
      self.log.warn('No standard output returned')

    if 'StdErr' in outputs:
      stderr = '%s/std.err' % (outputPath)
      with open(stderr, 'w') as fopen:
        fopen.write(outputs['StdErr'])
      self.log.info('Standard error written to %s' % (stderr))
    else:
      self.log.warn('No standard error returned')

    self.log.always('Outputs retrieved in %s' % outputPath)
    return result

  #############################################################################
  def getPilotInfo(self, gridReference):
    """Retrieve info relative to a pilot reference

         >>> gLogger.notice(dirac.getPilotInfo(12345))
         {'OK': True, 'Value': {}}

       :param gridReference: Pilot Job Reference
       :type gridReference: string
       :return: S_OK,S_ERROR
    """
    if not isinstance(gridReference, six.string_types):
      return self._errorReport('Expected string for pilot reference')

    result = PilotManagerClient().getPilotInfo(gridReference)
    return result

  #############################################################################
  def killPilot(self, gridReference):
    """Kill the pilot specified

         >>> gLogger.notice(dirac.getPilotInfo(12345))
         {'OK': True, 'Value': {}}

       :param gridReference: Pilot Job Reference
       :return: S_OK,S_ERROR
    """
    if not isinstance(gridReference, six.string_types):
      return self._errorReport('Expected string for pilot reference')

    result = PilotManagerClient().killPilot(gridReference)
    return result

  #############################################################################
  def getPilotLoggingInfo(self, gridReference):
    """Retrieve the pilot logging info for an existing job in the WMS.

         >>> gLogger.notice(dirac.getPilotLoggingInfo(12345))
         {'OK': True, 'Value': {"The output of the command"}}

       :param gridReference: Gridp pilot job reference Id
       :type gridReference: string
       :return: S_OK,S_ERROR
    """
    if not isinstance(gridReference, six.string_types):
      return self._errorReport('Expected string for pilot reference')

    return PilotManagerClient().getPilotLoggingInfo(gridReference)

  #############################################################################
  def getJobPilots(self, jobID):
    """Extract the list of submitted pilots and their status for a given
       jobID from the WMS.  Useful information is printed to the screen.

         >>> gLogger.notice(dirac.getJobPilots())
         {'OK': True, 'Value': {PilotID:{StatusDict}}}

       :param job: JobID
       :type job: integer or string
       :return: S_OK,S_ERROR

    """
    if isinstance(jobID, six.string_types):
      try:
        jobID = int(jobID)
      except Exception as x:
        return self._errorReport(str(x), 'Expected integer or string for existing jobID')

    result = PilotManagerClient().getPilots(jobID)
    if result['OK']:
      gLogger.notice(self.pPrint.pformat(result['Value']))
    return result

  #############################################################################
  def getPilotSummary(self, startDate='', endDate=''):
    """Retrieve the pilot output for an existing job in the WMS.  Summary is
       printed at INFO level, full dictionary of results also returned.

         >>> gLogger.notice(dirac.getPilotSummary())
         {'OK': True, 'Value': {CE:{Status:Count}}}

       :param job: JobID
       :type job: integer or string
       :return: S_OK,S_ERROR
    """
    result = PilotManagerClient().getPilotSummary(startDate, endDate)
    if not result['OK']:
      return result

    ceDict = result['Value']
    headers = 'CE'.ljust(28)
    i = 0
    for ce, summary in ceDict.items():
      states = list(summary)
      if len(states) > i:
        i = len(states)

    for i in range(i):
      headers += 'Status'.ljust(12) + 'Count'.ljust(12)
    gLogger.notice(headers)

    for ce, summary in ceDict.items():
      line = ce.ljust(28)
      states = sorted(summary)
      for state in states:
        count = str(summary[state])
        line += state.ljust(12) + count.ljust(12)
      gLogger.notice(line)

    return result

  #############################################################################
  def setSiteProtocols(self, site, protocolsList, printOutput=False):
    """
    Allows to set the defined protocols for each SE for a given site.
    """
    result = self._checkSiteIsValid(site)
    if not result['OK']:
      return result

    siteSection = '/Resources/Sites/%s/%s/SE' % (site.split('.')[0], site)
    siteSEs = gConfig.getValue(siteSection, [])
    if not siteSEs:
      return S_ERROR('No SEs found for site %s in section %s' % (site, siteSection))

    defaultProtocols = gConfig.getValue('/Resources/StorageElements/DefaultProtocols', [])
    self.log.verbose('Default list of protocols are', ', '.join(defaultProtocols))

    for protocol in protocolsList:
      if protocol not in defaultProtocols:
        return S_ERROR('Requested to set protocol %s in list but %s is not '
                       'in default list of protocols:\n%s' % (protocol, protocol, ', '.join(defaultProtocols)))

    modifiedCS = False
    result = promptUser('Do you want to add the following default protocols:'
                        ' %s for SE(s):\n%s' % (', '.join(protocolsList), ', '.join(siteSEs)))
    if not result['OK']:
      return result
    if result['Value'].lower() != 'y':
      self.log.always('No protocols will be added')
      return S_OK()

    for se in siteSEs:
      sections = gConfig.getSections('/Resources/StorageElements/%s/' % (se))
      if not sections['OK']:
        return sections
      for section in sections['Value']:
        if gConfig.getValue('/Resources/StorageElements/%s/%s/ProtocolName' % (se, section), '') == 'SRM2':
          path = '/Resources/StorageElements/%s/%s/ProtocolsList' % (se, section)
          self.log.verbose('Setting %s to %s' % (path, ', '.join(protocolsList)))
          result = self.csSetOption(path, ', '.join(protocolsList))
          if not result['OK']:
            return result
          modifiedCS = True

    if modifiedCS:
      result = self.csCommitChanges(False)
      if not result['OK']:
        return S_ERROR('CS Commit failed with message = %s' % (result['Message']))
      else:
        if printOutput:
          gLogger.notice('Successfully committed changes to CS')
    else:
      if printOutput:
        gLogger.notice('No modifications to CS required')

    return S_OK()

  #############################################################################
  def csSetOption(self, optionPath, optionValue):
    """
    Function to modify an existing value in the CS.
    """
    return self.csAPI.setOption(optionPath, optionValue)

  #############################################################################
  def csSetOptionComment(self, optionPath, comment):
    """
    Function to modify an existing value in the CS.
    """
    return self.csAPI.setOptionComment(optionPath, comment)

  #############################################################################
  def csModifyValue(self, optionPath, newValue):
    """
    Function to modify an existing value in the CS.
    """
    return self.csAPI.modifyValue(optionPath, newValue)

  #############################################################################
  def csRegisterUser(self, username, properties):
    """
    Registers a user in the CS.

        - username: Username of the user (easy;)
        - properties: Dict containing:
            - DN
            - groups : list/tuple of groups the user belongs to
            - <others> : More properties of the user, like mail

    """
    return self.csAPI.addUser(username, properties)

  #############################################################################
  def csDeleteUser(self, user):
    """
    Deletes a user from the CS. Can take a list of users
    """
    return self.csAPI.deleteUsers(user)

  #############################################################################
  def csModifyUser(self, username, properties, createIfNonExistant=False):
    """
    Modify a user in the CS. Takes the same params as in addUser and
    applies the changes
    """
    return self.csAPI.modifyUser(username, properties, createIfNonExistant)

  #############################################################################
  def csListUsers(self, group=False):
    """
    Lists the users in the CS. If no group is specified return all users.
    """
    return self.csAPI.listUsers(group)

  #############################################################################
  def csDescribeUsers(self, mask=False):
    """
    List users and their properties in the CS.
    If a mask is given, only users in the mask will be returned
    """
    return self.csAPI.describeUsers(mask)

  #############################################################################
  def csModifyGroup(self, groupname, properties, createIfNonExistant=False):
    """
    Modify a user in the CS. Takes the same params as in addGroup and applies
    the changes
    """
    return self.csAPI.modifyGroup(groupname, properties, createIfNonExistant)

  #############################################################################
  def csListHosts(self):
    """
    Lists the hosts in the CS
    """
    return self.csAPI.listHosts()

  #############################################################################
  def csDescribeHosts(self, mask=False):
    """
    Gets extended info for the hosts in the CS
    """
    return self.csAPI.describeHosts(mask)

  #############################################################################
  def csModifyHost(self, hostname, properties, createIfNonExistant=False):
    """
    Modify a host in the CS. Takes the same params as in addHost and applies
    the changes
    """
    return self.csAPI.modifyHost(hostname, properties, createIfNonExistant)

  #############################################################################
  def csListGroups(self):
    """
    Lists groups in the CS
    """
    return self.csAPI.listGroups()

  #############################################################################
  def csDescribeGroups(self, mask=False):
    """
    List groups and their properties in the CS.
    If a mask is given, only groups in the mask will be returned
    """
    return self.csAPI.describeGroups(mask)

  #############################################################################
  def csSyncUsersWithCFG(self, usersCFG):
    """
    Synchronize users in cfg with its contents
    """
    return self.csAPI.syncUsersWithCFG(usersCFG)

  #############################################################################
  def csCommitChanges(self, sortUsers=True):
    """
    Commit the changes in the CS
    """
    return self.csAPI.commitChanges(sortUsers=False)

  #############################################################################
  def sendMail(self, address, subject, body, fromAddress=None, localAttempt=True, html=False):
    """
    Send mail to specified address with body.
    """
    notification = NotificationClient()
    return notification.sendMail(address, subject, body, fromAddress, localAttempt, html)

  #############################################################################
  def sendSMS(self, userName, body, fromAddress=None):
    """
    Send mail to specified address with body.
    """
    if len(body) > 160:
      return S_ERROR('Exceeded maximum SMS length of 160 characters')
    notification = NotificationClient()
    return notification.sendSMS(userName, body, fromAddress)

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
