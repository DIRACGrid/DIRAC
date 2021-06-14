########################################################################
# File :   ComputingElement.py
# Author : Stuart Paterson, A.T.
########################################################################

"""  The Computing Element class is a base class for all the various
     types CEs. It serves several purposes:

      - collects general CE related parameters to generate CE description
        for the job matching
      - provides logic for evaluation of the number of available CPU slots
      - provides logic for the proxy renewal while executing jobs

     The CE parameters are collected from the following sources, in hierarchy
     descending order:

      - parameters provided through setParameters() method of the class
      - parameters in /LocalSite configuration section
      - parameters in /LocalSite/<ceName>/ResourceDict configuration section
      - parameters in /LocalSite/ResourceDict configuration section
      - parameters in /LocalSite/<ceName> configuration section
      - parameters in /Resources/Computing/<ceName> configuration section
      - parameters in /Resources/Computing/CEDefaults configuration section

     The ComputingElement objects are usually instantiated with the help of
     ComputingElementFactory.
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import six

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, S_ERROR, gLogger, version

from DIRAC.Core.Security import Properties
from DIRAC.Core.Security.VOMS import VOMS
from DIRAC.Core.Security.ProxyFile import writeToProxyFile
from DIRAC.Core.Security.ProxyInfo import getProxyInfoAsString
from DIRAC.Core.Security.ProxyInfo import formatProxyInfoAsString
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.Time import dateTime, second
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.WorkloadManagementSystem.Utilities.JobParameters import getNumberOfProcessors


INTEGER_PARAMETERS = ['CPUTime',
                      'NumberOfProcessors', 'NumberOfPayloadProcessors',
                      'MaxRAM']
FLOAT_PARAMETERS = ['WaitingToRunningRatio']

__RCSID__ = "$Id$"

LIST_PARAMETERS = ['Tag', 'RequiredTag']
WAITING_TO_RUNNING_RATIO = 0.5
MAX_WAITING_JOBS = 1
MAX_TOTAL_JOBS = 1


class ComputingElement(object):
  """ ComputingElement base class """
  #############################################################################

  def __init__(self, ceName):
    """ Standard constructor
    """
    self.log = gLogger.getSubLogger(ceName)
    self.ceName = ceName
    self.ceType = ''
    self.ceParameters = {}
    self.proxy = ''
    self.valid = None
    self.mandatoryParameters = []
    self.batchSystem = None
    self.parallelLibrary = None
    self.taskResults = {}
    self.minProxyTime = gConfig.getValue('/Registry/MinProxyLifeTime', 10800)  # secs
    self.defaultProxyTime = gConfig.getValue('/Registry/DefaultProxyLifeTime', 43200)  # secs
    self.proxyCheckPeriod = gConfig.getValue('/Registry/ProxyCheckingPeriod', 3600)  # secs

    self.initializeParameters()
    self.log.debug("CE parameters", self.ceParameters)

  def setProxy(self, proxy, valid=0):
    """ Set proxy for this instance
    """
    self.proxy = proxy
    self.valid = dateTime() + second * valid
    return S_OK()

  def _prepareProxy(self):
    """ Set the environment variable X509_USER_PROXY
    """
    if not self.proxy:
      result = getProxyInfo()
      if not result['OK']:
        return S_ERROR("No proxy available")
      if "path" in result['Value']:
        os.environ['X509_USER_PROXY'] = result['Value']['path']
        return S_OK()
    else:
      result = gProxyManager.dumpProxyToFile(self.proxy, requiredTimeLeft=self.minProxyTime)
      if not result['OK']:
        return result
      os.environ['X509_USER_PROXY'] = result['Value']

    self.log.debug("Set proxy variable X509_USER_PROXY to %s" % os.environ['X509_USER_PROXY'])
    return S_OK()

  def isProxyValid(self, valid=1000):
    """ Check if the stored proxy is valid
    """
    if not self.valid:
      result = S_ERROR('Proxy is not valid for the requested length')
      result['Value'] = 0
      return result
    delta = self.valid - dateTime()
    totalSeconds = delta.days * 86400 + delta.seconds
    if totalSeconds > valid:
      return S_OK(totalSeconds - valid)

    result = S_ERROR('Proxy is not valid for the requested length')
    result['Value'] = totalSeconds - valid
    return result

  def initializeParameters(self):
    """ Initialize the CE parameters after they are collected from various sources
    """

    self.log.debug("Initializing the CE parameters")

    # Collect global defaults first
    for section in ['/Resources/Computing/CEDefaults', '/Resources/Computing/%s' % self.ceName]:
      result = gConfig.getOptionsDict(section)

      self.log.debug(result)

      if not result['OK']:
        self.log.warn(result['Message'])
      else:
        ceOptions = result['Value']
        for key in ceOptions:
          if key in INTEGER_PARAMETERS:
            ceOptions[key] = int(ceOptions[key])
          if key in FLOAT_PARAMETERS:
            ceOptions[key] = float(ceOptions[key])
          if key in LIST_PARAMETERS:
            ceOptions[key] = gConfig.getValue(os.path.join(section, key), [])
        self.ceParameters.update(ceOptions)

    # Get local CE configuration
    localConfigDict = getCEConfigDict(self.ceName)
    self.ceParameters.update(localConfigDict)

    # Adds site level parameters
    section = '/LocalSite'
    result = gConfig.getOptionsDict(section)
    if result['OK'] and result['Value']:
      localSiteParameters = result['Value']
      self.log.debug('Local site parameters are: %s' % (localSiteParameters))
      for option, value in localSiteParameters.items():
        if option == 'Architecture':
          self.ceParameters['Platform'] = value
          self.ceParameters['Architecture'] = value
        elif option == 'LocalSE':
          self.ceParameters['LocalSE'] = value.split(', ')
        else:
          self.ceParameters[option] = value

    self._addCEConfigDefaults()

  def isValid(self):
    """ Check the sanity of the Computing Element definition
    """
    for par in self.mandatoryParameters:
      if par not in self.ceParameters:
        return S_ERROR('Missing Mandatory Parameter in Configuration: %s' % par)
    return S_OK()

  #############################################################################
  def _addCEConfigDefaults(self):
    """Method to make sure all necessary Configuration Parameters are defined
    """
    self.ceParameters['WaitingToRunningRatio'] = float(
        self.ceParameters.get('WaitingToRunningRatio', WAITING_TO_RUNNING_RATIO))
    self.ceParameters['MaxWaitingJobs'] = int(self.ceParameters.get('MaxWaitingJobs', MAX_WAITING_JOBS))
    self.ceParameters['MaxTotalJobs'] = int(self.ceParameters.get('MaxTotalJobs', MAX_TOTAL_JOBS))

  def _reset(self):
    """ Make specific CE parameter adjustments after they are collected or added
    """
    return S_OK()

  def loadBatchSystem(self, batchSystemName):
    """ Instantiate object representing the backend batch system

    :param str batchSystemName: name of the batch system
    """
    result = self._loadCEAttribute('BatchSystem', batchSystemName, 'Resources.Computing.BatchSystems')
    if not result['OK']:
      return result
    self.batchSystem = result['Value']
    return S_OK()

  def loadParallelLibrary(self, parallelLibraryName, workingDirectory='.'):
    """ Instantiate object representing the parallel library that will generate a script to wrap the executable

    :param str parallelLibraryName: name of the parallel library
    """
    result = self._loadCEAttribute('ParallelLibrary', parallelLibraryName, 'Resources.Computing.ParallelLibraries',
                                   workingDirectory=workingDirectory)
    if not result['OK']:
      return result
    self.parallelLibrary = result['Value']
    return S_OK()

  def _loadCEAttribute(self, ceAttribute, typeAttribute, moduleLocation, **kwargs):
    """ Instantiate specific CE attributes

    :param str ceAttribute: name of the CE attribute
    :param str typeAttribute: class of the CE attribute
    :param str moduleLocation: location of the class definition in the source code
    :param dict kwargs: parameters to pass to the constructor of the ceAttribute instance
    """
    if typeAttribute is None:
      typeAttribute = self.ceParameters[ceAttribute]

    objectLoader = ObjectLoader()
    moduleLocation += '.%s' % typeAttribute
    result = objectLoader.loadObject(moduleLocation, typeAttribute)
    if not result['OK']:
      self.log.error('Failed to load %s object: %s' % (ceAttribute, result['Message']))
      return result
    ceAttributeClass = result['Value']
    ceAttributeModuleFile = result['ModuleFile']

    ceAttributeObject = ceAttributeClass(**kwargs)
    self.log.info("Class from module: ", ceAttributeModuleFile)

    return S_OK(ceAttributeObject)

  def setParameters(self, ceOptions):
    """ Add parameters from the given dictionary overriding the previous values

        :param dict ceOptions: CE parameters dictionary to update already defined ones
    """
    self.ceParameters.update(ceOptions)

    # At this point we can know the exact type of CE,
    # try to get generic parameters for this type
    ceType = self.ceParameters.get('CEType')
    if ceType:
      result = gConfig.getOptionsDict('/Resources/Computing/%s' % ceType)
      if result['OK']:
        generalCEDict = result['Value']
        generalCEDict.update(self.ceParameters)
        self.ceParameters = generalCEDict

    # If NumberOfProcessors is present in the description but is equal to zero
    # interpret it as needing local evaluation
    if self.ceParameters.get("NumberOfProcessors", -1) == 0:
      self.ceParameters["NumberOfProcessors"] = getNumberOfProcessors()

    for key in ceOptions:
      if key in INTEGER_PARAMETERS:
        self.ceParameters[key] = int(self.ceParameters[key])
      if key in FLOAT_PARAMETERS:
        self.ceParameters[key] = float(self.ceParameters[key])

    return self._reset()

  #############################################################################
  def setCPUTimeLeft(self, cpuTimeLeft=None):
    """Update the CPUTime parameter of the CE classAd, necessary for running in filling mode
    """
    if not cpuTimeLeft:
      # do nothing
      return S_OK()
    try:
      intCPUTimeLeft = int(cpuTimeLeft)
      self.ceParameters['CPUTime'] = intCPUTimeLeft
      return S_OK(intCPUTimeLeft)
    except ValueError:
      return S_ERROR('Wrong type for setCPUTimeLeft argument')

  #############################################################################
  def available(self, jobIDList=None):
    """This method returns the number of available slots in the target CE. The CE
       instance polls for waiting and running jobs and compares to the limits
       in the CE parameters.

       :param list jobIDList: list of already existing job IDs to be checked against
    """

    # If there are no already registered jobs
    if jobIDList is not None and not jobIDList:
      result = S_OK()
      result['RunningJobs'] = 0
      result['WaitingJobs'] = 0
      result['SubmittedJobs'] = 0
    else:
      result = self.ceParameters.get('CEType')
      if result and result == 'CREAM':
        result = self.getCEStatus(jobIDList)  # pylint: disable=too-many-function-args
      else:
        result = self.getCEStatus()
      if not result['OK']:
        return result
    runningJobs = result['RunningJobs']
    waitingJobs = result['WaitingJobs']
    submittedJobs = result['SubmittedJobs']
    availableProcessors = result.get('AvailableProcessors')
    ceInfoDict = dict(result)

    maxTotalJobs = int(self.ceParameters.get('MaxTotalJobs', 0))
    ceInfoDict['MaxTotalJobs'] = maxTotalJobs
    waitingToRunningRatio = float(self.ceParameters.get('WaitingToRunningRatio', 0.0))
    # if there are no Running job we can submit to get at most 'MaxWaitingJobs'
    # if there are Running jobs we can increase this to get a ratio W / R 'WaitingToRunningRatio'
    maxWaitingJobs = int(max(int(self.ceParameters.get('MaxWaitingJobs', 0)),
                             runningJobs * waitingToRunningRatio))

    self.log.verbose('Max Number of Jobs:', maxTotalJobs)
    self.log.verbose('Max W/R Ratio:', waitingToRunningRatio)
    self.log.verbose('Max Waiting Jobs:', maxWaitingJobs)

    # Determine how many more jobs can be submitted
    message = '%s CE: SubmittedJobs=%s' % (self.ceName, submittedJobs)
    message += ', WaitingJobs=%s, RunningJobs=%s' % (waitingJobs, runningJobs)
    totalJobs = runningJobs + waitingJobs

    message += ', MaxTotalJobs=%s' % (maxTotalJobs)

    if totalJobs >= maxTotalJobs:
      self.log.verbose('Max Number of Jobs reached:', maxTotalJobs)
      result['Value'] = 0
      message = 'There are %s waiting jobs and total jobs %s >= %s max total jobs' % (
          waitingJobs, totalJobs, maxTotalJobs)
    else:
      additionalJobs = 0
      if waitingJobs < maxWaitingJobs:
        additionalJobs = maxWaitingJobs - waitingJobs
        if totalJobs + additionalJobs >= maxTotalJobs:
          additionalJobs = maxTotalJobs - totalJobs
      # For SSH CE case
      if int(self.ceParameters.get('MaxWaitingJobs', 0)) == 0:
        additionalJobs = maxTotalJobs - runningJobs

      if availableProcessors is not None:
        additionalJobs = min(additionalJobs, availableProcessors)
      result['Value'] = additionalJobs

    result['Message'] = message
    result['CEInfoDict'] = ceInfoDict
    return result

  #############################################################################
  def writeProxyToFile(self, proxy):
    """CE helper function to write a CE proxy string to a file.
    """
    result = writeToProxyFile(proxy)
    if not result['OK']:
      self.log.error('Could not write proxy to file', result['Message'])
      return result

    proxyLocation = result['Value']
    result = getProxyInfoAsString(proxyLocation)
    if not result['OK']:
      self.log.error('Could not get proxy info', result)
      return result
    else:
      self.log.info('Payload proxy information:')
      print(result['Value'])

    return S_OK(proxyLocation)

  #############################################################################
  def _monitorProxy(self, payloadProxy=None):
    """Base class for the monitor and update of the payload proxy, to be used in
      derived classes for the basic renewal of the proxy, if further actions are
      necessary they should be implemented there

      :param str payloadProxy: location of the payload proxy file

      :returns: S_OK(filename)/S_ERROR
    """
    if not payloadProxy:
      return S_ERROR("No payload proxy")

    # This will get the pilot proxy
    ret = getProxyInfo()
    if not ret['OK']:
      pilotProxy = None
    else:
      pilotProxy = ret['Value']['path']
      self.log.notice('Pilot Proxy:', pilotProxy)

    retVal = getProxyInfo(payloadProxy)
    if not retVal['OK']:
      self.log.error('Could not get payload proxy info', retVal)
      return retVal
    self.log.verbose('Payload Proxy information:\n%s' % formatProxyInfoAsString(retVal['Value']))

    payloadProxyDict = retVal['Value']
    payloadSecs = payloadProxyDict['chain'].getRemainingSecs()['Value']
    if payloadSecs > self.minProxyTime:
      self.log.verbose('No need to renew payload Proxy')
      return S_OK()

    # if there is no pilot proxy, assume there is a certificate and try a renewal
    if not pilotProxy:
      self.log.info('Using default credentials to get a new payload Proxy')
      return gProxyManager.renewProxy(proxyToBeRenewed=payloadProxy, minLifeTime=self.minProxyTime,
                                      newProxyLifeTime=self.defaultProxyTime,
                                      proxyToConnect=pilotProxy)

    # if there is pilot proxy
    retVal = getProxyInfo(pilotProxy)
    if not retVal['OK']:
      return retVal
    pilotProxyDict = retVal['Value']

    if 'groupProperties' not in pilotProxyDict:
      self.log.error('Invalid Pilot Proxy', 'Group has no properties defined')
      return S_ERROR('Proxy has no group properties defined')

    pilotProps = pilotProxyDict['groupProperties']

    # if running with a pilot proxy, use it to renew the proxy of the payload
    if Properties.PILOT in pilotProps or Properties.GENERIC_PILOT in pilotProps:
      self.log.info('Using Pilot credentials to get a new payload Proxy')
      return gProxyManager.renewProxy(proxyToBeRenewed=payloadProxy, minLifeTime=self.minProxyTime,
                                      newProxyLifeTime=self.defaultProxyTime,
                                      proxyToConnect=pilotProxy)

    # if we are running with other type of proxy check if they are for the same user and group
    # and copy the pilot proxy if necessary

    self.log.info('Trying to copy pilot Proxy to get a new payload Proxy')
    pilotProxySecs = pilotProxyDict['chain'].getRemainingSecs()['Value']
    if pilotProxySecs <= payloadSecs:
      errorStr = 'Pilot Proxy is not longer than payload Proxy'
      self.log.error(errorStr)
      return S_ERROR('Can not renew by copy: %s' % errorStr)

    # check if both proxies belong to the same user and group
    pilotDN = pilotProxyDict['chain'].getIssuerCert()['Value'].getSubjectDN()['Value']
    retVal = pilotProxyDict['chain'].getDIRACGroup()
    if not retVal['OK']:
      return retVal
    pilotGroup = retVal['Value']

    payloadDN = payloadProxyDict['chain'].getIssuerCert()['Value'].getSubjectDN()['Value']
    retVal = payloadProxyDict['chain'].getDIRACGroup()
    if not retVal['OK']:
      return retVal
    payloadGroup = retVal['Value']
    if pilotDN != payloadDN or pilotGroup != payloadGroup:
      errorStr = 'Pilot Proxy and payload Proxy do not have same DN and Group'
      self.log.error(errorStr)
      return S_ERROR('Can not renew by copy: %s' % errorStr)

    if pilotProxyDict.get('hasVOMS', False):
      return pilotProxyDict['chain'].dumpAllToFile(payloadProxy)

    attribute = Registry.getVOMSAttributeForGroup(payloadGroup)
    vo = Registry.getVOMSVOForGroup(payloadGroup)

    retVal = VOMS().setVOMSAttributes(pilotProxyDict['chain'], attribute=attribute, vo=vo)
    if not retVal['OK']:
      return retVal

    chain = retVal['Value']
    return chain.dumpAllToFile(payloadProxy)

  def getDescription(self):
    """ Get CE description as a dictionary.

        This is called by the JobAgent for the case of "inner" CEs.
    """

    ceDict = {}
    for option, value in self.ceParameters.items():
      if isinstance(value, list):
        ceDict[option] = value
      elif isinstance(value, six.string_types):
        try:
          ceDict[option] = int(value)
        except ValueError:
          ceDict[option] = value
      elif isinstance(value, six.integer_types + (float,)):
        ceDict[option] = value
      else:
        self.log.warn('Type of option %s = %s not determined' % (option, value))

    release = gConfig.getValue('/LocalSite/ReleaseVersion', version)
    ceDict['DIRACVersion'] = release
    ceDict['ReleaseVersion'] = release
    project = gConfig.getValue("/LocalSite/ReleaseProject", "")
    if project:
      ceDict['ReleaseProject'] = project

    # the getCEStatus is implemented in each of the specific CE classes
    result = self.getCEStatus()
    if result['OK']:
      ceDict['NumberOfProcessors'] = result.get('AvailableProcessors',
                                                result.get('NumberOfProcessors', 1))
    else:
      self.log.error("Failure getting CE status",
                     "(we keep going without the number of waiting and running pilots/jobs)")

    return S_OK(ceDict)

  #############################################################################
  def sendOutput(self, stdid, line):  # pylint: disable=unused-argument, no-self-use
    """ Callback function such that the results from the CE may be returned.
    """
    print(line)

  #############################################################################
  def submitJob(self, executableFile, proxy, dummy=None, processors=1):  # pylint: disable=unused-argument
    """ Method to submit job, should be overridden in sub-class.
    """
    name = 'submitJob()'
    self.log.error('ComputingElement should be implemented in a subclass', name)
    return S_ERROR('ComputingElement: %s should be implemented in a subclass' % (name))

  #############################################################################
  def getCEStatus(self):
    """ Method to get dynamic job information, can be overridden in sub-class.
    """
    name = 'getCEStatus()'
    self.log.error('ComputingElement should be implemented in a subclass', name)
    return S_ERROR('ComputingElement: %s should be implemented in a subclass' % (name))

  #############################################################################
  def shutdown(self):
    """ Optional method to shutdown the (Inner) Computing Element
    """
    return S_OK(self.taskResults)


def getCEConfigDict(ceName):
  """Look into LocalSite for configuration Parameters for this CE
  """
  ceConfigDict = {}
  if ceName:
    result = gConfig.getOptionsDict('/LocalSite/%s' % ceName)
    if result['OK']:
      ceConfigDict = result['Value']
  return ceConfigDict
