########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Resources/Computing/ComputingElement.py,v 1.6 2008/07/16 09:08:23 rgracian Exp $
# File :   ComputingElement.py
# Author : Stuart Paterson
########################################################################

"""  The Computing Element class provides the default options to construct
     resource JDL for subsequent use during the matching process.
"""

__RCSID__ = "$Id: ComputingElement.py,v 1.6 2008/07/16 09:08:23 rgracian Exp $"

from DIRAC.Core.Utilities.ClassAd.ClassAdLight      import *
from DIRAC.ConfigurationSystem.Client.Config        import gConfig
from DIRAC                                          import S_OK, S_ERROR, gLogger

import os, re, string

class ComputingElement:

  #############################################################################
  def __init__(self,ceName):
    """ Standard constructor
    """
    self.log  = gLogger
    self.name = ceName
    #self.log.setLevel('debug') #temporary for debugging
    self.classAd = ClassAd('[]')
    self.ceParameters = {}
    self.percentageRatio = 0.3
    result = self.__getCEParameters(ceName)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Failed to add CE parameters')
    result = self.__getSiteParameters()
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Failed to add site parameters')
    result = self.__getResourceRequirements()
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Failed to add resource requirements')

  #############################################################################
  def __getResourceRequirements(self):
    """Adds resource requirements to the ClassAd.
    """
    reqtSection = '/AgentJobRequirements'
    result = gConfig.getOptionsDict(reqtSection)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_OK(result['Message'])

    requirements = ''
    reqsDict = result['Value']
    for option,value in reqsDict.items():
      if type(value) == type(' '):
        jdlInt = self.__getInt(value)
        if type(jdlInt) == type(1):
          requirements += ' other.'+option+' == %d &&' %(jdlInt)
        else:
          requirements += ' other.'+option+' == "%s" &&' %(value)
      elif type(value) == type(1):
        requirements += ' other.'+option+' == %d &&' %(value)
      else:
        self.log.warn('Could not determine type of:  %s = %s' %(option,value))

    if requirements:
      if re.search('&&$',requirements):
        requirements = requirements[:-3]
      self.classAd.set_expression('Requirements',requirements)
    else:
      self.classAd.set_expression('Requirements','True')

    return S_OK('Added requirements')

  #############################################################################
  def __getInt(self,value):
    """To deal with JDL integer values.
    """
    tmpValue = None

    try:
     tmpValue = int(value.replace('"',''))
    except Exception, x:
      pass

    if tmpValue:
      value = tmpValue

    return value

  #############################################################################
  def __getSiteParameters(self):
    """Adds site specific parameters to the resource ClassAd.
    """
    osSection = '/Resources/Computing/OSCompatibility'
    result = gConfig.getOptionsDict(osSection)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR(result['Message'])
    if not result['Value']:
      self.log.warn('Could not obtain %s section from CS' %(osSection))
      return S_ERROR('Could not obtain %s section from CS' %(osSection))

    platforms = result['Value']
    self.log.debug('Platforms are %s' %(platforms))

    section = '/LocalSite'
    options = gConfig.getOptionsDict(section)
    if not options['OK']:
      self.log.warn(options['Message'])
      return S_ERROR(options['Message'])
    if not result['Value']:
      self.log.warn('Could not obtain %s section from CS' %(section))
      return S_ERROR('Could not obtain %s section from CS' %(section))

    localSite = options['Value']
    self.log.debug('Local site parameters are: %s' %(localSite))

    for option,value in localSite.items():
      if option == 'Architecture':
        if value in platforms.keys():
          compatiblePlatforms = platforms[value]
          self.classAd.insertAttributeVectorString('CompatiblePlatforms', compatiblePlatforms.split(', '))
      elif option == 'LocalSE':
        self.classAd.insertAttributeVectorString('LocalSE',value.split(', '))
      elif type(value) == type(' '):
        jdlInt = self.__getInt(value)
        if type(jdlInt) == type(1):
          self.log.debug('Found JDL integer attribute: %s = %s' %(option,jdlInt))
          self.classAd.insertAttributeInt(option, jdlInt)
        else:
          self.log.debug('Found string attribute: %s = %s' %(option,value))
          self.classAd.insertAttributeString(option, value)
      elif type(value) == type(1):
        self.log.debug('Found integer attribute: %s = %s' %(option,value))
        self.classAd.insertAttributeInt(option, value)
      else:
        self.log.warn('Type of option %s = %s not determined' %(option,value))

    return S_OK()

  #############################################################################
  def __getCEParameters(self,ceName):
    """Adds CE specific parameters to the resource ClassAd.
    """
    section = '/Resources/Computing/%s' % (ceName)
    options = gConfig.getOptionsDict(section)
    if not options['OK']:
      self.log.warn(options['Message'])
      return S_ERROR('Could not obtain %s section from CS' %(section))
    if not options['Value']:
      return S_ERROR('Empty CS section %s' %(section))

    ceOptions = options['Value']
    self.ceParameters = ceOptions
    for option,value in ceOptions.items():
      if type(value) == type(' '):
        jdlInt = self.__getInt(value)
        if type(jdlInt) == type(1):
          self.log.debug('Found JDL integer attribute: %s = %s' %(option,jdlInt))
          self.classAd.insertAttributeInt(option, jdlInt)
        else:
          self.log.debug('Found string attribute: %s = %s' %(option,value))
          self.classAd.insertAttributeString(option, value)
      elif type(value) == type(1):
        self.log.debug('Found integer attribute: %s = %s' %(option,value))
        self.classAd.insertAttributeInt(option, value)
      else:
        self.log.warn('Type of option %s = %s not determined' %(option,value))

    return S_OK()

  #############################################################################
  def __getParameters(self,param):
    """Returns CE parameter, e.g. MaxTotalJobs
    """
    if type(param)==type(' '):
      if param in self.ceParameters.keys():
        return S_OK(self.ceParameters[param])
      else:
        return S_ERROR('Parameter %s not found' %(param))
    elif type(param)==type([]):
      result = {}
      for p in param:
        if param in self.ceParameters.keys():
          result[param] = self.ceParameters[param]
      if len(result.keys()) == len(p):
        return S_OK(result)
      else:
        return S_ERROR('Not all specified parameters available')

  #############################################################################
  def available(self):
    """This method returns True if CE is available and false if not.  The CE
       instance polls for waiting and running jobs and compares to the limits
       in the CE parameters.
    """
    result = self.getDynamicInfo()
    if not result['OK']:
      self.log.warn('Could not obtain CE dynamic information')
      self.log.warn(result['Message'])
      runningJobs = 0
      waitingJobs = 0
      submittedJobs = 0
    else:
      runningJobs = result['Value']['RunningJobs']
      waitingJobs = result['Value']['WaitingJobs']
      submittedJobs = result['Value']['SubmittedJobs']

    maxTotalJobs = self.__getParameters('MaxTotalJobs')
    if not maxTotalJobs['OK']:
      self.log.warn('MaxTotalJobs is not specified')
      maxTotalJobs = 0
    else:
      maxTotalJobs = int(maxTotalJobs['Value'])

    totalCPU = self.__getParameters('TotalCPUs')
    if not totalCPU['OK']:
      self.log.warn('TotalCPUs not specified, setting default of 1')
      totalCPU = 1
    else:
      totalCPU = int(totalCPU['Value'])

    message=''
    if not waitingJobs and not runningJobs:
      message = '%sCE: SubmittedJobs=%s' %(self.name,submittedJobs)
      totalJobs = int(submittedJobs)
    else:
      message = '%sCE: WaitingJobs=%s, RunningJobs=%s' %(self.name,waitingJobs,runningJobs)
      totalJobs = int(runningJobs) + int(waitingJobs)

    if totalCPU:
      message +=', TotalCPU=%s' %(totalCPU)
    if maxTotalJobs:
      message +=', MaxTotalJobs=%s' %(maxTotalJobs)

    pendingJobRatio = (float(waitingJobs)/float(totalCPU)<float(self.percentageRatio))
    resourceAvailable = (waitingJobs == 0 or pendingJobRatio) and totalJobs < maxTotalJobs
    if resourceAvailable:
      return S_OK(message)
    else:
      message = 'There are %s waiting jobs, %.2f ratio and total jobs %s < %s max total jobs' % (waitingJobs,pendingJobRatio,totalJobs,maxTotalJobs)
      return S_ERROR(message)

  #############################################################################
  def getJDL(self):
    """Returns CE JDL as a string.
    """
    if self.classAd.isOK():
      jdl = self.classAd.asJDL()
      return S_OK(jdl)
    else:
      return S_ERROR('ClassAd job is not valid')

  #############################################################################
  def sendOutput(self,stdid,line):
    """ Callback function such that the results from the CE may be returned.
    """
    print line

  #############################################################################
  def submitJob(self,executableFile,jdl,localID):
    """ Method to submit job, should be overridden in sub-class.
    """
    name = 'submitJob()'
    self.log.error('ComputingElement: %s should be implemented in a subclass' %(name))
    return S_ERROR('ComputingElement: %s should be implemented in a subclass' %(name))

  #############################################################################
  def getDynamicInfo(self):
    """ Method to get dynamic job information, can be overridden in sub-class.
    """
    name = 'getDynamicInfo()'
    self.log.error('ComputingElement: %s should be implemented in a subclass' %(name))
    return S_ERROR('ComputingElement: %s should be implemented in a subclass' %(name))

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#