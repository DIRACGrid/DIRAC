"""  Agent to manage a DIRAC site.
"""

from DIRAC.Core.Utilities.ModuleFactory                  import ModuleFactory
from DIRAC.Core.Utilities.ClassAd.ClassAdLight           import ClassAd
from DIRAC.Core.Utilities.TimeLeft.TimeLeft              import TimeLeft
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.Core.Security.Locations                       import getProxyLocation
from DIRAC.Resources.Computing.ComputingElementFactory   import ComputingElementFactory
from DIRAC.Resources.Computing.ComputingElement          import ComputingElement
from DIRAC                                               import S_OK, S_ERROR, gConfig, platform
from DIRAC.FrameworkSystem.Client.ProxyManagerClient     import gProxyManager
from DIRAC.Core.Security.Misc                            import getProxyInfo
from DIRAC.Core.Security                                 import Locations
from DIRAC.Core.Security                                 import Properties
import DIRAC

import os, sys, re, string, time, urllib

AGENT_NAME = 'WorkloadManagement/DiracSiteAgent'

class DiracSiteAgent(AgentModule):

  #############################################################################
  def initialize(self,loops=0):
    """Sets default parameters and creates CE instance
    """
    self.maxcount = loops

    self.logLevel = gConfig.getValue('DIRAC/LogLevel','INFO')
    self.siteRoot = gConfig.getValue('LocalSite/Root',DIRAC.rootPath)
    self.localArea = gConfig.getValue('LocalSite/LocalArea','/tmp')
    self.siteName = gConfig.getValue('LocalSite/Site','Unknown')
    self.cpuFactor = gConfig.getValue('LocalSite/CPUScalingFactor','Unknown')
    self.maxPilots = gConfig.getValue('LocalSite/MaxPilots',100)

    self.log.setLevel(self.logLevel)

    self.log.info("Log level set to",self.logLevel)

    #these options are temporary until the Matcher procedure for the DIRAC site exists
    #they determine for which jobs in the WMS pilots are submitted
    self.propertiesDict = {
                      '/DIRAC/Setup' : 'LHCb-Development',
                      '/LocalSite/Properties/OwnerDN' : '',
                      '/LocalSite/Site' : ''
                      }

    for propLocation, propDefault in self.propertiesDict.items():
      try:
        property = gConfig.getValue(propLocation, propDefault).replace('"','')
        self.propertiesDict[propLocation] = str( property )
      except Exception,x:
        print x
        return S_ERROR('Expected string for %s field' %propLocation)

    self.matchDict = {
                      'Setup' : self.propertiesDict['/DIRAC/Setup'],
                      'Site' : self.propertiesDict['/LocalSite/Site'],
                      'CPUTime' : 3000000,
#                      'GridMiddleware' : '',
#                      'LHCbPlatform' : '',
#                      'PilotType' : '',
#                      'JobType' : '',
#                      'OwnerGroup' : '',
#                      'GridCE' : '',
                      'OwnerDN' : self.propertiesDict['/LocalSite/Properties/OwnerDN'],
                      }

    #options to pass to the pilot
    self.pilotOptions = {
                         '/LocalSite/SharedArea' : '',
                         '/LocalSite/LocalArea' : '',
                         '/LocalSite/Architecture' : '',
                         '/LocalSite/CPUScalingFactor' : '',
                         '/LocalSite/LocalCE' : 'InProcess',
                         '/LocalSite/Site' : '',
                         '/LocalSite/ConcurrentJobs' : '',
                         '/LocalSite/MaxCPUTime' : ''
                         }
    for optName, optDefault in self.pilotOptions.items():
        self.pilotOptions[optName] = gConfig.getValue('%(optName)s' %{'optName':optName}, optDefault)

    self.log.debug('======= Pilot Options =======')
    self.log.debug(self.pilotOptions)
    self.log.debug('=============================')

    #create CE
    ceUniqueID = self.am_getOption('CEUniqueID','Torque')
    if not ceUniqueID['OK']:
      self.log.warn(ceUniqueID['Message'])
      return ceUniqueID
    self.ceName = ceUniqueID['Value']
    ce = self.__createCE(self.ceName)
    if not ce['OK']:
      self.log.warn(ce['Message'])
      return ce
    self.computingElement = ce['Value']

    #path to dirac-pilot script
    self.diracPilotFileName = 'dirac-pilot'
    self.diracPilotPath = self.siteRoot + '/DIRAC/WorkloadManagementSystem/PilotAgent/' + self.diracPilotFileName

    #path to dirac-install script
    self.diracInstallFileName = 'dirac-install'
    self.diracInstallURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/scripts/dirac-install'
    self.diracInstallPath = self.siteRoot + '/' + self.diracInstallFileName
    if not os.path.exists(self.diracInstallPath):
      self.diracInstallPath = self.diracInstallFileName
      try:
        urllib.urlretrieve(self.diracInstallURL, self.diracInstallPath)
        os.chmod(self.diracInstallPath, 0755)
      except:
        self.log.error('Failed to retrieve %(diracInstallFileName)s from %(diracInstallUrl)s' % {'diracInstallFileName':self.diracInstallFileName,'diracInstallUrl':self.diracInstallURL})

    return result

  #############################################################################
  def execute(self):
    """The CE Agent execution method.
       Retrieve number of pilots to submit and submit them to the Batch System
       depending on the restrictions set by the site administrator.
    """

    self.log.verbose('CE Agent execution loop')

    #available = self.computingElement.available()
    #if not available['OK']:
    #  self.log.info('Resource is not available')
    #  self.log.info(available['Message'])
    #  return self.__finish('CE Not Available')

    #self.log.info(available['Value'])


    start = time.time()
    ret = self.__getPilots()
    if not ret['OK']:
      self.log.warn(ret['Message'])
      return S_OK
    elif ret['Value'].__contains__('No jobs'):
      self.log.debug('get Pilot return value',ret['Value'])
      return ret
    pilots = ret['Value']

    matchTime = time.time() - start
    self.log.verbose('Pilot Matcher Time = %.2f (s)' %(matchTime))

    ret = self.__submitPilots(pilots,self.computingElement)
    if not ret['OK']:
      self.log.warn(ret['Message'])

    return ret


  #############################################################################
  def __createCE(self,ceName):
    self.log.info("Creating %s CE" %(ceName) )

    ceFactory = ComputingElementFactory(ceName)
    ret = ceFactory.getCE()
    if not ret['OK']:
      self.log.warn(ret['Message'])
      return ret

    ceJDL = ret['Value'].getJDL()
    self.log.debug('CE jdl',ceJDL)
    return ret

  #############################################################################
  def __createPilotFile(self):

    pilotOptionString = ''

    for optName, optValue in self.pilotOptions.items():
      if len(optValue) > 0:
        self.log.debug('option',optName)
        pilotOptionString = ''.join([pilotOptionString,' -o ',optName,'=',optValue])

    self.log.verbose("Pilot Options: ", pilotOptionString)

    pilotFileName = 'pilot.sh'

    fileContent = """#!/bin/bash
export LD_LIBRARY_PATH=
./%(diracPilotFileName)s%(options)s
    """  %  {'diracPilotFileName':self.diracPilotFileName,'options':pilotOptionString}

    fopen = open(pilotFileName,'w')
    fopen.write(fileContent)
    fopen.close()

    return pilotFileName

  #############################################################################
  def __getProxy(self):
    # get proxy
    proxyLocation = getProxyLocation()

    fopen = open(proxyLocation,'r')
    proxyString = fopen.read()
    fopen.close()

    return proxyString

  #############################################################################
  def __submitPilots(self,pilots,ce):
    resourceJDL = ''
    while len(pilots) > 0:
      pilot = pilots.pop(0)
      ret = ce.submitJob(pilot['pilotFile'],resourceJDL,pilot['proxyString'],'0',[self.diracInstallPath,self.diracPilotPath])
      if not ret['OK']:
        self.log.warn(ret['Message'])
        return ret
      self.log.debug("Result of Pilot submission:",ret)

    return S_OK()

  #############################################################################
  def __getPilots(self):

    rpcClient = RPCClient( "WorkloadManagement/Matcher" )
    result = rpcClient.getMatchingTaskQueues( self.matchDict )
    self.log.info('Matching result',result)

    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR(result)

    taskQueues = result['Value']

    numberOfJobs = 0

    for taskQueueID in taskQueues:
      numberOfJobs += taskQueues[taskQueueID]['Jobs']

    if not numberOfJobs:
      return S_OK('No jobs selected for conditions: %s' %self.matchDict)
    else:
      # numberOfPilots = len(jobIDs)
      pilots = []
      pilot = self.__createPilotFile()
      proxy = self.__getProxy()
      self.log.verbose('%s job(s) selected' %(numberOfJobs))
      # submit pilots for all jobs, but not more than configured in maxPilots
      for i in xrange(0,min(self.maxPilots,numberOfJobs)):
        pilots.append( {'pilotFile':pilot,'proxyString':proxy} )

      return S_OK(pilots)

  #############################################################################
  def __finish(self,message):
    """Force the Dirac Site Agent to complete gracefully.
    """
    self.log.info('Dirac site agent will stop with message "%s", execution complete.' %message)
    fd = open(self.controlDir+'/stop_agent','w')
    fd.write('Dirac site agent Stopped at %s [UTC]' % (time.asctime(time.gmtime())))
    fd.close()
    return S_OK(message)

  #############################################################################
  def finalize(self):
    """Force the Dirac site agent to complete gracefully.
    """

    Agent.finalize(self)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
