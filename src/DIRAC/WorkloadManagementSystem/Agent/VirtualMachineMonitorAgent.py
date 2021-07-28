""" VirtualMachineMonitorAgent plays the role of the watch dog for the Virtual Machine
"""

import os
import time
import glob

from DIRAC import S_OK, S_ERROR, gConfig, rootPath
from DIRAC.ConfigurationSystem.Client.Helpers import Operations
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import List, Network
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import virtualMachineDB

__RCSID__ = "$Id$"


class VirtualMachineMonitorAgent(AgentModule):

  def __getCSConfig(self):
    if not self.runningPod:
      return S_ERROR("/LocalSite/RunningPod is not defined")
    # Variables coming from the vm
    imgPath = "/Cloud/%s" % self.runningPod
    for csOption, csDefault, varName in (("MinWorkingLoad", 0.01, "vmMinWorkingLoad"),
                                         ("LoadAverageTimespan", 60, "vmLoadAvgTimespan"),
                                         ("HaltPeriod", 600, "haltPeriod"),
                                         ("HaltBeforeMargin", 300, "haltBeforeMargin"),
                                         ("HeartBeatPeriod", 300, "heartBeatPeriod"),
                                         ):

      path = "%s/%s" % (imgPath, csOption)
      value = self.op.getValue(path, csDefault)
      if not value > 0:
        return S_ERROR("%s has an incorrect value, must be > 0" % path)
      setattr(self, varName, value)

    for csOption, csDefault, varName in (
        ("JobWrappersLocation", "/scratch", "vmJobWrappersLocation"),
    ):

      path = "%s/%s" % (imgPath, csOption)
      value = gConfig.getValue(path, csDefault)
      if not value:
        return S_ERROR("%s points to an empty string, cannot be!" % path)
      setattr(self, varName, value)

    self.haltBeforeMargin = max(self.haltBeforeMargin, int(self.am_getPollingTime()) + 5)
    self.haltPeriod = max(self.haltPeriod, int(self.am_getPollingTime()) + 5)
    self.heartBeatPeriod = max(self.heartBeatPeriod, int(self.am_getPollingTime()) + 5)

    self.log.info("** VM Info **")
    self.log.info("Name                  : %s" % self.runningPod)
    self.log.info("Min Working Load      : %f" % self.vmMinWorkingLoad)
    self.log.info("Load Avg Timespan     : %d" % self.vmLoadAvgTimespan)
    self.log.info("Job wrappers location : %s" % self.vmJobWrappersLocation)
    self.log.info("Halt Period           : %d" % self.haltPeriod)
    self.log.info("Halt Before Margin    : %d" % self.haltBeforeMargin)
    self.log.info("HeartBeat Period      : %d" % self.heartBeatPeriod)
    if self.vmID:
      self.log.info("DIRAC ID              : %s" % self.vmID)
    if self.uniqueID:
      self.log.info("Unique ID             : %s" % self.uniqueID)
    self.log.info("*************")
    return S_OK()

  def __declareInstanceRunning(self):
    # Connect to VM monitor and register as running
    retries = 3
    sleepTime = 30
    for i in range(retries):
      result = virtualMachineDB.declareInstanceRunning(self.uniqueID, self.ipAddress)
      if result['OK']:
        self.log.info("Declared instance running")
        return result
      self.log.error("Could not declare instance running", result['Message'])
      if i < retries - 1:
        self.log.info("Sleeping for %d seconds and retrying" % sleepTime)
        time.sleep(sleepTime)

    return S_ERROR("Could not declare instance running after %d retries" % retries)

  def initialize(self):

    self.am_disableMonitoring()
    # Init vars
    self.runningPod = gConfig.getValue('/LocalSite/RunningPod')
    self.log.info("Running pod name of the image is %s" % self.runningPod)
    self.vmID = gConfig.getValue('/LocalSite/VMID')

    self.__loadHistory = []
    self.vmMinWorkingLoad = None
    self.vmLoadAvgTimespan = None
    self.vmJobWrappersLocation = None
    self.haltPeriod = None
    self.haltBeforeMargin = None
    self.heartBeatPeriod = None
    self.am_setOption("MaxCycles", 0)
    self.am_setOption("PollingTime", 60)

    # Discover net address
    self.ipAddress = None
    netData = Network.discoverInterfaces()
    for iface in sorted(netData):
      # Warning! On different clouds interface name may be different(eth, ens, ...)
      if "eth" in iface or "ens" in iface:
        self.ipAddress = netData[iface]['ip']
        self.log.info("IP Address is %s" % self.ipAddress)
        break

    # getting the stop policy
    self.op = Operations.Operations()
    self.vmStopPolicy = self.op.getValue("Cloud/%s/VMStopPolicy", 'elastic')
    self.log.info("vmStopPolicy is %s" % self.vmStopPolicy)

    # Declare instance running
    self.uniqueID = ''
    result = virtualMachineDB.getUniqueIDByName(self.vmID)
    if result['OK']:
      self.uniqueID = result['Value']
    result = self.__declareInstanceRunning()
    if not result['OK']:
      self.log.error("Could not declare instance running", result['Message'])
      self.__haltInstance()
      return S_ERROR("Halting!")

    self.__instanceInfo = result['Value']

    # Get the cs config
    result = self.__getCSConfig()
    if not result['OK']:
      return result

    return S_OK()

  def __getLoadAvg(self):
    result = self.__getCSConfig()
    if not result['OK']:
      return result
    with open("/proc/loadavg", "r") as fd:
      data = [float(v) for v in List.fromChar(fd.read(), " ")[:3]]
    self.__loadHistory.append(data)
    numRequiredSamples = max(self.vmLoadAvgTimespan / self.am_getPollingTime(), 1)
    while len(self.__loadHistory) > numRequiredSamples:
      self.__loadHistory.pop(0)
    self.log.info("Load averaged over %d seconds" % self.vmLoadAvgTimespan)
    self.log.info(" %d/%s required samples to average load" % (len(self.__loadHistory),
                                                               numRequiredSamples))
    avgLoad = 0
    for f in self.__loadHistory:
      avgLoad += f[0]
    return avgLoad / len(self.__loadHistory), len(self.__loadHistory) == numRequiredSamples

  def __getNumJobWrappers(self):
    if not os.path.isdir(self.vmJobWrappersLocation):
      return 0
    self.log.info("VM job wrappers path: %s" % self.vmJobWrappersLocation)
    jdlList = glob.glob(os.path.join(self.vmJobWrappersLocation, "*.jdl"))
    jdlList += glob.glob(os.path.join(self.vmJobWrappersLocation, "*", "*.jdl"))
    return len(jdlList)

  def execute(self):

    # Get load
    avgLoad, avgRequiredSamples = self.__getLoadAvg()
    self.log.info("Load Average is %.2f" % avgLoad)
    if not avgRequiredSamples:
      self.log.info(" Not all required samples yet there")
    # Do we need to send heartbeat?
    with open("/proc/uptime") as fd:
      uptime = float(List.fromChar(fd.read().strip(), " ")[0])
    hours = int(uptime / 3600)
    minutes = int(uptime - hours * 3600) / 60
    seconds = uptime - hours * 3600 - minutes * 60
    self.log.info("Uptime is %.2f (%d:%02d:%02d)" % (uptime, hours, minutes, seconds))
    # Num jobs
    numJobs = self.__getNumJobWrappers()
    self.log.info("There are %d job wrappers" % numJobs)
    if uptime % self.heartBeatPeriod <= self.am_getPollingTime():
      # Heartbeat time!
      self.log.info("Sending hearbeat...")
      result = virtualMachineDB.instanceIDHeartBeat(self.uniqueID, avgLoad, numJobs, 0, 0, )
      status = None
      if result['OK']:
        self.log.info(" heartbeat sent!")
        status = result['Value']
      else:
        if "Transition" in result["Message"]:
          self.log.error("Error on service:", result['Message'])
          status = result['State']
        else:
          self.log.error("Connection error", result["Message"])
      if status:
        self.__processHeartBeatMessage(status, avgLoad)

    # Do we need to check if halt?
    if avgRequiredSamples and uptime % self.haltPeriod + self.haltBeforeMargin > self.haltPeriod:
      self.log.info("Load average is %s (minimum for working instance is %s)" % (avgLoad,
                                                                                 self.vmMinWorkingLoad))
      # current stop polices: elastic (load) and never
      if self.vmStopPolicy == 'elastic':
        # If load less than X, then halt!
        if avgLoad < self.vmMinWorkingLoad:
          self.__haltInstance(avgLoad)
      if self.vmStopPolicy == 'never':
        self.log.info("VM stop policy is defined as never (until SaaS or site request)")
    return S_OK()

  def __processHeartBeatMessage(self, hbMsg, avgLoad=0.0):
    if hbMsg == 'stop':
      # Write stop file for jobAgent
      self.log.info("Received STOP signal. Writing stop files...")
      for agentName in ["WorkloadManagement/JobAgent"]:
        ad = os.path.join(*agentName.split("/"))
        stopDir = os.path.join(gConfig.getValue('/LocalSite/InstancePath', rootPath), 'control', ad)
        stopFile = os.path.join(stopDir, "stop_agent")
        try:
          if not os.path.isdir(stopDir):
            os.makedirs(stopDir)
          fd = open(stopFile, "w")
          fd.write("stop!")
          fd.close()
          self.log.info("Wrote stop file %s for agent %s" % (stopFile, agentName))
        except Exception as e:
          self.log.error("Could not write stop agent file", stopFile)
    if hbMsg == 'halt':
      self.__haltInstance(avgLoad)

  def __haltInstance(self, avgLoad=0.0):
    self.log.info("Halting instance...")
    retries = 3
    sleepTime = 10
    for i in range(retries):
      result = virtualMachineDB.declareInstanceHalting(self.uniqueID, avgLoad)
      if result['OK']:
        self.log.info("Declared instance halting")
        break
      self.log.error("Could not send halting state:", result['Message'])
      if i < retries - 1:
        self.log.info("Sleeping for %d seconds and retrying" % sleepTime)
        time.sleep(sleepTime)

    # self.log.info( "Executing system halt..." )
    # os.system( "halt" )
