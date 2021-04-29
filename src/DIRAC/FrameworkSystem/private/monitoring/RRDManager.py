""" This class is a wrap around the rrdtool as it is a command line based tool within this class there are
    several methods which take in some parameters required by the corresponding rrd command and executes it.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os.path
import hashlib
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.FrameworkSystem.private.monitoring.ColorGenerator import ColorGenerator
from DIRAC.Core.Utilities import Subprocess, Time
from DIRAC.Core.Utilities.File import mkDir

__RCSID__ = "$Id$"


class RRDManager(object):

  __sizesList = [[200, 50], [400, 100], [600, 150], [800, 200]]
  __logRRDCommands = False

  def __init__(self, rrdLocation, graphLocation):
    """
    Initialize RRDManager
    """
    self.rrdLocation = rrdLocation
    self.graphLocation = graphLocation
    self.log = gLogger.getSubLogger("RRDManager")
    self.rrdExec = gConfig.getValue("%s/RRDExec" % getServiceSection("Framework/Monitoring"), "rrdtool")
    for path in (self.rrdLocation, self.graphLocation):
      mkDir(path)

  def existsRRDFile(self, rrdFile):
    """ Checks whether a given rrd file exists or not.

        :type rrdFile: string
        :param rrdFile: name of the rrd file.
        :return: bool
    """
    rrdFilePath = "%s/%s" % (self.rrdLocation, rrdFile)
    return os.path.isfile(rrdFilePath)

  def getGraphLocation(self):
    """
    Sets the location for graph files
    """
    return self.graphLocation

  def __exec(self, cmd, rrdFile=None):
    """
    Executes a system command.

    :type cmd: string
    :param cmd: The cmd command to be executed.
    :type rrdFile: string
    :param rrdFile: name of the rrd file.
    :return: The value dictionary / S_ERROR with a message.
    """
    self.log.debug("RRD command: %s" % cmd)
    retVal = Subprocess.shellCall(0, cmd)
    if self.__logRRDCommands and rrdFile:
      try:
        logFile = "%s.log" % rrdFile
        with open(logFile, "a") as fd:
          if not retVal['OK'] or retVal['Value'][0]:
            fd.write("ERROR %s\n" % cmd)
          else:
            fd.write("OK    %s\n" % cmd)
      except Exception as e:
        self.log.warn("Cannot write log %s: %s" % (logFile, str(e)))
    if not retVal['OK']:
      return retVal
    retTuple = retVal['Value']
    if retTuple[0]:
      return S_ERROR("Failed to execute rrdtool: %s" % (retTuple[2]))
    return retVal

  def getCurrentBucketTime(self, bucketLength):
    """
    Gets current time "bucketized"
    """
    return self.bucketize(Time.toEpoch(), bucketLength)

  def bucketize(self, secs, bucketLength):
    """
    Bucketizes a time (in secs)
    """
    secs = int(secs)
    return secs - secs % bucketLength

  def create(self, type, rrdFile, bucketLength):
    """
    Creates an rrd file.

    :type rrdFile: string
    :param rrdFile: name of the rrd file.
    :type bucketLength: int
    :param bucketLength: The required bucket length.
    :return: The value dictionary / S_ERROR with a message.
    """
    # Understanding this method it basically takes in the activity data which is sent from the client i.e. the gMonitor
    # object and checks if the file is created by the MonitoringCatalog as it creates an .rrd file using the
    # registerActivity method within it.

    rrdFilePath = "%s/%s" % (self.rrdLocation, rrdFile)
    if os.path.isfile(rrdFilePath):
      return S_OK()
    try:
      os.makedirs(os.path.dirname(rrdFilePath))
    except Exception:
      pass
    self.log.info("Creating rrd file %s" % rrdFile)
    cmd = "%s create '%s'" % (self.rrdExec, rrdFilePath)
    # Start GMT(now) - 1h
    cmd += " --start %s" % (self.getCurrentBucketTime(bucketLength) - 86400)
    cmd += " --step %s" % bucketLength
    if type in ('mean'):
      dst = "GAUGE"
      cf = "AVERAGE"
    elif type in ('sum', 'acum', 'rate'):
      dst = "ABSOLUTE"
      cf = "AVERAGE"
    cmd += " DS:value:%s:%s:U:U" % (dst, bucketLength * 10)
    # 1m res for 1 month
    # cmd += " RRA:%s:0.9:1:43200" % cf
    # 1m red for 1 year
    cmd += " RRA:%s:0.999:1:%s" % (cf, int(31536000 / bucketLength))
    return self.__exec(cmd, rrdFilePath)

  def __getLastUpdateTime(self, rrdFile):
    """
    Gets last update time from an rrd.

    :type rrdFile: string
    :param rrdFile: name of the rrd file.
    :return: S_OK / S_ERROR with a message.
    """
    cmd = "%s last %s" % (self.rrdExec, rrdFile)
    retVal = Subprocess.shellCall(0, cmd)
    if not retVal['OK']:
      return retVal
    retTuple = retVal['Value']
    if retTuple[0]:
      return S_ERROR("Failed to fetch last update %s : %s" % (rrdFile, retTuple[2]))
    return S_OK(int(retTuple[1].strip()))

  def __fillWithZeros(self, lastUpdateTime, bucketLength, valuesList):
    filledList = []
    expectedUpdateTime = lastUpdateTime + bucketLength
    for valueTuple in valuesList:
      while expectedUpdateTime < valueTuple[0]:
        filledList.append((expectedUpdateTime, 0))
        expectedUpdateTime += bucketLength
      filledList.append(valueTuple)
      expectedUpdateTime = valueTuple[0] + bucketLength
    return filledList

  def update(self, type, rrdFile, bucketLength, valuesList, lastUpdate=0):
    """
    Updates an rrd file.

    :type rrdFile: string
    :param rrdFile: name of the rrd file.
    :type bucketLength: int
    :param bucketLength: The required bucket length.
    :type valuesList: list
    :param valuesList: a list of values to be updated.
    :type lastUpdate: int
    :param lastUpdate: The timestamp of the last update made to the rrd file.
    :return: S_OK with the updated values list.
    """
    rrdFilePath = "%s/%s" % (self.rrdLocation, rrdFile)
    self.log.info("Updating rrd file", rrdFilePath)
    if lastUpdate == 0:
      retVal = self.__getLastUpdateTime(rrdFilePath)
      if retVal['OK']:
        lastUpdateTime = retVal['Value']
        self.log.verbose("Last update time is %s" % lastUpdateTime)
    else:
      lastUpdateTime = lastUpdate
    cmd = "%s update %s" % (self.rrdExec, rrdFilePath)
    # we have to fill with 0 the db to ensure the mean is valid
    valuesList = self.__fillWithZeros(lastUpdateTime, bucketLength, valuesList)
    rrdUpdates = []
    for entry in valuesList:
      rrdUpdates.append("%s:%s" % entry)
    maxRRDArgs = 50
    for i in range(0, len(rrdUpdates), maxRRDArgs):
      finalCmd = "%s %s" % (cmd, " ".join(rrdUpdates[i: i + maxRRDArgs]))
      retVal = self.__exec(finalCmd, rrdFilePath)
      if not retVal['OK']:
        self.log.warn("Error updating rrd file", "%s rrd: %s" % (rrdFile, retVal['Message']))
    return S_OK(valuesList[-1][0])

  def __generateName(self, *args, **kwargs):
    """
    Generates a random name
    """
    m = hashlib.md5()
    m.update(str(args).encode())
    m.update(str(kwargs).encode())
    return m.hexdigest()

  def __generateRRDGraphVar(self, entryName, activity, timeSpan, plotWidth):
    """
    Calculates the graph query in rrd lingo for an activity
    """
    rrdFile = activity.getFile()
    rrdType = activity.getType()
    bucketLength = activity.getBucketLength()
    yScaleFactor = self.__getYScalingFactor(timeSpan, bucketLength, plotWidth)
    activity.setBucketScaleFactor(yScaleFactor)
    varStr = "'DEF:ac%sRAW=%s/%s:value:AVERAGE'" % (entryName, self.rrdLocation, rrdFile)
    if rrdType in ("mean", "rate"):
      varStr += " 'CDEF:%s=ac%sRAW,UN,0,ac%sRAW,IF'" % (entryName, entryName, entryName)
    elif rrdType == "sum":
      scale = yScaleFactor * bucketLength
      varStr += " 'CDEF:%s=ac%sRAW,UN,0,ac%sRAW,%s,*,IF'" % (entryName, entryName, entryName, scale)
    elif rrdType == "acum":
      scale = yScaleFactor * bucketLength
      varStr += " 'CDEF:ac%sNOTUN=ac%sRAW,UN,0,ac%sRAW,%s,*,IF'" % (entryName, entryName, entryName, scale)
      varStr += " 'CDEF:%s=PREV,UN,ac%sNOTUN,PREV,ac%sNOTUN,+,IF'" % (entryName, entryName, entryName)
    return varStr

  def __graphTimeComment(self, fromEpoch, toEpoch):
    comStr = " 'COMMENT:Generated on %s UTC'" % Time.toString().replace(":", r"\:").split(".")[0]
    comStr += " 'COMMENT:%s'" % ("From %s to %s" % (Time.fromEpoch(fromEpoch),
                                                    Time.fromEpoch(toEpoch))).replace(":", r"\:")
    return comStr

  def __getYScalingFactor(self, timeSpan, bucketLength, plotWidth):
    expectedTimeSpan = plotWidth * bucketLength
    if timeSpan < expectedTimeSpan:
      return 1
    else:
      return float(timeSpan) / expectedTimeSpan

  def groupPlot(self, fromSecs, toSecs, activitiesList, stackActivities, size, graphFilename=""):
    """
    Generates a group plot.

    :type fromSecs: int
    :param fromSecs: A value in seconds from where to start.
    :type toSecs: int
    :param toSecs: A value in seconds for where to end.
    :type activitiesList: list
    :param activitiesList: A list of activities.
    :type stackActivities: list
    :param stackActivities: A list of stacked activities.
    :type size: int
    :param size: There is a matrix defined for size so here only one of these values go [0, 1, 2, 3].
    :type graphFilename: string
    :param graphFilename: A name for the graph file.
    :return: S_OK with the graph filename / The error message.
    """
    plotTimeSpan = toSecs - fromSecs
    if not graphFilename:
      graphFilename = "%s.png" % self.__generateName(fromSecs,
                                                     toSecs,
                                                     activitiesList,
                                                     stackActivities
                                                     )
    rrdCmd = "%s graph %s/%s" % (self.rrdExec, self.graphLocation, graphFilename)
    rrdCmd += " -s %s" % fromSecs
    rrdCmd += " -e %s" % toSecs
    rrdCmd += " -w %s" % self.__sizesList[size][0]
    rrdCmd += " -h %s" % self.__sizesList[size][1]
    rrdCmd += " --title '%s'" % activitiesList[0].getGroupLabel()
    colorGen = ColorGenerator()
    activitiesList.sort()
    for idActivity in range(len(activitiesList)):
      activity = activitiesList[idActivity]
      rrdCmd += " %s" % self.__generateRRDGraphVar(idActivity, activity, plotTimeSpan, self.__sizesList[size][0])
      if stackActivities:
        rrdCmd += " 'AREA:%s#%s:%s:STACK'" % (idActivity, colorGen.getHexColor(),
                                              activity.getLabel().replace(":", r"\:"))
      else:
        rrdCmd += " 'LINE1:%s#%s:%s'" % (idActivity, colorGen.getHexColor(), activity.getLabel().replace(":", r"\:"))
    rrdCmd += self.__graphTimeComment(fromSecs, toSecs)
    retVal = self.__exec(rrdCmd)
    if not retVal['OK']:
      return retVal
    return S_OK(graphFilename)

  def plot(self, fromSecs, toSecs, activity, stackActivities, size, graphFilename=""):
    """
    Generates a non grouped plot.

    :type fromSecs: int
    :param fromSecs: A value in seconds from where to start.
    :type toSecs: int
    :param toSecs: A value in seconds for where to end.
    :type activitiesList: list
    :param activitiesList: A list of activities.
    :type stackActivities: list
    :param stackActivities: A list of stacked activities.
    :type size: int
    :param size: There is a matrix defined for size so here only one of these values go [0, 1, 2, 3].
    :type graphFilename: string
    :param graphFilename: A name for the graph file.
    :return: S_OK with the graph filename / The error message.
    """
    plotTimeSpan = toSecs - fromSecs
    if not graphFilename:
      graphFilename = "%s.png" % self.__generateName(fromSecs,
                                                     toSecs,
                                                     activity,
                                                     stackActivities
                                                     )
    graphVar = self.__generateRRDGraphVar(0, activity, plotTimeSpan, self.__sizesList[size][0])
    rrdCmd = "%s graph %s/%s" % (self.rrdExec, self.graphLocation, graphFilename)
    rrdCmd += " -s %s" % fromSecs
    rrdCmd += " -e %s" % toSecs
    rrdCmd += " -w %s" % self.__sizesList[size][0]
    rrdCmd += " -h %s" % self.__sizesList[size][1]
    rrdCmd += " --title '%s'" % activity.getLabel()
    rrdCmd += " --vertical-label '%s'" % activity.getUnit()
    rrdCmd += " %s" % graphVar
    if stackActivities:
      rrdCmd += " 'AREA:0#0000FF::STACK'"
    else:
      rrdCmd += " 'LINE1:0#0000FF'"
    rrdCmd += self.__graphTimeComment(fromSecs, toSecs)
    retVal = self.__exec(rrdCmd)
    if not retVal['OK']:
      return retVal
    return S_OK(graphFilename)

  def deleteRRD(self, rrdFile):
    """ This method is used to delete an rrd file.

        :type rrdFile: string
        :param rrdFile: name of the rrd file.
    """
    try:
      os.unlink("%s/%s" % (self.rrdLocation, rrdFile))
    except Exception as e:
      self.log.error("Could not delete rrd file", "%s: %s" % (rrdFile, str(e)))
