########################################################################
# $Id: DataLoggingHandler.py,v 1.2 2008/02/21 18:32:49 acsmith Exp $
########################################################################

""" DataLoggingHandler is the implementation of the Data Logging
    service in the DISET framework

    The following methods are available in the Service interface

    addFileRecord()
    getFileLoggingInfo()

"""

__RCSID__ = "$Id: DataLoggingHandler.py,v 1.2 2008/02/21 18:32:49 acsmith Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.DataLoggingDB import DataLoggingDB
from DIRAC.Core.Utilities.Graph import Graph
import time,os
# This is a global instance of the DataLoggingDB class
logDB = False

def initializeDataLoggingHandler( serviceInfo ):

  global logDB
  logDB = DataLoggingDB()
  return S_OK()

class DataLoggingHandler( RequestHandler ):

  ###########################################################################
  types_addFileRecord = [StringType,StringType,StringType,StringType,StringType]
  def export_addFileRecord(self,lfn,status,minor,date,source):
    """ Add a logging record for the given file
    """

    result = logDB.addFileRecord(lfn,status,minor,date,source)
    return result

  ###########################################################################
  types_getFileLoggingInfo = [StringType]
  def export_getFileLoggingInfo(self,lfn):
    """ Get the file logging information
    """
    result = logDB.getFileLoggingInfo(lfn)
    return result

  types_getUniqueStates = []
  def export_getUniqueStates(self):
    """ Get all the unique states
    """
    result = logDB.getUniqueStates()
    return result

  types_plotView = [DictType]
  def export_plotView(self,paramsDict):
    """  Plot the view for the supplied parameters
    """ 
    
    startState = paramsDict['StartState']
    endState = paramsDict['EndState']
    startTime = ''
    endTime = ''
    title = 'Time between %s and %s events' % (startState,endState)
    if paramsDict.has_key('StartTime'):
      startTime = paramsDict['StartTime']
    if paramsDict.has_key('EndTime'):
      endTime = paramsDict['EndTime']
    xlabel = 'Time between states (min)'
    ylabel = ''
    res = logDB.getStateDiff(startState,endState,startTime,endTime)
    if not res['OK']:
      return S_ERROR('Failed to get DB info: %s' % res['Message'])
    dataPoints = res['Value']
    outputFile = '%s-%s-%s' % (time.time(),startState,endState)
    res = Graph().histogram(title,xlabel,ylabel,dataPoints,outputFile)
    if not res['OK']:
      return res
    return S_OK('%s.png' % outputFile)
