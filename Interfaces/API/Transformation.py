# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/API/Transformation.py $
__RCSID__ = "$Id: Transformation.py 19505 2009-12-15 15:43:27Z paterson $"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import string, os, shutil, types, pprint

from DIRAC                                                        import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.API                                          import API
from DIRAC.TransformationSystem.Client.TransformationDBClient     import TransformationDBClient

COMPONENT_NAME='Transformation'

class Transformation(API):

  #############################################################################
  def __init__(self):
    self.transClient = TransformationDBClient()
    #TODO REMOVE THIS
    self.transClient.setServer("ProductionManagement/ProductionManager")
    
  #############################################################################
  def getTransformations(self,transID=[], transStatus=[], outputFields=['TransformationID','Status','AgentType','TransformationName','CreationDate'],orderBy='TransformationID',printOutput=False):
    condDict = {}
    if transID:
      condDict['TransformationID'] = transID
    if transStatus:
      condDict['Status'] = transStatus
    res = self.transClient.getTransformations(condDict=condDict)
    if not res['OK']:
      return res
    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % string.join(res['ParameterNames']))
      else:
        self._printFormattedDictList(res['Value'],outputFields,'TransformationID',orderBy)
    return res

  #############################################################################
  def getTransformationLogging(self,transID,printOutput=False):
    """The logging information for the given transformation is returned.
    """
    pass

  #############################################################################
  def getTransformationProgress(self,transID=None,printOutput=False):
    """Returns the status of transformation
    """
    pass
  
  #############################################################################
  def checkFilesStatus(self,lfns,transID='',printOutput=False):
    """ Checks the given LFN(s) status in the transformation database. """
    pass

  #############################################################################
  def setTransformationFileStatus(self,lfns,transID,status,printOutput=False):
    """ Set status for the given files in the lfns list for supplied transformation ID """
    pass

  #############################################################################
  def getTransformationTaskInfo(self,transID,taskID,printOutput=False):
    """Retrieve transformation task information from transformation database """
    res = self.transClient.getTransformationTasks(condDict={'TransformationID':transID,'JobID':taskID},inputVector=True)
    pass
  
  #############################################################################
  def extendTransformation(self,transID,nTasks,printOutput=False):
    """ Extend Simulation type transformation by number of tasks. """
    pass

  #############################################################################
  def addTransformation(self,transName,description,longDescription,type,plugin,agentType,fileMask,
                            transformationGroup = 'General',
                            groupSize           = 1,
                            inheritedFrom       = 0,
                            body                = '', 
                            maxJobs             = 0,
                            eventsPerJob        = 0,
                            addFiles            = True,
                            printOutput = False):
    pass
              
  def deleteTransformation(self,transName,printOutput=False):
    pass
          
  def cleanTransformation(self,transName,printOutput=False):
    pass
          
  def setTransformationParameter(self,transName,paramName,paramValue,printOutput=False):
    pass

  def setTransformationStatus(self,transName,status,printOutput=False):
    pass

  def getTransformationParameters(self,transName,paramNames,printOutput=False):
    pass
          
  def getTransformationWithStatus(self,status,printOutput=False):
    pass
      
  def addFilesToTransformation(self,transName,lfns, printOutput=False):
    pass

  def addTaskForTransformation(self,transName,lfns=[],se='Unknown', printOutput=False):
    pass

  def setFileStatusForTransformation(self,transName,status,lfns,printOutput=False): 
    pass

  def setTaskStatus(self,transName, taskID, status, printOutput=False):
    pass
   
  def setTaskStatusAndWmsID(self,transName, taskID, status, taskWmsID, printOutput=False): 
    pass
          
  def getTransformationTaskStats(self,transName,printOutput=False):
    pass 
          
  def deleteTasks(self,transName, taskMin, taskMax, printOutput=False): 
    pass

  def getTasksToSubmit(self,transName,numTasks,site='', printOutput=False): 
    pass
      
  def getTransformationFileSummary(self,lfns,transName, printOutput=False):
    pass

  def getTransformationStats(self,transName, printOutput=False):
    pass

  def getTransformation(self,transName,extraParams=False,printOutput=False):
    pass

  def getTransformationFiles(self,condDict={},older=None, newer=None, timeStamp='LastUpdate', orderAttribute=None, limit=None, printOutput=False):
    pass
  
  def getTransformationTasks(self,condDict={},older=None, newer=None, timeStamp='CreationTime', orderAttribute=None, limit=None, inputVector=False, printOutput=False):
    pass
