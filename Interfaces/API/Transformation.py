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
  def __init__(self,transID=0):
    self.paramTypes =   { 'TransformationID'      : [types.IntType,types.LongType],
                          'TransformationName'    : types.StringTypes,
                          'Status'                : types.StringTypes,
                          'Description'           : types.StringTypes,
                          'LongDescription'       : types.StringTypes,
                          'Type'                  : types.StringTypes,
                          'Plugin'                : types.StringTypes,
                          'AgentType'             : types.StringTypes,
                          'FileMask'              : types.StringTypes,
                          'TransformationGroup'   : types.StringTypes,
                          'GroupSize'             : [types.IntType,types.LongType],
                          'InheritedFrom'         : [types.IntType,types.LongType],
                          'Body'                  : types.StringTypes,
                          'MaxNumberOfJobs'       : [types.IntType,types.LongType],
                          'EventsPerJob'          : [types.IntType,types.LongType]}
    self.paramValues =  { 'TransformationID'      : 0,
                          'TransformationName'    : '',
                          'Status'                : 'New',
                          'Description'           : '',
                          'LongDescription'       : '',
                          'Type'                  : '',
                          'Plugin'                : 'Standard',
                          'AgentType'             : 'Manual',
                          'FileMask'              : '',
                          'TransformationGroup'   : 'General',
                          'GroupSize'             : 1,
                          'InheritedFrom'         : 0,
                          'Body'                  : '',
                          'MaxNumberOfJobs'       : 0,
                          'EventsPerJob'          : 0}
    self.extraParams  = {}

    self.transClient = TransformationDBClient()
    #TODO REMOVE THIS
    self.transClient.setServer("ProductionManagement/ProductionManager")
    self.exists = False
    if transID:
      self.paramValues['TransformationID'] = transID
      res = self.getTransformation()
      if res['OK']:
        self.exists = True
      else:
        self.paramValues['TransformationID'] = 0
        gLogger.fatal("The supplied transformation does not exist in transformation database", "%s @ %s" % (transID,self.transClient.serverURL))

  def __getattr__(self,name):
    if name.find('get') ==0:
      item = name[3:]
      self.item_called = item
      return self.__getParam
    if name.find('set') == 0:
      item = name[3:]
      self.item_called = item
      return self.__setParam
    raise AttributeError, name

  def __getParam(self):
    if self.item_called == 'Parameters':
      return S_OK(self.paramValues.keys()+self.extraParams.keys())
    if self.item_called in self.paramValues.keys():
      return S_OK(self.paramValues[self.item_called])
    if self.item_called in self.extraParams.keys():
      return S_OK(self.extraParams[self.item_called])
    raise AttributeError, "Unknown parameter for transformation: %s" % self.item_called

  def __setParam(self,value):
    if not self.item_called in self.paramTypes.keys():
       self.extraParams[self.item_called] = value
    else:
      if type(value) in self.paramTypes[self.item_called]:
        self.paramValues[self.item_called] = value
      else:
        raise TypeError, "%s %s %s expected one of %s" % (self.item_called,value,type(value),self.paramTypes[self.item_called])
    return S_OK()
      
  def getTransformation(self,printOutput=False):
    transID = self.paramValues['TransformationID']
    if not transID:
      gLogger.fatal("No TransformationID known")
      return S_ERROR()
    res = self.transClient.getTransformation(transID,extraParams=True)
    if not res['OK']:
      return res
    transParams = res['Value']
    for paramName,paramValue in transParams.items():
      self.item_called = paramName
      self.__setParam(paramValue)
    if printOutput:
      gLogger.info("No printing available yet")
    return S_OK(transParams)

  def getTransformationLogging(self,printOutput=False):
    transID = self.paramValues['TransformationID']
    if not transID:
      gLogger.fatal("No TransformationID known")
      return S_ERROR()
    res = self.transClient.getTransformationLogging(transID)
    if not res['OK']:
      return res
    loggingList = res['Value']
    if printOutput:
      self._printFormattedDictList(loggingList,['Message','MessageDate','AuthorDN'],'MessageDate','MessageDate')
    return S_OK(loggingList)

  def getTransformationProgress(self,transID=None,printOutput=False):
    pass

  def extendTransformation(self,transID,nTasks,printOutput=False):
    pass

  def deleteTransformation(self,transName,printOutput=False):
    pass
          
  def cleanTransformation(self,transName,printOutput=False):
    pass
          
  def setTransformationParameter(self,transName,paramName,paramValue,printOutput=False):
    pass

  def setTransformationStatus(self,transName,status,printOutput=False):
    pass

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
  def checkFilesStatus(self,lfns,transID='',printOutput=False):
    """ Checks the given LFN(s) status in the transformation database. """
    pass

  #############################################################################
  def setTransformationFileStatus(self,lfns,transID,status,printOutput=False):
    """ Set status for the given files in the lfns list for supplied transformation ID """
    pass

  #############################################################################

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

  def getTransformationFiles(self,condDict={},older=None, newer=None, timeStamp='LastUpdate', orderAttribute=None, limit=None, printOutput=False):
    pass
  
  def getTransformationTasks(self,condDict={},older=None, newer=None, timeStamp='CreationTime', orderAttribute=None, limit=None, inputVector=False, printOutput=False):
    pass
