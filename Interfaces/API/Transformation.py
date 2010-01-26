# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/API/Transformation.py $
__RCSID__ = "$Id: Transformation.py 19505 $"

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
    API.__init__(self)
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

    self.transClient = TransformationDBClient()
    #TODO REMOVE THIS
    self.transClient.setServer("ProductionManagement/ProductionManager")
    self.exists = False
    print transID
    if transID:
      self.paramValues['TransformationID'] = transID
      res = self.getTransformation()
      if res['OK']:
        self.exists = True
      else:
        self.paramValues['TransformationID'] = 0
        gLogger.fatal("The supplied transformation does not exist in transformation database", "%s @ %s" % (transID,self.transClient.serverURL))

  def resetTransformation(self):
    self.__init__(self)

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
    if self.item_called == 'Available':
      return S_OK(self.paramTypes.keys())
    if self.item_called == 'Parameters':
      return S_OK(self.paramValues)
    if self.item_called in self.paramValues.keys():
      return S_OK(self.paramValues[self.item_called])
    raise AttributeError, "Unknown parameter for transformation: %s" % self.item_called

  def __setParam(self,value):
    change = False
    if self.item_called in self.paramTypes.keys():
      oldValue = self.paramValues[self.item_called]
      if oldValue != value:
        if type(value) in self.paramTypes[self.item_called]:
          change = True
        else:
          raise TypeError, "%s %s %s expected one of %s" % (self.item_called,value,type(value),self.paramTypes[self.item_called])
    if not self.item_called in self.paramTypes.keys():
      if self.paramValues.has_key(self.item_called):
        oldValue =  self.paramValues[self.item_called]
        if oldValue != value:
          change = True
    if not change:
      gLogger.verbose("No change of parameter %s required" % self.item_called)
    else:
      gLogger.verbose("Parameter %s to be changed" % self.item_called)
      transID = self.paramValues['TransformationID']
      if self.exists and transID:
        res = self.transClient.setTransformationParameter(transID,self.item_called,value)
        if not res['OK']:
          return res
      self.paramValues[self.item_called] = value
    return S_OK()

  def getTransformation(self,printOutput=False):
    transID = self.paramValues['TransformationID']
    if not transID:
      gLogger.fatal("No TransformationID known")
      return S_ERROR()
    res = self.transClient.getTransformation(transID,extraParams=True)
    if not res['OK']:
      self._prettyPrint(res)
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
      self._prettyPrint(res)
      return res
    loggingList = res['Value']
    if printOutput:
      self._printFormattedDictList(loggingList,['Message','MessageDate','AuthorDN'],'MessageDate','MessageDate')
    return S_OK(loggingList)

  def extendTransformation(self,nTasks, printOutput=False):
    return self.__executeOperation('extendTransformation', nTasks, printOutput=printOutput)

  def cleanTransformation(self,printOutput=False):
    return self.__executeOperation('cleanTransformation', printOutput=printOutput)

  def deleteTransformation(self,printOutput=False):
    return self.__executeOperation('deleteTransformation', printOutput=printOutput)

  def addFilesToTransformation(self,lfns, printOutput=False):
    return self.__executeOperation('addFilesToTransformation', lfns, printOutput=printOutput)

  def setFileStatusForTransformation(self,status,lfns,printOutput=False): 
    return self.__executeOperation('setFileStatusForTransformation', status, lfns, printOutput=printOutput)

  def getTransformationTaskStats(self,printOutput=False):
    return self.__executeOperation('getTransformationTaskStats', printOutput=printOutput)

  def getTransformationStats(self,printOutput=False):
    return self.__executeOperation('getTransformationStats',printOutput=printOutput)

  def deleteTasks(self,taskMin, taskMax, printOutput=False): 
    return self.__executeOperation('deleteTasks',taskMin,taskMax,printOutput=printOutput)

  def addTaskForTransformation(self,lfns=[],se='Unknown', printOutput=False):
    return self.__executeOperation('addTaskForTransformation',lfns=lfns,se=se,printOutput=printOutput)

  def setTaskStatus(self, taskID, status, printOutput=False):
    return self.__executeOperation('setTaskStatus',taskID, status,printOutput=printOutput)

  def __executeOperation(self,operation,*parms,**kwds):
    transID = self.paramValues['TransformationID']
    if not transID:
      gLogger.fatal("No TransformationID known")
      return S_ERROR()
    printOutput = kwds.pop('printOutput')
    execString = "res = self.transClient.%s(transID,*parms,**kwds)" % operation
    exec(execString)
    if printOutput:
      self._prettyPrint(res)
    return res
  
  def getTransformationFiles(self,fileStatus=[],lfns=[],outputFields=['FileID','LFN','Status','JobID','TargetSE','UsedSE','ErrorCount','InsertedTime','LastUpdate'], orderBy='FileID', printOutput=False):
    condDict = {'TransformationID':self.paramValues['TransformationID']}
    if fileStatus:
      condDict['Status'] = fileStatus
    if lfns:
      condDict['LFN'] = lfns
    res = self.transClient.getTransformationFiles(condDict=condDict)
    #TODO RETURN PROPERLY FROM THE SERVICE
    res['ParameterNames'] = ['Status','LastUpdate','TargetSE','TransformationID','LFN','JobID','UsedSE','InsertedTime','ErrorCount','FileID']
    if not res['OK']:
      self._prettyPrint(res)
      return res
    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % string.join(res['ParameterNames']))
      elif not res['Value']:
        gLogger.info("No tasks found for selection")
      else:
        self._printFormattedDictList(res['Value'],outputFields,'FileID',orderBy)
    return res

  def getTransformationTasks(self,taskStatus=[],taskIDs=[],outputFields=['TransformationID','JobID','WmsStatus','JobWmsID','TargetSE','CreationTime','LastUpdateTime'],orderBy='JobID',printOutput=False):
    condDict = {'TransformationID':self.paramValues['TransformationID']}
    if taskStatus:
      condDict['WmsStatus'] = taskStatus
    if taskIDs:
      condDict['JobID'] = taskIDs
    res = self.transClient.getTransformationTasks(condDict=condDict)
    if not res['OK']:
      self._prettyPrint(res)
      return res
    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % string.join(res['ParameterNames']))
      elif not res['Value']:
        gLogger.info("No tasks found for selection")
      else:
        self._printFormattedDictList(res['Value'],outputFields,'JobID',orderBy)
    return res

  #############################################################################
  def getTransformations(self,transID=[], transStatus=[], outputFields=['TransformationID','Status','AgentType','TransformationName','CreationDate'],orderBy='TransformationID',printOutput=False):
    condDict = {}
    if transID:
      condDict['TransformationID'] = transID
    if transStatus:
      condDict['Status'] = transStatus
    res = self.transClient.getTransformations(condDict=condDict)
    if not res['OK']:
      self._prettyPrint(res)
      return res
    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % string.join(res['ParameterNames']))
      elif not res['Value']:
        gLogger.info("No tasks found for selection")
      else:
        self._printFormattedDictList(res['Value'],outputFields,'TransformationID',orderBy)
    return res

  #############################################################################
  def addTransformation(self,addFiles=True, printOutput=False):
    if self.paramValues['TransformationID']:
      gLogger.info("You are currently working with an active transformation definition.")
      gLogger.info("If you wish to create a new transformation reset the TransformationID.")
      gLogger.info("oTransformation.setTransformationID(0)") 
      return S_ERROR()

    requiredParameters = ['TransformationName','Description' ,'LongDescription','Type']
    for parameter in requiredParameters: 
      if not self.paramValues[parameter]:
        gLogger.info("%s is not defined for this transformation. This is required..." % parameter)
        res = self.__promptForParameter(parameter)
        if not res['OK']:
          return res

    pluginParams = {}
    pluginParams['Broadcast'] = {}
    pluginParams['Broadcast']['SourceSE'] = [types.ListType] + list(types.StringTypes)
    pluginParams['Broadcast']['TargetSE'] = [types.ListType] + list(types.StringTypes)
    pluginParams['Standard'] = {}
    pluginParams['Standard']['GroupSize'] = [types.IntType,types.LongType]
    pluginParams['BySize'] = {}
    pluginParams['BySize']['GroupSize'] = [types.IntType,types.LongType]

    plugin = self.paramValues['Plugin']
    if not plugin in pluginParams.keys():
      gLogger.info("The selected Plugin (%s) is not known to the transformation agent." % self.paramValues['Plugin'])
      res = self.__promptForParameter('Plugin',choices=pluginParams.keys(),default='Standard')
      if not res['OK']:
        return res
    plugin = self.paramValues['Plugin']
 
    requiredParams = pluginParams[plugin].keys()
    gLogger.info("The plugin %s required the following parameters be set: %s" % (plugin,string.join(requiredParams,', ')))
    for requiredParam in requiredParams:
      if (not self.paramValues.has_key(requiredParam)) or (not self.paramValues[requiredParam]):
        res = self.__promptForParameter(requiredParam,insert=False)
        if not res['OK']:
          return res
        paramValue = res['Value']
        if requiredParam = 'TargetSE':
          pass
	
    print self.paramValues
    return S_OK()

  def __promptForParameter(self,parameter,choices = [], default = '',insert=True):
    res = self._promptUser("Please enter %s" % parameter, choices=choices,default=default)
    if not res['OK']:
      return self._errorReport(res)
    gLogger.info("%s will be set to '%s'" % (parameter,res['Value']))
    self.item_called = parameter
    paramValue = res['Value']
    if insert:
      res = self.__setParam(paramValue)
      if not res['OK']:
        return res
    return S_OK(paramValue)    

    """
                            ,transName,description,longDescription,type,plugin,agentType,fileMask,
                            transformationGroup = 'General',
                            groupSize           = 1,
                            inheritedFrom       = 0,
                            body                = '', 
                            maxJobs             = 0,
                            eventsPerJob        = 0,
                            addFiles            = True):
    """
