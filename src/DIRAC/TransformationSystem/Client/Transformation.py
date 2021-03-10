"""A generic client for creating and managing transformations.

See the information about transformation parameters below.
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import six
import json

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.PromptUser import promptUser
from DIRAC.Core.Base.API import API
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.RequestManagementSystem.Client.Operation import Operation

COMPONENT_NAME = 'Transformation'

__RCSID__ = '$Id$'


class Transformation(API):

  #############################################################################
  def __init__(self, transID=0, transClient=None):
    """ c'tor
    """
    super(Transformation, self).__init__()

    self.paramTypes = {'TransformationID': six.integer_types,
                       'TransformationName': six.string_types,
                       'Status': six.string_types,
                       'Description': six.string_types,
                       'LongDescription': six.string_types,
                       'Type': six.string_types,
                       'Plugin': six.string_types,
                       'AgentType': six.string_types,
                       'FileMask': six.string_types,
                       'TransformationGroup': six.string_types,
                       'GroupSize': six.integer_types + (float,),
                       'InheritedFrom': six.integer_types,
                       'Body': six.string_types,
                       'MaxNumberOfTasks': six.integer_types,
                       'EventsPerTask': six.integer_types}
    self.paramValues = {'TransformationID': 0,
                        'TransformationName': '',
                        'Status': 'New',
                        'Description': '',
                        'LongDescription': '',
                        'Type': '',
                        'Plugin': 'Standard',
                        'AgentType': 'Manual',
                        'FileMask': '',
                        'TransformationGroup': 'General',
                        'GroupSize': 1,
                        'InheritedFrom': 0,
                        'Body': '',
                        'MaxNumberOfTasks': 0,
                        'EventsPerTask': 0}

    # the metaquery parameters are neither part of the transformation parameters nor the additional parameters, so
    # special treatment is necessary
    self.inputMetaQuery = None
    self.outputMetaQuery = None

    self.ops = Operations()
    self.supportedPlugins = self.ops.getValue('Transformations/AllowedPlugins',
                                              ['Broadcast', 'Standard', 'BySize', 'ByShare'])
    if not transClient:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient
    self.serverURL = self.transClient.getServer()
    self.exists = False
    if transID:
      self.paramValues['TransformationID'] = transID
      res = self.getTransformation()
      if res['OK']:
        self.exists = True
      elif res['Message'] == 'Transformation does not exist':
        raise AttributeError('TransformationID %d does not exist' % transID)
      else:
        self.paramValues['TransformationID'] = 0
        gLogger.fatal("Failed to get transformation from database", "%s @ %s" % (transID,
                                                                                 self.transClient.serverURL))

  def getServer(self):
    return self.serverURL

  def reset(self, transID=0):
    self.__init__(transID)
    self.transClient.setServer(self.serverURL)
    return S_OK()

  def setTargetSE(self, seList):
    return self.__setSE('TargetSE', seList)

  def setSourceSE(self, seList):
    return self.__setSE('SourceSE', seList)

  def setBody(self, body):
    """ check that the body is a string, or using the proper syntax for multiple operations

    :param body: transformation body, for example

      .. code :: python

        body = [ ( "ReplicateAndRegister", { "SourceSE":"FOO-SRM", "TargetSE":"BAR-SRM" }),
                 ( "RemoveReplica", { "TargetSE":"FOO-SRM" } ),
               ]

    :type body: string or list of tuples (or lists) of string and dictionaries
    :raises TypeError: If the structure is not as expected
    :raises ValueError: If unknown attribute for the :class:`~DIRAC.RequestManagementSystem.Client.Operation.Operation`
                        is used
    :returns: S_OK, S_ERROR
    """
    self.item_called = "Body"
    if isinstance(body, six.string_types):
      return self.__setParam(body)
    if not isinstance(body, (list, tuple)):
      raise TypeError("Expected list or string, but %r is %s" % (body, type(body)))

    for tup in body:
      if not isinstance(tup, (tuple, list)):
        raise TypeError("Expected tuple or list, but %r is %s" % (tup, type(tup)))
      if len(tup) != 2:
        raise TypeError("Expected 2-tuple, but %r is length %d" % (tup, len(tup)))
      if not isinstance(tup[0], six.string_types):
        raise TypeError("Expected string, but first entry in tuple %r is %s" % (tup, type(tup[0])))
      if not isinstance(tup[1], dict):
        raise TypeError("Expected dictionary, but second entry in tuple %r is %s" % (tup, type(tup[0])))
      for par, val in tup[1].items():
        if not isinstance(par, six.string_types):
          raise TypeError("Expected string, but key in dictionary %r is %s" % (par, type(par)))
        if par not in Operation.ATTRIBUTE_NAMES:
          raise ValueError("Unknown attribute for Operation: %s" % par)
        if not isinstance(val, six.string_types + six.integer_types + (float, list, tuple, dict)):
          raise TypeError("Cannot encode %r, in json" % (val))
      return self.__setParam(json.dumps(body))

  def setInputMetaQuery(self, query):
    """Set the input meta query.

    :param dict query: dictionary to use for input meta query
    """
    self.inputMetaQuery = query
    return S_OK()

  def setOutputMetaQuery(self, query):
    """Set the output meta query.

    :param dict query: dictionary to use for output meta query
    """
    self.outputMetaQuery = query
    return S_OK()

  def __setSE(self, seParam, seList):
    if isinstance(seList, six.string_types):
      try:
        seList = eval(seList)
      except Exception:
        seList = seList.split(',')
    elif isinstance(seList, (list, dict, tuple)):
      seList = list(seList)
    else:
      return S_ERROR("Bad argument type")
    res = self.__checkSEs(seList)
    if not res['OK']:
      return res
    self.item_called = seParam
    return self.__setParam(seList)

  def __getattr__(self, name):
    if name.find('get') == 0:
      item = name[3:]
      self.item_called = item
      return self.__getParam
    if name.find('set') == 0:
      item = name[3:]
      self.item_called = item
      return self.__setParam
    raise AttributeError(name)

  def __getParam(self):
    if self.item_called == 'Available':
      return S_OK(list(self.paramTypes))
    if self.item_called == 'Parameters':
      return S_OK(self.paramValues)
    if self.item_called in self.paramValues:
      return S_OK(self.paramValues[self.item_called])
    raise AttributeError("Unknown parameter for transformation: %s" % self.item_called)

  def __setParam(self, value):
    change = False
    if self.item_called in self.paramTypes:
      if self.paramValues[self.item_called] != value:
        if isinstance(value, self.paramTypes[self.item_called]):
          change = True
        else:
          raise TypeError("%s %s %s expected one of %s" % (self.item_called, value, type(value),
                                                           self.paramTypes[self.item_called]))
    else:
      if self.item_called not in self.paramValues:
        change = True
      else:
        if self.paramValues[self.item_called] != value:
          change = True
    if not change:
      gLogger.verbose("No change of parameter %s required" % self.item_called)
    else:
      gLogger.verbose("Parameter %s to be changed" % self.item_called)
      transID = self.paramValues['TransformationID']
      if self.exists and transID:
        res = self.transClient.setTransformationParameter(transID, self.item_called, value)
        if not res['OK']:
          return res
      self.paramValues[self.item_called] = value
    return S_OK()

  def getTransformation(self, printOutput=False):
    transID = self.paramValues['TransformationID']
    if not transID:
      gLogger.fatal("No TransformationID known")
      return S_ERROR()
    res = self.transClient.getTransformation(transID, extraParams=True)
    if not res['OK']:
      if printOutput:
        self._prettyPrint(res)
      return res
    transParams = res['Value']
    for paramName, paramValue in transParams.items():
      setter = None
      setterName = "set%s" % paramName
      if hasattr(self, setterName) and callable(getattr(self, setterName)):
        setter = getattr(self, setterName)
      if not setterName:
        gLogger.error("Unable to invoke setter %s, it isn't a member function" % setterName)
        continue
      setter(paramValue)
    if printOutput:
      gLogger.info("No printing available yet")
    return S_OK(transParams)

  def getTransformationLogging(self, printOutput=False):
    transID = self.paramValues['TransformationID']
    if not transID:
      gLogger.fatal("No TransformationID known")
      return S_ERROR()
    res = self.transClient.getTransformationLogging(transID)
    if not res['OK']:
      if printOutput:
        self._prettyPrint(res)
      return res
    loggingList = res['Value']
    if printOutput:
      self._printFormattedDictList(loggingList, ['Message', 'MessageDate', 'AuthorDN'], 'MessageDate', 'MessageDate')
    return S_OK(loggingList)

  def extendTransformation(self, nTasks, printOutput=False):
    return self.__executeOperation('extendTransformation', nTasks, printOutput=printOutput)

  def cleanTransformation(self, printOutput=False):
    res = self.__executeOperation('cleanTransformation', printOutput=printOutput)
    if res['OK']:
      self.paramValues['Status'] = 'Cleaned'
    return res

  def deleteTransformation(self, printOutput=False):
    res = self.__executeOperation('deleteTransformation', printOutput=printOutput)
    if res['OK']:
      self.reset()
    return res

  def addFilesToTransformation(self, lfns, printOutput=False):
    return self.__executeOperation('addFilesToTransformation', lfns, printOutput=printOutput)

  def setFileStatusForTransformation(self, status, lfns, printOutput=False):
    return self.__executeOperation('setFileStatusForTransformation', status, lfns, printOutput=printOutput)

  def getTransformationTaskStats(self, printOutput=False):
    return self.__executeOperation('getTransformationTaskStats', printOutput=printOutput)

  def getTransformationStats(self, printOutput=False):
    return self.__executeOperation('getTransformationStats', printOutput=printOutput)

  def deleteTasks(self, taskMin, taskMax, printOutput=False):
    return self.__executeOperation('deleteTasks', taskMin, taskMax, printOutput=printOutput)

  def addTaskForTransformation(self, lfns=[], se='Unknown', printOutput=False):
    return self.__executeOperation('addTaskForTransformation', lfns, se, printOutput=printOutput)

  def setTaskStatus(self, taskID, status, printOutput=False):
    return self.__executeOperation('setTaskStatus', taskID, status, printOutput=printOutput)

  def __executeOperation(self, operation, *parms, **kwds):
    transID = self.paramValues['TransformationID']
    if not transID:
      gLogger.fatal("No TransformationID known")
      return S_ERROR()
    printOutput = kwds.pop('printOutput')
    fcn = None
    if hasattr(self.transClient, operation) and callable(getattr(self.transClient, operation)):
      fcn = getattr(self.transClient, operation)
    if not fcn:
      return S_ERROR("Unable to invoke %s, it isn't a member funtion of TransformationClient")
    res = fcn(transID, *parms, **kwds)
    if printOutput:
      self._prettyPrint(res)
    return res

  def getTransformationFiles(self, fileStatus=[], lfns=[], outputFields=['FileID', 'LFN', 'Status', 'TaskID',
                                                                         'TargetSE', 'UsedSE', 'ErrorCount',
                                                                         'InsertedTime', 'LastUpdate'],
                             orderBy='FileID', printOutput=False):
    condDict = {'TransformationID': self.paramValues['TransformationID']}
    if fileStatus:
      condDict['Status'] = fileStatus
    if lfns:
      condDict['LFN'] = lfns
    res = self.transClient.getTransformationFiles(condDict=condDict)
    if not res['OK']:
      if printOutput:
        self._prettyPrint(res)
      return res
    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % res['ParameterNames'].join(' '))
      elif not res['Value']:
        gLogger.info("No tasks found for selection")
      else:
        self._printFormattedDictList(res['Value'], outputFields, 'FileID', orderBy)
    return res

  def getTransformationTasks(self, taskStatus=[], taskIDs=[], outputFields=['TransformationID', 'TaskID',
                                                                            'ExternalStatus', 'ExternalID',
                                                                            'TargetSE', 'CreationTime',
                                                                            'LastUpdateTime'],
                             orderBy='TaskID', printOutput=False):
    condDict = {'TransformationID': self.paramValues['TransformationID']}
    if taskStatus:
      condDict['ExternalStatus'] = taskStatus
    if taskIDs:
      condDict['TaskID'] = taskIDs
    res = self.transClient.getTransformationTasks(condDict=condDict)
    if not res['OK']:
      if printOutput:
        self._prettyPrint(res)
      return res
    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % res['ParameterNames'].join(' '))
      elif not res['Value']:
        gLogger.info("No tasks found for selection")
      else:
        self._printFormattedDictList(res['Value'], outputFields, 'TaskID', orderBy)
    return res

  #############################################################################
  def getTransformations(self, transID=[], transStatus=[], outputFields=['TransformationID', 'Status',
                                                                         'AgentType', 'TransformationName',
                                                                         'CreationDate'],
                         orderBy='TransformationID', printOutput=False):
    condDict = {}
    if transID:
      condDict['TransformationID'] = transID
    if transStatus:
      condDict['Status'] = transStatus
    res = self.transClient.getTransformations(condDict=condDict)
    if not res['OK']:
      if printOutput:
        self._prettyPrint(res)
      return res
    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % res['ParameterNames'].join(' '))
      elif not res['Value']:
        gLogger.info("No tasks found for selection")
      else:
        self._printFormattedDictList(res['Value'], outputFields, 'TransformationID', orderBy)
    return res

  #############################################################################
  def getAuthorDNfromProxy(self):
    """ gets the AuthorDN and username of the transformation from the uploaded proxy
    """
    username = ""
    author = ""
    res = getProxyInfo()
    if res['OK']:
      author = res['Value']['identity']
      username = res['Value']['username']
    else:
      gLogger.error("Unable to get uploaded proxy Info %s " % res['Message'])
      return S_ERROR(res['Message'])

    res = {'username': username, 'authorDN': author}
    return S_OK(res)

  #############################################################################
  def getTransformationsByUser(self, authorDN="", userName="", transID=[], transStatus=[],
                               outputFields=['TransformationID', 'Status',
                                              'AgentType', 'TransformationName',
                                             'CreationDate', 'AuthorDN'],
                               orderBy='TransformationID', printOutput=False):
    condDict = {}
    if authorDN == "":
      res = self.getAuthorDNfromProxy()
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR(res['Message'])
      else:
        foundUserName = res['Value']['username']
        foundAuthor = res['Value']['authorDN']
        # If the username whom created the uploaded proxy is different than the provided username report error and exit
        if not (userName == "" or userName == foundUserName):
          gLogger.error(
              "Couldn't resolve the authorDN for user '%s' from the uploaded proxy (proxy created by '%s')" %
              (userName, foundUserName))
          return S_ERROR(
              "Couldn't resolve the authorDN for user '%s' from the uploaded proxy (proxy created by '%s')" %
              (userName, foundUserName))

        userName = foundUserName
        authorDN = foundAuthor
        gLogger.info(
            "Will list transformations created by user '%s' with status '%s'" %
            (userName, ', '.join(transStatus)))
    else:
      gLogger.info("Will list transformations created by '%s' with status '%s'" % (authorDN, ', '.join(transStatus)))

    condDict['AuthorDN'] = authorDN
    if transID:
      condDict['TransformationID'] = transID
    if transStatus:
      condDict['Status'] = transStatus
    res = self.transClient.getTransformations(condDict=condDict)
    if not res['OK']:
      if printOutput:
        self._prettyPrint(res)
      return res

    if printOutput:
      if not outputFields:
        gLogger.info("Available fields are: %s" % res['ParameterNames'].join(' '))
      elif not res['Value']:
        gLogger.info("No tasks found for selection")
      else:
        self._printFormattedDictList(res['Value'], outputFields, 'TransformationID', orderBy)
    return res

  #############################################################################
  def getSummaryTransformations(self, transID=[]):
    """Show the summary for a list of Transformations

       Fields starting with 'F' ('J')  refers to files (jobs).
       Proc. stand for processed.
    """
    condDict = {'TransformationID': transID}
    orderby = []
    start = 0
    maxitems = len(transID)
    paramShowNames = ['TransformationID', 'Type', 'Status', 'Files_Total', 'Files_PercentProcessed',
                      'Files_Processed', 'Files_Unused', 'Jobs_TotalCreated', 'Jobs_Waiting',
                      'Jobs_Running', 'Jobs_Done', 'Jobs_Failed', 'Jobs_Stalled']
    # Below, the header used for each field in the printing: short to fit in one line
    paramShowNamesShort = ['TransID', 'Type', 'Status', 'F_Total', 'F_Proc.(%)', 'F_Proc.',
                           'F_Unused', 'J_Created', 'J_Wait', 'J_Run', 'J_Done', 'J_Fail', 'J_Stalled']
    dictList = []

    result = self.transClient.getTransformationSummaryWeb(condDict, orderby, start, maxitems)
    if not result['OK']:
      self._prettyPrint(result)
      return result

    if result['Value']['TotalRecords'] > 0:
      try:
        paramNames = result['Value']['ParameterNames']
        for paramValues in result['Value']['Records']:
          paramShowValues = map(lambda pname: paramValues[paramNames.index(pname)], paramShowNames)
          showDict = dict(zip(paramShowNamesShort, paramShowValues))
          dictList.append(showDict)

      except Exception as x:
        print('Exception %s ' % str(x))

    if not len(dictList) > 0:
      gLogger.error('No found transformations satisfying input condition')
      return S_ERROR('No found transformations satisfying input condition')
    else:
      print(self._printFormattedDictList(dictList, paramShowNamesShort, paramShowNamesShort[0], paramShowNamesShort[0]))

    return S_OK(dictList)

  #############################################################################
  def addTransformation(self, addFiles=True, printOutput=False):
    """Add transformation to the transformation system.

    Sets all parameters currently assigned to the transformation.

    :param bool addFiles: if True, immediately perform input data query
    :param bool printOutput: if True, print information about transformation
    """
    res = self._checkCreation()
    if not res['OK']:
      return self._errorReport(res, 'Failed transformation sanity check')
    if printOutput:
      gLogger.info("Will attempt to create transformation with the following parameters")
      self._prettyPrint(self.paramValues)

    res = self.transClient.addTransformation(self.paramValues['TransformationName'],
                                             self.paramValues['Description'],
                                             self.paramValues['LongDescription'],
                                             self.paramValues['Type'],
                                             self.paramValues['Plugin'],
                                             self.paramValues['AgentType'],
                                             self.paramValues['FileMask'],
                                             transformationGroup=self.paramValues['TransformationGroup'],
                                             groupSize=self.paramValues['GroupSize'],
                                             inheritedFrom=self.paramValues['InheritedFrom'],
                                             body=self.paramValues['Body'],
                                             maxTasks=self.paramValues['MaxNumberOfTasks'],
                                             eventsPerTask=self.paramValues['EventsPerTask'],
                                             addFiles=addFiles,
                                             inputMetaQuery=self.inputMetaQuery,
                                             outputMetaQuery=self.outputMetaQuery,
                                             )
    if not res['OK']:
      if printOutput:
        self._prettyPrint(res)
      return res
    transID = res['Value']
    self.exists = True
    self.setTransformationID(transID)
    gLogger.notice("Created transformation %d" % transID)
    for paramName, paramValue in self.paramValues.items():
      if paramName not in self.paramTypes:
        res = self.transClient.setTransformationParameter(transID, paramName, paramValue)
        if not res['OK']:
          gLogger.error("Failed to add parameter", "%s %s" % (paramName, res['Message']))
          gLogger.notice("To add this parameter later please execute the following.")
          gLogger.notice("oTransformation = Transformation(%d)" % transID)
          gLogger.notice("oTransformation.set%s(...)" % paramName)
    return S_OK(transID)

  def _checkCreation(self):
    """ Few checks
    """
    if self.paramValues['TransformationID']:
      gLogger.info("You are currently working with an active transformation definition.")
      gLogger.info("If you wish to create a new transformation reset the TransformationID.")
      gLogger.info("oTransformation.reset()")
      return S_ERROR()

    requiredParameters = ['TransformationName', 'Description', 'LongDescription', 'Type']
    for parameter in requiredParameters:
      if not self.paramValues[parameter]:
        gLogger.info("%s is not defined for this transformation. This is required..." % parameter)
        self.paramValues[parameter] = six.moves.input("Please enter the value of " + parameter + " ")

    plugin = self.paramValues['Plugin']
    if plugin:
      if plugin not in self.supportedPlugins:
        gLogger.info("The selected Plugin (%s) is not known to the transformation agent." % plugin)
        res = self.__promptForParameter('Plugin', choices=self.supportedPlugins, default='Standard')
        if not res['OK']:
          return res
        self.paramValues['Plugin'] = res['Value']

    plugin = self.paramValues['Plugin']

    return S_OK()

  def _checkBySizePlugin(self):
    return self._checkStandardPlugin()

  def _checkBySharePlugin(self):
    return self._checkStandardPlugin()

  def _checkStandardPlugin(self):
    groupSize = self.paramValues['GroupSize']
    if groupSize <= 0:
      gLogger.info("The GroupSize was found to be less than zero. It has been set to 1.")
      res = self.setGroupSize(1)
      if not res['OK']:
        return res
    return S_OK()

  def _checkBroadcastPlugin(self):
    gLogger.info("The Broadcast plugin requires the following parameters be set: %s" % (', '.join(['SourceSE',
                                                                                                   'TargetSE'])))
    requiredParams = ['SourceSE', 'TargetSE']
    for requiredParam in requiredParams:
      if not self.paramValues.get(requiredParam):
        paramValue = six.moves.input("Please enter " + requiredParam + " ")
        setter = None
        setterName = "set%s" % requiredParam
        if hasattr(self, setterName) and callable(getattr(self, setterName)):
          setter = getattr(self, setterName)
        if not setter:
          return S_ERROR("Unable to invoke %s, this function hasn't been implemented." % setterName)
        ses = paramValue.replace(',', ' ').split()
        res = setter(ses)
        if not res['OK']:
          return res
    return S_OK()

  def __checkSEs(self, seList):
    res = gConfig.getSections('/Resources/StorageElements')
    if not res['OK']:
      return self._errorReport(res, 'Failed to get possible StorageElements')
    missing = set(seList) - set(res['Value'])
    if missing:
      for se in missing:
        gLogger.error("StorageElement %s is not known" % se)
      return S_ERROR("%d StorageElements not known" % len(missing))
    return S_OK()

  def __promptForParameter(self, parameter, choices=[], default='', insert=True):
    res = promptUser("Please enter %s" % parameter, choices=choices, default=default)
    if not res['OK']:
      return self._errorReport(res)
    gLogger.notice("%s will be set to '%s'" % (parameter, res['Value']))
    paramValue = res['Value']
    if insert:
      setter = None
      setterName = "set%s" % parameter
      if hasattr(self, setterName) and callable(getattr(self, setterName)):
        setter = getattr(self, setterName)
      if not setter:
        return S_ERROR("Unable to invoke %s, it isn't a member function of Transformation!")
      res = setter(paramValue)
      if not res['OK']:
        return res
    return S_OK(paramValue)
