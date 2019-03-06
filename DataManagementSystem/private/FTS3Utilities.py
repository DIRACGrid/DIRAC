""" Some utilities for FTS3...
"""

import json
import datetime
import random
import threading

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.Logger import gLogger
from DIRAC.Core.Utilities.Decorators import deprecated
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus


def _checkSourceReplicas(ftsFiles):
  """ Check the active replicas
      :params ftsFiles: list of FT3Files

      :returns: Successful/Failed {lfn : { SE1 : PFN1, SE2 : PFN2 } , ... }
  """

  lfns = list(set([f.lfn for f in ftsFiles]))
  res = DataManager().getActiveReplicas(lfns)

  return res


@deprecated("Not in use in the code, selectUniqueRandomSource prefered")
def selectUniqueSourceforTransfers(multipleSourceTransfers):
  """
      When we have several possible source for a given SE, choose one.
      In this particular case, we always choose the one that has the biggest
      amount of replicas,

      :param multipleSourceTransfers : { sourceSE : [FTSFiles] }


      :return { source SE : [ FTSFiles] } where each LFN appears only once
  """
  # the more an SE has files, the more likely it is that it is a big good old T1 site.
  # So we start packing with these SEs
  orderedSources = sorted(multipleSourceTransfers,
                          key=lambda srcSE: len(multipleSourceTransfers[srcSE]),
                          reverse=True)

  transfersBySource = {}
  usedLFNs = set()

  for sourceSE in orderedSources:
    transferList = []
    for ftsFile in multipleSourceTransfers[sourceSE]:
      if ftsFile.lfn not in usedLFNs:
        transferList.append(ftsFile)
        usedLFNs.add(ftsFile.lfn)

    if transferList:
      transfersBySource[sourceSE] = transferList

  return S_OK(transfersBySource)


@deprecated("Not in use in the code, selectUniqueRandomSource prefered")
def generatePossibleTransfersBySources(ftsFiles, allowedSources=None):
  """
      For a list of FTS3files object, group the transfer possible sources
      CAUTION ! a given LFN can be in multiple source
                You still have to choose your source !

      :param allowedSources : list of allowed sources
      :param ftsFiles : list of FTS3File object
      :return  S_OK({ sourceSE: [ FTS3Files] })

  """

  _log = gLogger.getSubLogger("generatePossibleTransfersBySources", True)

  # destGroup will contain for each target SE a dict { possible source : transfer metadata }
  groupBySource = {}

  # For all files, check which possible sources they have
  res = _checkSourceReplicas(ftsFiles)
  if not res['OK']:
    return res

  filteredReplicas = res['Value']

  for ftsFile in ftsFiles:

    if ftsFile.lfn in filteredReplicas['Failed']:
      _log.error("Failed to get active replicas", "%s,%s" %
                 (ftsFile.lfn, filteredReplicas['Failed'][ftsFile.lfn]))
      continue

    replicaDict = filteredReplicas['Successful'][ftsFile.lfn]

    for se in replicaDict:
      # if we are imposed a source, respect it
      if allowedSources and se not in allowedSources:
        continue

      groupBySource.setdefault(se, []).append(ftsFile)

  return S_OK(groupBySource)


def selectUniqueRandomSource(ftsFiles, allowedSources=None):
  """
      For a list of FTS3files object, select a random source, and group the files by source.

      :param allowedSources: list of allowed sources
      :param ftsFiles: list of FTS3File object

      :return:  S_OK({ sourceSE: [ FTS3Files] })

  """

  _log = gLogger.getSubLogger("selectUniqueRandomSource")

  allowedSourcesSet = set(allowedSources) if allowedSources else set()

  # destGroup will contain for each target SE a dict { source : [list of FTS3Files] }
  groupBySource = {}

  # For all files, check which possible sources they have
  res = _checkSourceReplicas(ftsFiles)
  if not res['OK']:
    return res

  filteredReplicas = res['Value']

  for ftsFile in ftsFiles:

    if ftsFile.lfn in filteredReplicas['Failed']:
      _log.error("Failed to get active replicas", "%s,%s" %
                 (ftsFile.lfn, filteredReplicas['Failed'][ftsFile.lfn]))
      continue

    replicaDict = filteredReplicas['Successful'][ftsFile.lfn]

    # Only consider the allowed sources

    # If we have a restriction, apply it, otherwise take all the replicas
    allowedReplicaSource = (set(replicaDict) & allowedSourcesSet) if allowedSourcesSet else replicaDict

    # pick a random source

    randSource = random.choice(list(allowedReplicaSource))  # one has to convert to list

    groupBySource.setdefault(randSource, []).append(ftsFile)

  return S_OK(groupBySource)

def groupFilesByTarget(ftsFiles):
  """
        For a list of FTS3files object, group the Files by target

        :param ftsFiles : list of FTS3File object
        :return: {targetSE : [ ftsFiles] } }

    """

  # destGroup will contain for each target SE a dict { possible source : transfer metadata }
  destGroup = {}

  for ftsFile in ftsFiles:
    destGroup.setdefault(ftsFile.targetSE, []).append(ftsFile)

  return S_OK(destGroup)


class FTS3Serializable(object):
  """ This is the base class for all the FTS3 objects that
      needs to be serialized, so FTS3Operation, FTS3File
      and FTS3Job

      The inheriting classes just have to define a class
      attribute called _attrToSerialize, which is a list of
      strings, which correspond to the name of the attribute
      they want to serialize
  """
  _datetimeFormat = '%Y-%m-%d %H:%M:%S'

  # MUST BE OVERWRITTEN IN THE CHILD CLASS
  _attrToSerialize = []

  def toJSON(self, forPrint=False):
    """ Returns the JSON formated string

        :param forPrint: if set to True, we don't include
               the 'magic' arguments used for rebuilding the
               object
    """

    jsonStr = json.dumps(self, cls=FTS3JSONEncoder, forPrint=forPrint)
    return jsonStr

  def __str__(self):
    import pprint
    js = json.loads(self.toJSON(forPrint=True))
    return pprint.pformat(js)

  def _getJSONData(self, forPrint=False):
    """ Returns the data that have to be serialized by JSON

        :param forPrint: if set to True, we don't include
               the 'magic' arguments used for rebuilding the
               object

        :return dictionary to be transformed into json
    """
    jsonData = {}
    datetimeAttributes = []
    for attrName in self._attrToSerialize:
      # IDs might not be set since it is managed by SQLAlchemy
      if not hasattr(self, attrName):
        continue

      value = getattr(self, attrName)
      if isinstance(value, datetime.datetime):
        # We convert date time to a string
        jsonData[attrName] = value.strftime(self._datetimeFormat)
        datetimeAttributes.append(attrName)
      else:
        jsonData[attrName] = value

    if not forPrint:
      jsonData['__type__'] = self.__class__.__name__
      jsonData['__module__'] = self.__module__
      jsonData['__datetime__'] = datetimeAttributes

    return jsonData


class FTS3JSONEncoder(json.JSONEncoder):
  """ This class is an encoder for the FTS3 objects
  """

  def __init__(self, *args, **kwargs):
    if 'forPrint' in kwargs:
      self._forPrint = kwargs.pop('forPrint')
    else:
      self._forPrint = False

    super(FTS3JSONEncoder, self).__init__(*args, **kwargs)

  def default(self, obj):  # pylint: disable=method-hidden

    if hasattr(obj, '_getJSONData'):
      return obj._getJSONData(forPrint=self._forPrint)
    else:
      return json.JSONEncoder.default(self, obj)


class FTS3JSONDecoder(json.JSONDecoder):
  """ This class is an decoder for the FTS3 objects
  """

  def __init__(self, *args, **kargs):
    json.JSONDecoder.__init__(self, object_hook=self.dict_to_object,
                              *args, **kargs)

  def dict_to_object(self, dataDict):
    """ Convert the dictionary into an object """
    import importlib
    # If it is not an FTS3 object, just return the structure as is
    if not ('__type__' in dataDict and '__module__' in dataDict):
      return dataDict

    # Get the class and module
    className = dataDict.pop('__type__')
    modName = dataDict.pop('__module__')
    datetimeAttributes = dataDict.pop('__datetime__', [])
    datetimeSet = set(datetimeAttributes)
    try:
      # Load the module
      mod = importlib.import_module(modName)
      # import the class
      cl = getattr(mod, className)
      # Instantiate the object
      obj = cl()

      # Set each attribute
      for attrName, attrValue in dataDict.iteritems():
        # If the value is None, do not set it
        # This is needed to play along well with SQLalchemy

        if attrValue is None:
          continue
        if attrName in datetimeSet:
          attrValue = datetime.datetime.strptime(attrValue, FTS3Serializable._datetimeFormat)
        setattr(obj, attrName, attrValue)

      return obj

    except Exception as e:
      gLogger.error('exception in FTS3JSONDecoder %s for type %s' % (e, className))
      dataDict['__type__'] = className
      dataDict['__module__'] = modName
      dataDict['__datetime__'] = datetimeAttributes
      return dataDict


threadLocal = threading.local()


class FTS3ServerPolicy(object):
  """
  This class manages the policy for choosing a server
  """

  def __init__(self, serverDict, serverPolicy="Random"):
    """
        Call the init of the parent, and initialize the list of FTS3 servers
    """

    self.log = gLogger.getSubLogger("FTS3ServerPolicy")

    self._serverDict = serverDict
    self._serverList = serverDict.keys()
    self._maxAttempts = len(self._serverList)
    self._nextServerID = 0
    self._resourceStatus = ResourceStatus()

    methName = "_%sServerPolicy" % serverPolicy.lower()
    if not hasattr(self, methName):
      self.log.error('Unknown server policy %s. Using Random instead' % serverPolicy)
      methName = "_randomServerPolicy"

    self._policyMethod = getattr(self, methName)

  def _failoverServerPolicy(self, _attempt):
    """
       Returns always the server at a given position (normally the first one)

       :param attempt: position of the server in the list
    """
    if _attempt >= len(self._serverList):
      raise Exception(
          "FTS3ServerPolicy.__failoverServerPolicy: attempt to reach non existing server index")
    return self._serverList[_attempt]

  def _sequenceServerPolicy(self, _attempt):
    """
       Every time the this policy is called, return the next server on the list
    """

    fts3server = self._serverList[self._nextServerID]
    self._nextServerID = (self._nextServerID + 1) % len(self._serverList)
    return fts3server

  def _randomServerPolicy(self, _attempt):
    """
      return a server from shuffledServerList
    """

    if getattr(threadLocal, 'shuffledServerList', None) is None:
      threadLocal.shuffledServerList = self._serverList[:]
      random.shuffle(threadLocal.shuffledServerList)

    fts3Server = threadLocal.shuffledServerList[_attempt]

    if _attempt == self._maxAttempts - 1:
      random.shuffle(threadLocal.shuffledServerList)

    return fts3Server

  def _getFTSServerStatus(self, ftsServer):
    """ Fetch the status of the FTS server from RSS """

    res = self._resourceStatus.getElementStatus(ftsServer, 'FTS')
    if not res['OK']:
      return res

    result = res['Value']
    if ftsServer not in result:
      return S_ERROR("No FTS Server %s known to RSS" % ftsServer)

    if result[ftsServer]['all'] == 'Active':
      return S_OK(True)

    return S_OK(False)

  def chooseFTS3Server(self):
    """
      Choose the appropriate FTS3 server depending on the policy
    """

    fts3Server = None
    attempt = 0

    while not fts3Server and attempt < self._maxAttempts:

      fts3Server = self._policyMethod(attempt)
      res = self._getFTSServerStatus(fts3Server)

      if not res['OK']:
        self.log.warn("Error getting the RSS status for %s: %s" % (fts3Server, res))
        fts3Server = None
        attempt += 1
        continue

      ftsServerStatus = res['Value']

      if not ftsServerStatus:
        self.log.warn('FTS server %s is not in good shape. Choose another one' % fts3Server)
        fts3Server = None
        attempt += 1

    if fts3Server:
      return S_OK(self._serverDict[fts3Server])

    return S_ERROR("Could not find an FTS3 server (max attempt reached)")
