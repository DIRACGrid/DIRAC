""" Some utilities for FTS3...
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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


def selectUniqueRandomSource(ftsFiles, allowedSources=None):
  """
      For a list of FTS3files object, select a random source, and group the files by source.

      We also return the FTS3Files for which we had problems getting replicas

      :param allowedSources: list of allowed sources
      :param ftsFiles: list of FTS3File object

      :return:  S_OK(({ sourceSE: [ FTS3Files] }, {FTS3File: errors}))

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

  # LFNs for which we failed to get replicas
  failedFiles = {}

  for ftsFile in ftsFiles:

    # If we failed to get the replicas, add the FTS3File to
    # the dictionnary
    if ftsFile.lfn in filteredReplicas['Failed']:
      errMsg = filteredReplicas['Failed'][ftsFile.lfn]
      failedFiles[ftsFile] = errMsg
      _log.debug("Failed to get active replicas", "%s,%s" %
                 (ftsFile.lfn, errMsg))
      continue

    replicaDict = filteredReplicas['Successful'][ftsFile.lfn]

    # Only consider the allowed sources

    # If we have a restriction, apply it, otherwise take all the replicas
    allowedReplicaSource = (set(replicaDict) & allowedSourcesSet) if allowedSourcesSet else replicaDict

    # pick a random source

    randSource = random.choice(list(allowedReplicaSource))  # one has to convert to list

    groupBySource.setdefault(randSource, []).append(ftsFile)

  return S_OK((groupBySource, failedFiles))


def groupFilesByTarget(ftsFiles):
  """
        For a list of FTS3files object, group the Files by target

        :param ftsFiles: list of FTS3File object
        :return: {targetSE : [ ftsFiles] } }

    """

  # destGroup will contain for each target SE a dict { possible source : transfer metadata }
  destGroup = {}

  for ftsFile in ftsFiles:
    destGroup.setdefault(ftsFile.targetSE, []).append(ftsFile)

  return S_OK(destGroup)


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
    self._serverList = list(serverDict)
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
