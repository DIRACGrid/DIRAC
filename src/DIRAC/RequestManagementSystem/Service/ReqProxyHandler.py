########################################################################
# File: ReqProxyHandler.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/06/04 13:18:41
########################################################################

"""
:mod: RequestProxyHandler

.. module: ReqtProxyHandler
  :synopsis: ReqProxy service

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

Careful with that axe, Eugene! Some 'transfer' requests are using local fs
and they never should be forwarded to the central RequestManager.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

# #
# @file RequestProxyHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/20 13:18:58
# @brief Definition of RequestProxyHandler class.

# # imports
import os
try:
  from hashlib import md5
except ImportError:
  from md5 import md5

import json
import six
# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.RequestManagementSystem.Client.Request import Request


def initializeReqProxyHandler(serviceInfo):
  """ init RequestProxy handler

  :param serviceInfo: whatever
  """
  gLogger.info("Initalizing ReqProxyHandler")
  gThreadScheduler.addPeriodicTask(120, ReqProxyHandler.sweeper)
  return S_OK()

########################################################################


class ReqProxyHandler(RequestHandler):
  """
  .. class:: ReqProxyHandler

  :param RPCCLient requestManager: a RPCClient to RequestManager
  :param str cacheDir: os.path.join( workDir, "requestCache" )
  """
  __requestManager = None
  __cacheDir = None

  def initialize(self):
    """ service initialization

    :param self: self reference
    """
    gLogger.notice("CacheDirectory: %s" % self.cacheDir())
    gMonitor.registerActivity("reqSwept", "Request successfully swept",
                              "ReqProxy", "Requests/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("reqFailed", "Request forward failed",
                              "ReqProxy", "Requests/min", gMonitor.OP_SUM)
    gMonitor.registerActivity("reqReceived", "Request received",
                              "ReqProxy", "Requests/min", gMonitor.OP_SUM)
    return S_OK()

  @classmethod
  def requestManager(cls):
    """ get request manager """
    if not cls.__requestManager:
      cls.__requestManager = RPCClient("RequestManagement/ReqManager")
    return cls.__requestManager

  @classmethod
  def cacheDir(cls):
    """ get cache dir """
    if not cls.__cacheDir:
      cls.__cacheDir = os.path.abspath("requestCache")
      if not os.path.exists(cls.__cacheDir):
        os.mkdir(cls.__cacheDir)
    return cls.__cacheDir

  @classmethod
  def sweeper(cls):
    """ move cached request to the central request manager

    :param self: self reference
    """
    cacheDir = cls.cacheDir()

    # # cache dir empty?
    if not os.listdir(cacheDir):
      gLogger.always("sweeper: CacheDir %s is empty, nothing to do" % cacheDir)
      return S_OK()
    else:
      # # read 10 cache dir files, the oldest first
      cachedRequests = [os.path.abspath(requestFile) for requestFile in
                        sorted(filter(os.path.isfile,
                                      [os.path.join(cacheDir, requestName)
                                       for requestName in os.listdir(cacheDir)]),
                               key=os.path.getctime)][:10]
      # # set cached requests to the central RequestManager
      for cachedFile in cachedRequests:
        # # break if something went wrong last time
        try:
          requestJSON = "".join(open(cachedFile, "r").readlines())
          cachedRequest = json.loads(requestJSON)
          cachedName = cachedRequest.get("RequestName", "***UNKNOWN***")
          putRequest = cls.requestManager().putRequest(requestJSON)
          if not putRequest["OK"]:
            gLogger.error(
                "sweeper: unable to set request %s @ ReqManager: %s" %
                (cachedName, putRequest["Message"]))
            gMonitor.addMark("reqFailed", 1)

            continue
          gLogger.info("sweeper: successfully put request '%s' @ ReqManager" % cachedName)
          gMonitor.addMark("reqSwept", 1)
          os.unlink(cachedFile)
        except Exception as error:
          gMonitor.addMark("reqFailed", 1)
          gLogger.exception("sweeper: hit by exception", lException=error)

      return S_OK()

  def __saveRequest(self, requestName, requestJSON):
    """ save request string to the working dir cache

    :param self: self reference
    :param str requestName: request name
    :param str requestJSON:  request serialized to JSON format
    """
    try:
      requestFile = os.path.join(self.cacheDir(), md5(requestJSON.encode()).hexdigest())
      with open(requestFile, "w+") as request:
        request.write(requestJSON)
      return S_OK(requestFile)
    except OSError as error:
      err = "unable to dump %s to cache file: %s" % (requestName, str(error))
      gLogger.exception(err)
      return S_ERROR(err)

  types_getStatus = []

  def export_getStatus(self):
    """ get number of requests in cache """
    try:
      cachedRequests = len(os.listdir(self.cacheDir()))
    except OSError as error:
      err = "getStatus: unable to list cache dir contents: %s" % str(error)
      gLogger.exception(err)
      return S_ERROR(err)
    return S_OK(cachedRequests)

  types_putRequest = [six.string_types]

  def export_putRequest(self, requestJSON):
    """ forward request from local RequestDB to central RequestManager

    :param self: self reference
    :param str requestType: request type
    """

    gMonitor.addMark('reqReceived', 1)

    requestDict = json.loads(requestJSON)
    requestName = requestDict.get("RequestID", requestDict.get('RequestName', "***UNKNOWN***"))
    gLogger.info("putRequest: got request '%s'" % requestName)

    # We only need the object to check the authorization
    request = Request(requestDict)
    # Check whether the credentials in the Requests are correct and allowed to be set
    isAuthorized = RequestValidator.setAndCheckRequestOwner(request, self.getRemoteCredentials())

    if not isAuthorized:
      return S_ERROR(DErrno.ENOAUTH, "Credentials in the requests are not allowed")

    forwardable = self.__forwardable(requestDict)
    if not forwardable["OK"]:
      gLogger.warn("putRequest: %s" % forwardable["Message"])

    setRequest = self.requestManager().putRequest(requestJSON)
    if not setRequest["OK"]:
      gLogger.error(
          "setReqeuest: unable to set request '%s' @ RequestManager: %s" %
          (requestName, setRequest["Message"]))
      # # put request to the request file cache
      save = self.__saveRequest(requestName, requestJSON)
      if not save["OK"]:
        gLogger.error("setRequest: unable to save request to the cache: %s" % save["Message"])
        return save
      gLogger.info("setRequest: %s is saved to %s file" % (requestName, save["Value"]))
      return S_OK({"set": False, "saved": True})

    gLogger.info("setRequest: request '%s' has been set to the ReqManager" % (requestName))
    return S_OK({"set": True, "saved": False})

  @staticmethod
  def __forwardable(requestDict):
    """ check if request if forwardable

    The sub-request of type transfer:putAndRegister, removal:physicalRemoval and removal:reTransfer are
    definitely not, they should be executed locally, as they are using local fs.

    :param str requestJSON: serialized request
    """
    operations = requestDict.get("Operations", [])
    for operationDict in operations:
      if operationDict.get("Type", "") in ("PutAndRegister", "PhysicalRemoval", "ReTransfer"):
        return S_ERROR(
            DErrno.ERMSUKN,
            "found operation '%s' that cannot be forwarded" %
            operationDict.get(
                "Type",
                ""))
    return S_OK()

  types_listCacheDir = []

  def export_listCacheDir(self):
    """List the content of the Cache directory

        :returns: list of file
    """
    cacheDir = self.cacheDir()
    try:
      dirContent = os.listdir(cacheDir)
      return S_OK(dirContent)
    except OSError as e:
      return S_ERROR(DErrno.ERMSUKN, "Error listing %s: %s" % (cacheDir, repr(e)))

  types_showCachedRequest = [six.string_types]

  def export_showCachedRequest(self, filename):
    """ Show the request cached in the given file """
    fullPath = None
    try:
      fullPath = os.path.join(self.cacheDir(), filename)
      with open(fullPath, 'r') as cacheFile:
        requestJSON = "".join(cacheFile.readlines())
        return S_OK(requestJSON)
    except Exception as e:
      return S_ERROR(DErrno.ERMSUKN, "Error showing cached request %s: %s" % (fullPath, repr(e)))
