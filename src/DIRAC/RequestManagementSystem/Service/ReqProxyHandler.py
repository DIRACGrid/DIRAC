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

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN ReqProxy
  :end-before: ##END
  :dedent: 2
  :caption: ReqProxy options

"""
# #
# @file RequestProxyHandler.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2012/07/20 13:18:58
# @brief Definition of RequestProxyHandler class.

# # imports
import os
import json

from hashlib import md5

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.DISET.RequestHandler import RequestHandler, getServiceOption
from DIRAC.Core.Base.Client import Client
from DIRAC.Core.Utilities.ThreadScheduler import gThreadScheduler
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.RequestManagementSystem.Client.Request import Request


def initializeReqProxyHandler(serviceInfo):
    """init RequestProxy handler

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

    @classmethod
    def initializeHandler(cls, serviceInfoDict):
        """Initialize handler"""
        gLogger.notice("CacheDirectory: %s" % cls.cacheDir())
        cls.sweepSize = getServiceOption(serviceInfoDict, "SweepSize", 10)
        gLogger.notice(f"SweepSize: {cls.sweepSize}")
        return S_OK()

    @classmethod
    def requestManager(cls):
        """get request manager"""
        if not cls.__requestManager:
            cls.__requestManager = Client(url="RequestManagement/ReqManager")
        return cls.__requestManager

    @classmethod
    def cacheDir(cls):
        """get cache dir"""
        if not cls.__cacheDir:
            cls.__cacheDir = os.path.abspath("requestCache")
            if not os.path.exists(cls.__cacheDir):
                os.mkdir(cls.__cacheDir)
        return cls.__cacheDir

    @classmethod
    def sweeper(cls):
        """move cached request to the central request manager

        :param self: self reference
        """
        cacheDir = cls.cacheDir()

        # # cache dir empty?
        if not os.listdir(cacheDir):
            gLogger.always("sweeper: cache dir is empty, nothing to do", f"(cache dir: {cacheDir})")
            return S_OK()
        else:
            # # read <sweepSize> cache dir files, the oldest first
            cachedRequests = [
                os.path.abspath(requestFile)
                for requestFile in sorted(
                    filter(
                        os.path.isfile, [os.path.join(cacheDir, requestName) for requestName in os.listdir(cacheDir)]
                    ),
                    key=os.path.getctime,
                )
            ][: cls.sweepSize]
            # # set cached requests to the central RequestManager
            for cachedFile in cachedRequests:
                # # break if something went wrong last time
                try:
                    requestJSON = "".join(open(cachedFile).readlines())
                    cachedRequest = json.loads(requestJSON)
                    cachedName = cachedRequest.get("RequestName", "***UNKNOWN***")
                    putRequest = cls.requestManager().putRequest(requestJSON)
                    if not putRequest["OK"]:
                        gLogger.error(
                            "sweeper: unable to set request", f"{cachedName} @ ReqManager: {putRequest['Message']}"
                        )
                        continue
                    gLogger.info(f"sweeper: successfully put request", f"'{cachedName}' @ ReqManager")
                    os.unlink(cachedFile)
                except Exception as error:
                    gLogger.exception("sweeper: hit by exception", lException=error)

            return S_OK()

    def __saveRequest(self, requestName, requestJSON):
        """save request string to the working dir cache

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
            err = f"unable to dump {requestName} to cache file: {error}"
            gLogger.exception(err)
            return S_ERROR(err)

    types_getStatus = []

    def export_getStatus(self):
        """get number of requests in cache"""
        try:
            cachedRequests = len(os.listdir(self.cacheDir()))
        except OSError as error:
            err = f"getStatus: unable to list cache dir contents: {error}"
            gLogger.exception(err)
            return S_ERROR(err)
        return S_OK(cachedRequests)

    types_putRequest = [str]

    def export_putRequest(self, requestJSON):
        """forward request from local RequestDB to central RequestManager

        :param self: self reference
        :param str requestType: request type
        """

        requestDict = json.loads(requestJSON)
        requestName = requestDict.get("RequestID", requestDict.get("RequestName", "***UNKNOWN***"))
        gLogger.info("putRequest: got request", f"{requestName}")

        # We only need the object to check the authorization
        request = Request(requestDict)
        # Check whether the credentials in the Requests are correct and allowed to be set
        isAuthorized = RequestValidator.setAndCheckRequestOwner(request, self.getRemoteCredentials())

        if not isAuthorized:
            return S_ERROR(DErrno.ENOAUTH, "Credentials in the requests are not allowed")

        forwardable = self.__forwardable(requestDict)
        if not forwardable["OK"]:
            gLogger.warn("putRequest: ", f"{forwardable['Message']}")

        setRequest = self.requestManager().putRequest(requestJSON)
        if not setRequest["OK"]:
            gLogger.error(
                "setRequest: unable to set request", f"'{requestName}' @ RequestManager: {setRequest['Message']}"
            )
            # # put request to the request file cache
            save = self.__saveRequest(requestName, requestJSON)
            if not save["OK"]:
                gLogger.error("setRequest: unable to save request to the cache", save["Message"])
                return save
            gLogger.info("setRequest: ", f"{requestName} is saved to {save['Value']} file")
            return S_OK({"set": False, "saved": True})

        gLogger.info("setRequest: ", f"request '{requestName}' has been set to the ReqManager")
        return S_OK({"set": True, "saved": False})

    @staticmethod
    def __forwardable(requestDict):
        """check if request if forwardable

        The sub-request of type transfer:putAndRegister, removal:physicalRemoval and removal:reTransfer are
        definitely not, they should be executed locally, as they are using local fs.

        :param str requestJSON: serialized request
        """
        operations = requestDict.get("Operations", [])
        for operationDict in operations:
            if operationDict.get("Type", "") in ("PutAndRegister", "PhysicalRemoval", "ReTransfer"):
                return S_ERROR(
                    DErrno.ERMSUKN, "found operation '%s' that cannot be forwarded" % operationDict.get("Type", "")
                )
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
            return S_ERROR(DErrno.ERMSUKN, f"Error listing {cacheDir}: {repr(e)}")

    types_showCachedRequest = [str]

    def export_showCachedRequest(self, filename):
        """Show the request cached in the given file"""
        fullPath = None
        try:
            fullPath = os.path.join(self.cacheDir(), filename)
            with open(fullPath) as cacheFile:
                requestJSON = "".join(cacheFile.readlines())
                return S_OK(requestJSON)
        except Exception as e:
            return S_ERROR(DErrno.ERMSUKN, f"Error showing cached request {fullPath}: {repr(e)}")
