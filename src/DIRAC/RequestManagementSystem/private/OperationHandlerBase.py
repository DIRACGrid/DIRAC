""" :mod: OperationHandlerBase

    ==========================

    .. module: OperationHandlerBase

    :synopsis: request operation handler base class

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    RMS Operation handler base class.

    This should be a functor getting Operation as ctor argument and calling it (executing __call__)
    should return S_OK/S_ERROR.

    Helper functions and tools:

    * self.rssClient() -- returns RSSClient
    * self.getProxyForLFN( LFN ) -- sets X509_USER_PROXY environment variable to LFN owner proxy
    * self.rssSEStatus( SE, status ) returns S_OK(True/False) depending of RSS :status:

    Properties:

    * self.shifter -- list of shifters matching request owner (could be empty!!!)
    * each CS option stored under CS path "RequestExecutingAgent/OperationHandlers/Foo" is exported as read-only property too
    * self.initialize() -- overwrite it to perform additional initialization
    * self.log -- own sub logger
    * self.request, self.operation -- reference to Operation and Request itself

    In all inherited class one should overwrite __call__ and initialize, when appropriate.

"""

import os

from DIRAC import gLogger, gConfig, S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupsWithVOMSAttribute
from DIRAC.Core.Utilities import Network, TimeUtilities
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog


class DynamicProps(type):
    """

    metaclass allowing to create properties on the fly
    """

    def __new__(cls, name, bases, classdict):
        """
        new operator
        """

        def makeProperty(self, name, value, readOnly=False):
            """
            Add property :name: to class

            This also creates a private :_name: attribute
            If you want to make read only property, set :readOnly: flag to True
            :warn: could raise AttributeError if :name: of :_name: is already
            defined as an attribute
            """
            if hasattr(self, "_" + name) or hasattr(self, name):
                raise AttributeError(f"_{name} or {name} is already defined as a member")

            def fget(self):
                return self._getProperty(name)

            fset = None if readOnly else lambda self, value: self._setProperty(name, value)
            setattr(self, "_" + name, value)
            setattr(self.__class__, name, property(fget=fget, fset=fset))

        def _setProperty(self, name, value):
            """
            property setter
            """
            setattr(self, "_" + name, value)

        def _getProperty(self, name):
            """
            property getter
            """
            return getattr(self, "_" + name)

        classdict["makeProperty"] = makeProperty
        classdict["_setProperty"] = _setProperty
        classdict["_getProperty"] = _getProperty
        return super().__new__(cls, name, bases, classdict)


class OperationHandlerBase(metaclass=DynamicProps):
    """
    .. class:: OperationHandlerBase

    request operation handler base class
    """

    __rssClient = None
    __shifterList = []

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param Operation operation: Operation instance
        :param str csPath: config path in CS for this operation
        """
        # placeholders for operation and request
        self.operation = None
        self.request = None

        self.rmsMonitoring = False
        if "Monitoring" in Operations().getMonitoringBackends(monitoringType="RMSMonitoring"):
            self.rmsMonitoring = True
        self.dm = DataManager()
        self.fc = FileCatalog()

        self.csPath = csPath if csPath else ""
        # all options are r/o properties now
        csOptionsDict = gConfig.getOptionsDict(self.csPath)
        csOptionsDict = csOptionsDict.get("Value", {})

        for option, value in csOptionsDict.items():
            # hack to set proper types
            try:
                value = eval(value)
            except NameError:
                pass
            self.makeProperty(option, value, True)  # pylint: disable=no-member

        # pre setup logger
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        # set log level
        logLevel = getattr(self, "LogLevel") if hasattr(self, "LogLevel") else "INFO"
        self.log.setLevel(logLevel)

        # list properties
        for option in csOptionsDict:
            self.log.debug(f"{option} = {getattr(self, option)}")

        # setup operation
        if operation:
            self.setOperation(operation)
        # initialize at least
        if hasattr(self, "initialize") and callable(getattr(self, "initialize")):
            getattr(self, "initialize")()

    def setOperation(self, operation):
        """operation and request setter

        :param ~DIRAC.RequestManagementSystem.Client.Operation.Operation operation: operation instance
        :raises TypeError: if `operation` is not an instance of :class:`~DIRAC.RequestManagementSystem.Client.Operation.Operation`

        """
        if not isinstance(operation, Operation):
            raise TypeError("expecting Operation instance")
        self.operation = operation
        self.request = operation._parent
        self.log = gLogger.getSubLogger(
            f"pid_{os.getpid()}/{self.request.RequestName}/{self.request.Order}/{self.operation.Type}"
        )

    #   @classmethod
    #   def dataLoggingClient( cls ):
    #     """ DataLoggingClient getter """
    #     if not cls.__dataLoggingClient:
    #       from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
    #       cls.__dataLoggingClient = DataLoggingClient()
    #     return cls.__dataLoggingClient

    @classmethod
    def rssClient(cls):
        """ResourceStatusClient getter"""
        if not cls.__rssClient:
            from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

            cls.__rssClient = ResourceStatus()
        return cls.__rssClient

    def getProxyForLFN(self, lfn):
        """get proxy for lfn

        :param str lfn: LFN
        :return: S_ERROR or S_OK( "/path/to/proxy/file" )
        """
        dirMeta = returnSingleResult(self.fc.getDirectoryMetadata(os.path.dirname(lfn)))
        if not dirMeta["OK"]:
            return dirMeta
        dirMeta = dirMeta["Value"]

        ownerRole = "/%s" % dirMeta["OwnerRole"] if not dirMeta["OwnerRole"].startswith("/") else dirMeta["OwnerRole"]
        ownerDN = dirMeta["OwnerDN"]

        ownerProxy = None
        for ownerGroup in getGroupsWithVOMSAttribute(ownerRole):
            vomsProxy = gProxyManager.downloadVOMSProxy(
                ownerDN, ownerGroup, limited=True, requiredVOMSAttribute=ownerRole
            )
            if not vomsProxy["OK"]:
                self.log.debug(
                    "getProxyForLFN: failed to get VOMS proxy for %s role=%s: %s"
                    % (ownerDN, ownerRole, vomsProxy["Message"])
                )
                continue
            ownerProxy = vomsProxy["Value"]
            self.log.debug(f"getProxyForLFN: got proxy for {ownerDN}@{ownerGroup} [{ownerRole}]")
            break

        if not ownerProxy:
            return S_ERROR("Unable to get owner proxy")

        dumpToFile = ownerProxy.dumpAllToFile()
        if not dumpToFile["OK"]:
            self.log.debug("getProxyForLFN: error dumping proxy to file: %s" % dumpToFile["Message"])
        else:
            os.environ["X509_USER_PROXY"] = dumpToFile["Value"]
        return dumpToFile

    def getWaitingFilesList(self):
        """prepare waiting files list, update Attempt, filter out MaxAttempt"""
        if not self.operation:
            self.log.warning("getWaitingFilesList: operation not set, returning empty list")
            return []
        waitingFiles = [opFile for opFile in self.operation if opFile.Status == "Waiting"]
        for opFile in waitingFiles:
            opFile.Attempt += 1
            maxAttempts = getattr(self, "MaxAttempts") if hasattr(self, "MaxAttempts") else 1024
            if opFile.Attempt > maxAttempts:
                opFile.Status = "Failed"
                if opFile.Error is None:
                    opFile.Error = ""
                opFile.Error += " (Max attempts limit reached)"
        return [opFile for opFile in self.operation if opFile.Status == "Waiting"]

    def rssSEStatus(self, se, status, retries=2):
        """check SE :se: for status :status:

        :param str se: SE name
        :param str status: RSS status
        """
        # Allow a transient failure
        for _i in range(retries):
            rssStatus = self.rssClient().getElementStatus(se, "StorageElement", status)
            # gLogger.always( rssStatus )
            if rssStatus["OK"]:
                return S_OK(rssStatus["Value"][se][status] != "Banned")
        return S_ERROR(f"{status} status not found in RSS for SE {se}")

    @property
    def shifter(self):
        return self.__shifterList

    @shifter.setter
    def shifter(self, shifterList):
        self.__shifterList = shifterList

    def __call__(self):
        """this one should be implemented in the inherited class

        should return S_OK/S_ERROR
        """
        raise NotImplementedError("Implement me please!")

    def createRMSRecord(self, status, nbObject):
        """
        This method is used to create a record given some parameters for sending it to the ES backend.
        It is used inside DMS/Agent/RequestOperations and this method is designed particularly for file
        type of objects.

        :param status: This can be one of these i.e. Attempted, Failed, or Successful.
        :param nbObject: This is number of objects in question.

        :returns: a dictionary.
        """
        record = {
            "timestamp": int(TimeUtilities.toEpochMilliSeconds()),
            "host": Network.getFQDN(),
            "objectType": "File",
            "operationType": self.operation.Type,
            "status": status,
            "nbObject": nbObject,
            "parentID": self.operation.OperationID,
        }

        return record
