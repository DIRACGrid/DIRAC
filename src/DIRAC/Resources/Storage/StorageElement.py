""" This is the StorageElement module. It implements The StorageElementItem as well as the caching system
"""

# # custom duty


from copy import deepcopy
import datetime
import errno
import os
import re
import sys
import threading
import time


from functools import reduce

# # from DIRAC
from DIRAC import gLogger, gConfig, siteName
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.File import convertSizeUnits
from DIRAC.Core.Utilities.List import getIndexInList
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR, returnSingleResult, convertToReturnValue
from DIRAC.Core.Utilities.TimeUtilities import toEpochMilliSeconds
from DIRAC.Resources.Storage.StorageFactory import StorageFactory
from DIRAC.Core.Utilities.Pfn import pfnparse
from DIRAC.Core.Utilities.SiteSEMapping import getSEsForSite
from DIRAC.Core.Utilities.Network import getFQDN
from DIRAC.Core.Security.Locations import getProxyLocation
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Resources.Storage.Utilities import checkArgumentFormat
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.MonitoringSystem.Client.DataOperationSender import DataOperationSender
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader

DEFAULT_OCCUPANCY_FILE = "occupancy.json"

sLog = gLogger.getSubLogger(__name__)


class StorageElementCache:
    """
    The StorageElementCache keeps StorageElementItem instances in a cache to save on initialization cost.
    It keeps one instance per tuple (thread ID, seName, protocolSections, VO, proxy )
    """

    def __init__(self):
        self.seCache = DictCache()

    def __call__(self, name, protocolSections=None, vo=None, hideExceptions=False):
        self.seCache.purgeExpired(expiredInSeconds=60)
        tId = threading.current_thread().ident

        if not vo:
            result = getVOfromProxyGroup()
            if not result["OK"]:
                return
            vo = result["Value"]

        # Because the gfal2 context caches the proxy location,
        # we also use the proxy location as a key.
        # In practice, there should almost always be one, except for the REA
        # If we see its memory consumption exploding, this might be a place to look

        # ensure protocolSections is hashable! (tuple)
        if isinstance(protocolSections, list):
            protocolSections = tuple(protocolSections)

        argTuple = (tId, name, protocolSections, vo, getProxyLocation())
        seObj = self.seCache.get(argTuple)

        if not seObj:
            seObj = StorageElementItem(name, protocolSections=protocolSections, vo=vo, hideExceptions=hideExceptions)
            # Add the StorageElement to the cache for 1/2 hour
            self.seCache.add(argTuple, 1800, seObj)

        return seObj


class StorageElementItem:
    """
    .. class:: StorageElementItem

    This class implements all the necessary logic to interact with the GRID storage Elements. Actual interaction with storages are delegated to ``StoragePlugins``.

    The role of the StorageElementItem is to:

    * Provide a single interface to all the grid storages
    * Ensure the status of the Storage in RSS
    * Select multiple protocols for various operations
    * Support multiple protocols as failover
    * Negociate protocols with other StorageElement for Third Party Copy



    :ivar str name: Resolved name of the StorageElement
    :ivar dict options: dictionary containing the general options defined in the CS
    :ivar dict storages: dict of the stub objects created by StorageFactory for the protocols found in the CS. Index by the protocol section name.
    :ivar list localProtocolSections: list of the local protocols that were created by StorageFactory
    :ivar list remoteProtocolSections: list of the remote protocols that were created by StorageFactory
    :ivar list protocolOptions: list of dictionaries containing the options found in the CS. (should be removed)



    dynamic method::

      retransferOnlineFile( lfn )
      exists( lfn )
      isFile( lfn )
      getFile( lfn, localPath = False )
      putFile( lfnLocal, sourceSize = 0 ) : {lfn:local}
      replicateFile( lfn, sourceSize = 0 )
      getFileMetadata( lfn )
      getFileSize( lfn )
      removeFile( lfn )
      prestageFile( lfn, lifetime = 86400 )
      prestageFileStatus( lfn )
      releaseFile( lfn )
      isDirectory( lfn )
      getDirectoryMetadata( lfn )
      getDirectorySize( lfn )
      listDirectory( lfn )
      removeDirectory( lfn, recursive = False )
      createDirectory( lfn )
      putDirectory( lfn )
      getDirectory( lfn, localPath = False )


    """

    __deprecatedArguments = ["singleFile", "singleDirectory"]  # Arguments that are now useless

    # Some methods have a different name in the StorageElement and the plugins...
    # We could avoid this static list in the __getattr__ by checking the storage plugin and so on
    # but fine... let's not be too smart, otherwise it becomes unreadable :-)
    __equivalentMethodNames = {
        "exists": "exists",
        "isFile": "isFile",
        "getFile": "getFile",
        "putFile": "putFile",
        "replicateFile": "putFile",
        "getFileMetadata": "getFileMetadata",
        "getFileSize": "getFileSize",
        "removeFile": "removeFile",
        "prestageFile": "prestageFile",
        "prestageFileStatus": "prestageFileStatus",
        "releaseFile": "releaseFile",
        "isDirectory": "isDirectory",
        "getDirectoryMetadata": "getDirectoryMetadata",
        "getDirectorySize": "getDirectorySize",
        "listDirectory": "listDirectory",
        "removeDirectory": "removeDirectory",
        "createDirectory": "createDirectory",
        "putDirectory": "putDirectory",
        "getDirectory": "getDirectory",
    }

    # We can set default argument in the __executeFunction which impacts all plugins
    __defaultsArguments = {
        "putFile": {"sourceSize": 0},
        "getFile": {"localPath": False},
        "prestageFile": {"lifetime": 86400},
        "removeDirectory": {"recursive": False},
        "getDirectory": {"localPath": False},
    }

    def __init__(self, name, protocolSections=None, vo=None, hideExceptions=False):
        """c'tor

        :param str name: SE name
        :param list protocolSections: requested storage protocolSections
        :param vo: vo

        """

        self.methodName = None

        if protocolSections is None:
            protocolSections = []

        if vo:
            self.vo = vo
        else:
            result = getVOfromProxyGroup()
            if not result["OK"]:
                return
            self.vo = result["Value"]
        self.opHelper = Operations(vo=self.vo)

        # These things will soon have to go as well. 'AccessProtocol.1' is all but flexible.
        proxiedProtocols = gConfig.getValue("/LocalSite/StorageElements/ProxyProtocols", "").split(",")
        self.useProxy = (
            gConfig.getValue(f"/Resources/StorageElements/{name}/AccessProtocol.1/Protocol", "UnknownProtocol")
            in proxiedProtocols
        )

        if not self.useProxy:
            self.useProxy = gConfig.getValue(f"/LocalSite/StorageElements/{name}/UseProxy", False)
        if not self.useProxy:
            self.useProxy = self.opHelper.getValue(f"/Services/StorageElements/{name}/UseProxy", False)

        self.valid = True

        res = StorageFactory(useProxy=self.useProxy, vo=self.vo).getStorages(
            name, protocolSections=protocolSections, hideExceptions=hideExceptions
        )

        if not res["OK"]:
            self.valid = False
            self.name = name
            self.errorReason = res["Message"]
        else:
            factoryDict = res["Value"]
            self.name = factoryDict["StorageName"]
            self.options = factoryDict["StorageOptions"]
            self.localProtocolSections = factoryDict["LocalProtocolSections"]
            self.remoteProtocolSections = factoryDict["RemoteProtocolSections"]
            self.storages = factoryDict["StorageObjects"]
            self.protocolOptions = factoryDict["ProtocolOptions"]
            self.turlProtocols = factoryDict["TurlProtocols"]

            for storage in self.storages.values():
                storage.setStorageElement(self)

        self.log = sLog.getSubLogger(f"SE[{self.name}]")

        if self.valid:
            self.useCatalogURL = gConfig.getValue(f"/Resources/StorageElements/{self.name}/UseCatalogURL", False)
            self.log.debug(f"useCatalogURL: {self.useCatalogURL}")

            self.__dmsHelper = DMSHelpers(vo=vo)

            # Allow SE to overwrite general operation config
            accessProto = self.options.get("AccessProtocols")
            self.localAccessProtocolList = accessProto if accessProto else self.__dmsHelper.getAccessProtocols()
            self.log.debug(f"localAccessProtocolList {self.localAccessProtocolList}")

            writeProto = self.options.get("WriteProtocols")
            self.localWriteProtocolList = writeProto if writeProto else self.__dmsHelper.getWriteProtocols()
            self.log.debug(f"localWriteProtocolList {self.localWriteProtocolList}")

            # For the staging protocols, we take in order:
            # * the locally defined staging protocols list
            # * the locally defined access protocols list
            # * the globally defined staging protocols list
            # * the globally defined access protocols list
            # This ensures local over global preference as well
            # as backward compatibility (when staging was part of read methods)

            stageProto = self.options.get("StageProtocols")
            globalStageProto = self.__dmsHelper.getStageProtocols()
            self.localStageProtocolList = (
                stageProto
                if stageProto
                else (
                    accessProto
                    if accessProto
                    else globalStageProto
                    if globalStageProto
                    else self.localAccessProtocolList
                )
            )
            self.log.debug(f"localStageProtocolList {self.localStageProtocolList}")

        self.stageMethods = ["prestageFile", "prestageFileStatus"]

        self.readMethods = ["getFile", "getDirectory"]

        self.writeMethods = [
            "retransferOnlineFile",
            "putFile",
            "replicateFile",
            "pinFile",
            "releaseFile",
            "createDirectory",
            "putDirectory",
        ]

        self.removeMethods = ["removeFile", "removeDirectory"]

        self.checkMethods = [
            "exists",
            "getDirectoryMetadata",
            "getDirectorySize",
            "getFileSize",
            "getFileMetadata",
            "listDirectory",
            "isDirectory",
            "isFile",
            "getOccupancy",
            "getWLCGTokenPath",
        ]

        self.okMethods = [
            "getLocalProtocols",
            "getProtocols",
            "getProtocolSections",
            "getRemoteProtocols",
            "storageElementName",
            "getStorageParameters",
            "getTransportURL",
            "isLocalSE",
        ]

        self.__fileCatalog = None

        self.dataOpSender = DataOperationSender()

    def dump(self):
        """Dump to the logger a summary of the StorageElement items."""
        log = self.log.getLocalSubLogger("dump")
        log.debug(f"Preparing dump for StorageElement {self.name}.")
        if not self.valid:
            log.debug("Failed to create StorageElement plugins.", self.errorReason)
            return
        i = 1
        outStr = "\n\n============ Options ============\n"
        for key in sorted(self.options):
            outStr = f"{outStr}{key.ljust(15)}: {self.options[key]}\n"

        for storage in self.storages.values():
            outStr = f"{outStr}============Protocol {i} ============\n"
            storageParameters = storage.getParameters()
            for key in sorted(storageParameters):
                outStr = f"{outStr}{key.ljust(15)}: {storageParameters[key]}\n"
            i = i + 1
        log.debug(outStr)

    #################################################################################################
    #
    # These are the basic get functions for storage configuration
    #

    def getStorageElementName(self):
        """SE name getter for backward compatibility"""
        return S_OK(self.storageElementName())

    def storageElementName(self):
        """SE name getter"""
        return self.name

    def getChecksumType(self):
        """Checksum type getter for backward compatibility"""
        return S_OK(self.checksumType())

    def checksumType(self):
        """get specific /Resources/StorageElements/<SEName>/ChecksumType option if defined, otherwise
        global /Resources/StorageElements/ChecksumType
        """
        return (
            self.options["ChecksumType"].upper()
            if "ChecksumType" in self.options
            else gConfig.getValue("/Resources/StorageElements/ChecksumType", "ADLER32").upper()
        )

    def getStatus(self):
        """
        Return Status of the SE only if the SE is valid
        It returns an S_OK/S_ERROR structure
        """
        valid = self.isValid()
        if not valid["OK"]:
            return valid
        return S_OK(self.status())

    def isSameSE(self, otherSE):
        """Compares two SE together and tries to guess if the two SEs are pointing at the same
        location from the namespace point of view.
        This is primarily aimed at avoiding to overwrite a file with itself, in particular
        where the difference is only the SRM spacetoken.

        Two SEs are considered to be the same if they have a couple (Host, Path) in common
        among their various protocols

        :param otherSE: the storage element to which we compare
        :returns: boolean. True if the two SEs are the same.
        """

        # If the two objects are the same, it is obviously the same SE
        if self == otherSE:
            return True

        # Otherwise, we build the list of (Host, Path) couples

        selfEndpoints = set()
        otherSEEndpoints = set()

        for storage in self.storages.values():
            storageParam = storage.getParameters()
            selfEndpoints.add((storageParam["Host"], storageParam["Path"]))

        for storage in otherSE.storages.values():
            storageParam = storage.getParameters()
            otherSEEndpoints.add((storageParam["Host"], storageParam["Path"]))

        # The two SEs are the same if they have at least one couple in common
        return bool(selfEndpoints & otherSEEndpoints)

    def getOccupancy(self, unit="MB", **kwargs):
        """Retrieves the space information about the storage.
        It returns the Total and Free space, and a SpaceReservation.

        The SpaceReservation is just a name of a zone of the physical storage which can have some space reserved.
        It corresponds to the ``SpaceToken`` concept of SRM.
        If the StorageElement definition has a ``SpaceReservation`` option in the CS, this is returned, unless
        it is overwritten by the storage plugin.

        It loops over the different Storage Plugins to query it.

        :params occupancyLFN: (named param) LFN where to find the space reporting json file on the storage
                              The json file should contain the Free and Total space in B.
                              If not specified, the default path will be </vo/occupancy.json>

        :params unit: (default MB)unit of the value returned. See :py:func:`~DIRAC.Core.Utilities.File.convertSizeUnits`
                      CAUTION: only the `Total` and `Free` field are converted !
                      Since the rest is whatever is returned by the plugin no conversion is performed

        :returns: S_OK with dict (keys: Total, Free, SpaceReservation)
        """
        log = self.log.getSubLogger("getOccupancy")

        res = self.isValid(operation="getOccupancy")
        if not res["OK"]:
            return res
        else:
            if not self.valid:
                return S_ERROR(self.errorReason)

        if "occupancyLFN" not in kwargs:
            occupancyLFN = self.options.get("OccupancyLFN")
            if not occupancyLFN:
                occupancyLFN = os.path.join("/", self.vo, DEFAULT_OCCUPANCY_FILE)

            kwargs["occupancyLFN"] = occupancyLFN

        # Call occupancy plugin if requested
        occupancyPlugin = self.options.get("OccupancyPlugin")
        if occupancyPlugin:
            res = ObjectLoader().loadObject(f"Resources.Storage.OccupancyPlugins.{occupancyPlugin}")
            if not res["OK"]:
                return S_ERROR(errno.EPROTONOSUPPORT, f"Failed to load occupancy plugin {occupancyPlugin}")
            log.debug(f"Use occupancy plugin {occupancyPlugin}")
            try:
                occupancyPlugin = res["Value"](self)
                res = occupancyPlugin.getOccupancy(**kwargs)
                if not res["OK"]:
                    return res
                occupancyDict = res["Value"]
                return self.checkOccupancy(occupancyDict, unit)
            except Exception as e:
                return S_ERROR(f"Occupancy plugin failed: {str(e)}")

        filteredPlugins = self.__filterPlugins("getOccupancy")
        if not filteredPlugins:
            return S_ERROR(errno.EPROTONOSUPPORT, "No storage plugins to query the occupancy")

        # Try all of the storages one by one
        for storage in filteredPlugins:
            # The result of the plugin is always in B
            res = storage.getOccupancy(**kwargs)
            if res["OK"]:
                occupancyDict = res["Value"]
                result = self.checkOccupancy(occupancyDict, unit)
                if not result["OK"]:
                    continue
                occupancyDict = result["Value"]

                if "SpaceReservation" not in occupancyDict:
                    occupancyDict["SpaceReservation"] = self.options.get("SpaceReservation")
                return S_OK(occupancyDict)

        return S_ERROR("Could not retrieve the occupancy from any plugin")

    def checkOccupancy(self, occupancyDict, unit):
        """Validate occupancy dict given by getOccupancy

        :param dict occupancyDict: occupancy given by occupancy or storage plugins
        :param str unit: Any of ( 'B', 'kB', 'MB', 'GB', 'TB', 'PB')

        :returns: S_OK with updated occupancyDict
        """
        log = self.log.getSubLogger("checkOccupancy")

        # Mandatory parameters
        mandatoryParams = {"Total", "Free"}
        # Make sure all the mandatory parameters are present
        if set(occupancyDict) & mandatoryParams != mandatoryParams:
            msg = f"Missing mandatory parameters {str(mandatoryParams - set(occupancyDict))}"
            log.error(msg)
            return S_ERROR(msg)

        # It can happen that Used space > total space (quota enforcement on EOS are async)
        # In that case, just set it to 0, and issue a warning
        if occupancyDict["Free"] < 0:
            log.warn("Negative free value in occupancy dict", str(occupancyDict["Free"]))
            occupancyDict["Free"] = 0

        # Since plugins return Bytes, we do not need to convert if that's what we want
        if unit != "B":
            for space in ["Total", "Free"]:
                convertedSpace = convertSizeUnits(occupancyDict[space], "B", unit)
                # If we have a conversion error, we go to the next plugin
                if convertedSpace == -sys.maxsize:
                    msg = f"Error converting {space} space from MB to {unit}: {occupancyDict[space]}"
                    log.error(msg)
                    return S_ERROR("Error converting")
                occupancyDict[space] = convertedSpace
        return S_OK(occupancyDict)

    def status(self):
        """
         Return Status of the SE, a dictionary with:

          * Read: True (is allowed), False (it is not allowed)
          * Write: True (is allowed), False (it is not allowed)
          * Remove: True (is allowed), False (it is not allowed)
          * Check: True (is allowed), False (it is not allowed).

            .. note:: Check is always allowed IF Read is allowed
                      (regardless of what set in the Check option of the configuration)

          * DiskSE: True if TXDY with Y > 0 (defaults to True)
          * TapeSE: True if TXDY with X > 0 (defaults to False)
          * TotalCapacityTB: float (-1 if not defined)
          * DiskCacheTB: float (-1 if not defined)

        It returns directly the dictionary
        """

        self.log.getSubLogger("getStatus").debug(f"determining status of {self.name}.")

        retDict = {}
        if not self.valid:
            retDict["Read"] = False
            retDict["Write"] = False
            retDict["Remove"] = False
            retDict["Check"] = False
            retDict["DiskSE"] = False
            retDict["TapeSE"] = False
            retDict["TotalCapacityTB"] = -1
            retDict["DiskCacheTB"] = -1
            return retDict

        # If nothing is defined in the CS Access is allowed
        # If something is defined, then it must be set to Active
        retDict["Read"] = not (
            "ReadAccess" in self.options and self.options["ReadAccess"] not in ("Active", "Degraded")
        )
        retDict["Write"] = not (
            "WriteAccess" in self.options and self.options["WriteAccess"] not in ("Active", "Degraded")
        )
        retDict["Remove"] = not (
            "RemoveAccess" in self.options and self.options["RemoveAccess"] not in ("Active", "Degraded")
        )
        if retDict["Read"]:
            retDict["Check"] = True
        else:
            retDict["Check"] = not (
                "CheckAccess" in self.options and self.options["CheckAccess"] not in ("Active", "Degraded")
            )
        diskSE = True
        tapeSE = False
        if "SEType" in self.options:
            # Type should follow the convention TXDY
            seType = self.options["SEType"]
            diskSE = re.search("D[1-9]", seType) is not None
            tapeSE = re.search("T[1-9]", seType) is not None
        retDict["DiskSE"] = diskSE
        retDict["TapeSE"] = tapeSE
        try:
            retDict["TotalCapacityTB"] = float(self.options["TotalCapacityTB"])
        except Exception:
            retDict["TotalCapacityTB"] = -1
        try:
            retDict["DiskCacheTB"] = float(self.options["DiskCacheTB"])
        except Exception:
            retDict["DiskCacheTB"] = -1

        return retDict

    def isValid(self, operation=None):
        """check CS/RSS statuses for :operation:

        :param str operation: operation name
        """
        log = self.log.getSubLogger("isValid")
        log.debug(f"Determining if the StorageElement {self.name} is valid for VO {self.vo}")

        if not self.valid:
            log.debug("Failed to create StorageElement plugins.", self.errorReason)
            return S_ERROR(f"SE.isValid: Failed to create StorageElement plugins: {self.errorReason}")

        # Check if the Storage Element is eligible for the user's VO
        if "VO" in self.options and self.vo not in self.options["VO"]:
            log.debug("StorageElement is not allowed for VO", self.vo)
            return S_ERROR(errno.EACCES, "StorageElement.isValid: StorageElement is not allowed for VO")
        log.debug(f"Determining if the StorageElement {self.name} is valid for operation '{operation}'")
        if (not operation) or (operation in self.okMethods):
            return S_OK()

        # Determine whether the StorageElement is valid for checking, reading, writing
        status = self.status()
        checking = status["Check"]
        reading = status["Read"]
        writing = status["Write"]
        removing = status["Remove"]

        # Determine whether the requested operation can be fulfilled
        if (not operation) and (not reading) and (not writing) and (not checking):
            log.debug("Read, write and check access not permitted.")
            return S_ERROR(errno.EACCES, "SE.isValid: Read, write and check access not permitted.")

        # The supplied operation can be 'Read','Write' or any of the possible StorageElement methods.
        if (
            (operation in self.readMethods)
            or (operation in self.stageMethods)
            or (operation.lower() in ("read", "readaccess"))
        ):
            operation = "ReadAccess"
        elif operation in self.writeMethods or (operation.lower() in ("write", "writeaccess")):
            operation = "WriteAccess"
        elif operation in self.removeMethods or (operation.lower() in ("remove", "removeaccess")):
            operation = "RemoveAccess"
        elif operation in self.checkMethods or (operation.lower() in ("check", "checkaccess")):
            operation = "CheckAccess"
        else:
            log.debug("The supplied operation is not known.", operation)
            return S_ERROR(DErrno.ENOMETH, "SE.isValid: The supplied operation is not known.")
        log.debug(f"check the operation: {operation} ")

        # Check if the operation is valid
        if operation == "CheckAccess":
            if not reading:
                if not checking:
                    log.debug("Check access not currently permitted.")
                    return S_ERROR(errno.EACCES, "SE.isValid: Check access not currently permitted.")
        if operation == "ReadAccess":
            if not reading:
                log.debug("Read access not currently permitted.")
                return S_ERROR(errno.EACCES, "SE.isValid: Read access not currently permitted.")
        if operation == "WriteAccess":
            if not writing:
                log.debug("Write access not currently permitted.")
                return S_ERROR(errno.EACCES, "SE.isValid: Write access not currently permitted.")
        if operation == "RemoveAccess":
            if not removing:
                log.debug("Remove access not currently permitted.")
                return S_ERROR(errno.EACCES, "SE.isValid: Remove access not currently permitted.")
        return S_OK()

    def getProtocolSections(self):
        """Get the list of all the ProtocolSections defined for this Storage Element"""
        self.log.getSubLogger("getProtocolSections").debug(f"Obtaining all protocol sections of {self.name}.")
        if not self.valid:
            return S_ERROR(self.errorReason)
        allProtocolSections = self.localProtocolSections + self.remoteProtocolSections
        return S_OK(allProtocolSections)

    def getRemoteProtocolSections(self):
        """Get the list of all the remote access protocols defined for this Storage Element"""
        self.log.getSubLogger("getRemoteProtocolSections").debug(f"Obtaining remote protocols for {self.name}.")
        if not self.valid:
            return S_ERROR(self.errorReason)
        return S_OK(self.remoteProtocolSections)

    def getLocalProtocolSections(self):
        """Get the list of all the local access protocols defined for this Storage Element"""
        self.log.getSubLogger("getLocalProtocolSections").debug(f"Obtaining local protocols for {self.name}.")
        if not self.valid:
            return S_ERROR(self.errorReason)
        return S_OK(self.localProtocolSections)

    def getStorageParameters(self, protocolSection=None, protocol=None):
        """Get plugin specific options

        :param protocolSection: protocolSection we are interested in
        :param protocol: protocol we are interested in

        Either protocolSection or protocol can be defined, not both, but at least one of them
        """

        # both set
        if protocolSection and protocol:
            return S_ERROR(errno.EINVAL, "plugin and protocol cannot be set together.")
        # both None
        elif not (protocolSection or protocol):
            return S_ERROR(errno.EINVAL, "plugin and protocol cannot be None together.")

        log = self.log.getSubLogger("getStorageParameters")

        reqStr = f"protocolSection {protocolSection}" if protocolSection else f"protocol {protocol}"

        log.debug(f"Obtaining storage parameters for {self.name} for {reqStr}.")

        if protocolSection:
            storage = self.storages.get(protocolSection)
            if storage:
                return S_OK(storage.getParameters())
        else:
            for storage in self.storages.values():
                storageParameters = storage.getParameters()
                if storageParameters["Protocol"] == protocol:
                    return S_OK(storageParameters)

        errStr = "Requested protocolSection or protocol not available."
        log.debug(errStr, f"{reqStr} for {self.name}")
        return S_ERROR(errno.ENOPROTOOPT, errStr)

    def __getAllProtocols(self, protoType):
        """Returns the list of all protocols for Input or Output

        :param proto = InputProtocols or OutputProtocols

        """
        return set(
            reduce(lambda x, y: x + y, [plugin.protocolParameters[protoType] for plugin in self.storages.values()])
        )

    def _getAllInputProtocols(self):
        """Returns all the protocols supported by the SE for Input"""
        return self.__getAllProtocols("InputProtocols")

    def _getAllOutputProtocols(self):
        """Returns all the protocols supported by the SE for Output"""
        return self.__getAllProtocols("OutputProtocols")

    def generateTransferURLsBetweenSEs(self, lfns, sourceSE, protocols=None):
        """This negotiate the URLs to be used for third party copy.
        This is mostly useful for FTS. If protocols is given,
        it restricts the list of plugins to use

        :param lfns: list/dict of lfns to generate the URLs
        :param sourceSE: storageElement instance of the sourceSE
        :param protocols: ordered protocol restriction list

        :return: dictionary with keys:

          * Successful: lfn indexed pair (src, dest) urls
          * Failed: lfn indexed with error
          * Protocols: tuple (srcProto, destProto)
        """
        log = self.log.getSubLogger("generateTransferURLsBetweenSEs")

        result = checkArgumentFormat(lfns)
        if result["OK"]:
            lfns = result["Value"]
        else:
            errStr = "Supplied urls must be string, list of strings or a dictionary."
            log.debug(errStr)
            return S_ERROR(errno.EINVAL, errStr)

        # First, find common protocols to use
        res = self.negociateProtocolWithOtherSE(sourceSE, protocols=protocols)

        if not res["OK"]:
            return res

        commonProtocols = res["Value"]

        # We sort the storage plugins based on their native protocol
        # according to the common protocol list
        # This is to favor for example the xroot plugin over the SRM plugin
        # even if both can provide xroot
        sourceSEStorages = sorted(
            sourceSE.storages.values(), key=lambda x: getIndexInList(x.getParameters()["Protocol"], commonProtocols)
        )

        selfStorages = sorted(
            self.storages.values(), key=lambda x: getIndexInList(x.getParameters()["Protocol"], commonProtocols)
        )

        # Taking each protocol at the time, we try to generate src and dest URLs
        for proto in commonProtocols:
            srcPlugin = None
            destPlugin = None

            log.debug(f"Trying to find plugins for protocol {proto}")

            # Finding the source storage plugin
            for storagePlugin in sourceSEStorages:
                log.debug(f"Testing {storagePlugin.pluginName} as source plugin")
                storageParameters = storagePlugin.getParameters()
                nativeProtocol = storageParameters["Protocol"]
                # If the native protocol of the plugin is allowed for read
                if nativeProtocol in sourceSE.localAccessProtocolList:
                    # If the plugin can generate the protocol we are interested in
                    if proto in storageParameters["OutputProtocols"]:
                        log.debug("Selecting it")
                        srcPlugin = storagePlugin
                        break
            # If we did not find a source plugin, continue
            if srcPlugin is None:
                log.debug(f"Could not find a source plugin for protocol {proto}")
                continue

            # Finding the destination storage plugin
            for storagePlugin in selfStorages:
                log.debug(f"Testing {storagePlugin.pluginName} as destination plugin")

                storageParameters = storagePlugin.getParameters()
                nativeProtocol = storageParameters["Protocol"]
                # If the native protocol of the plugin is allowed for write
                if nativeProtocol in self.localWriteProtocolList:
                    # If the plugin can accept the protocol we are interested in
                    if proto in storageParameters["InputProtocols"]:
                        log.debug("Selecting it")
                        destPlugin = storagePlugin
                        break

            # If we found both a source and destination plugin, we are happy,
            # otherwise we continue with the next protocol
            if destPlugin is None:
                log.debug(f"Could not find a destination plugin for protocol {proto}")
                srcPlugin = None
                continue

            failed = {}
            successful = {}
            # Generate the URLs
            for lfn in lfns:
                # Source URL first
                res = srcPlugin.constructURLFromLFN(lfn, withWSUrl=True)
                if not res["OK"]:
                    errMsg = f"Error generating source url: {res['Message']}"
                    log.debug("Error generating source url", errMsg)
                    failed[lfn] = errMsg
                    continue
                srcURL = res["Value"]

                # Destination URL
                res = destPlugin.constructURLFromLFN(lfn, withWSUrl=True)
                if not res["OK"]:
                    errMsg = f"Error generating destination url: {res['Message']}"
                    log.debug("Error generating destination url", errMsg)
                    failed[lfn] = errMsg
                    continue
                destURL = res["Value"]

                successful[lfn] = (srcURL, destURL)

            nativeSrcProtocol = srcPlugin.getParameters()["Protocol"]
            nativeDestProtocol = destPlugin.getParameters()["Protocol"]
            return S_OK(
                {"Successful": successful, "Failed": failed, "Protocols": (nativeSrcProtocol, nativeDestProtocol)}
            )

        return S_ERROR(
            errno.ENOPROTOOPT, f"Could not find a protocol between source {sourceSE.name} and target {self.name}"
        )

    def negociateProtocolWithOtherSE(self, sourceSE, protocols=None):
        """Negotiate what protocol could be used for a third party transfer
        between the sourceSE and ourselves. If protocols is given,
        the chosen protocol has to be among those

        :param sourceSE: storageElement instance of the sourceSE
        :param protocols: ordered protocol restriction list

        :return: a list protocols that fits the needs, or None

        """

        # No common protocols if this is a proxy storage
        if self.useProxy:
            return S_OK([])

        log = self.log.getSubLogger("negociateProtocolWithOtherSE")

        log.debug(f"Negotiating protocols between {sourceSE.name} and {self.name} (protocols {protocols})")

        # Take all the protocols the destination can accept as input
        destProtocols = self._getAllInputProtocols()

        log.debug(f"Destination input protocols {destProtocols}")

        # Take all the protocols the source can provide
        sourceProtocols = sourceSE._getAllOutputProtocols()

        log.debug(f"Source output protocols {sourceProtocols}")

        commonProtocols = destProtocols & sourceProtocols

        # If a restriction list is defined
        # take the intersection, and sort the commonProtocols
        # based on the protocolList order
        if protocols:
            protocolList = list(protocols)
            commonProtocols = sorted(commonProtocols & set(protocolList), key=lambda x: getIndexInList(x, protocolList))

        log.debug(f"Common protocols {commonProtocols}")

        return S_OK(list(commonProtocols))

    #################################################################################################
    #
    # These are the basic get functions for lfn manipulation
    #

    def __getURLPath(self, url):
        """Get the part of the URL path below the basic storage path.
        This path must coincide with the LFN of the file in order to be compliant with the DIRAC conventions.
        """
        log = self.log.getSubLogger("__getURLPath")
        log.debug(f"Getting path from url in {self.name}.")
        if not self.valid:
            return S_ERROR(self.errorReason)
        res = pfnparse(url)
        if not res["OK"]:
            return res
        fullURLPath = f"{res['Value']['Path']}/{res['Value']['FileName']}"

        # Check all available storages and check whether the url is for that protocol
        urlPath = ""
        for storage in self.storages.values():
            res = storage.isNativeURL(url)
            if res["OK"]:
                if res["Value"]:
                    parameters = storage.getParameters()
                    saPath = parameters["Path"]
                    if not saPath:
                        # If the sa path doesn't exist then the url path is the entire string
                        urlPath = fullURLPath
                    else:
                        if re.search(saPath, fullURLPath):
                            # Remove the sa path from the fullURLPath
                            urlPath = fullURLPath.replace(saPath, "")
            if urlPath:
                return S_OK(urlPath)
        # This should never happen. DANGER!!
        errStr = "Failed to get the url path for any of the protocols!!"
        log.debug(errStr)
        return S_ERROR(errStr)

    def getLFNFromURL(self, urls):
        """Get the LFN from the PFNS .

        :param lfn: input lfn or lfns (list/dict)

        """
        result = checkArgumentFormat(urls)
        if result["OK"]:
            urlDict = result["Value"]
        else:
            errStr = "Supplied urls must be string, list of strings or a dictionary."
            self.log.getSubLogger("getLFNFromURL").debug(errStr)
            return S_ERROR(errno.EINVAL, errStr)

        retDict = {"Successful": {}, "Failed": {}}
        for url in urlDict:
            res = self.__getURLPath(url)
            if res["OK"]:
                retDict["Successful"][url] = res["Value"]
            else:
                retDict["Failed"][url] = res["Message"]
        return S_OK(retDict)

    ###########################################################################################
    #
    # This is the generic wrapper for file operations
    #

    @convertToReturnValue
    def getWLCGTokenPath(self, lfn: str):
        """
        EXPERIMENTAL
        return the path to put in the token, relative to the vo path as configured
        in the storage.

        """
        wlcgTokenBasePath = self.options.get("WLCGTokenBasePath")
        if not wlcgTokenBasePath:
            raise ValueError("WLCGTokenBasePath not configured")

        for storage in self.storages.values():
            try:
                return storage.getWLCGTokenPath(lfn, wlcgTokenBasePath)
            except Exception:
                continue
        raise RuntimeError("Could not get WLCGTokenPath")

    def getURL(self, lfn, protocol=False, replicaDict=None):
        """execute 'getTransportURL' operation.

        :param str lfn: string, list or dictionary of lfns
        :param protocol: if no protocol is specified, we will request self.turlProtocols
        :param replicaDict: optional results from the File Catalog replica query

        """

        self.log.getSubLogger("getURL").debug(
            f"Getting accessUrl {f'({protocol})' if protocol else ''} for lfn in {self.name}."
        )
        protocols = None
        if not protocol:
            # This turlProtocols seems totally useless.
            # Get ride of it when gfal2 is totally ready
            # and replace it with the localAccessProtocol list
            protocols = self.turlProtocols
        elif isinstance(protocol, list):
            protocols = protocol
        elif isinstance(protocol, str):
            protocols = [protocol]

        self.methodName = "getTransportURL"
        result = self.__executeMethod(lfn, protocols=protocols)
        return result

    def __isLocalSE(self):
        """Test if the Storage Element is local in the current context"""
        self.log.getSubLogger("LocalSE").debug(f"Determining whether {self.name} is a local SE.")

        import DIRAC

        localSEs = getSEsForSite(DIRAC.siteName())["Value"]
        if self.name in localSEs:
            return S_OK(True)
        else:
            return S_OK(False)

    def __getFileCatalog(self):
        if not self.__fileCatalog:
            self.__fileCatalog = FileCatalog(vo=self.vo)
        return self.__fileCatalog

    def __generateURLDict(self, lfns, storage, replicaDict=None):
        """Generates a dictionary (url : lfn ), where the url are constructed
        from the lfn using the constructURLFromLFN method of the storage plugins.

        :param lfns: dictionary {lfn:whatever}

        :returns: dictionary {constructed url : lfn}
        """
        log = self.log.getSubLogger("__generateURLDict")
        log.debug(f"generating url dict for {len(lfns)} lfn in {self.name}.")

        if not replicaDict:
            replicaDict = {}

        urlDict = {}  # url : lfn
        failed = {}  # lfn : string with errors
        for lfn in lfns:
            if self.useCatalogURL:
                # Is this self.name alias proof?
                url = replicaDict.get(lfn, {}).get(self.name, "")
                if url:
                    urlDict[url] = lfn
                    continue
                else:
                    fc = self.__getFileCatalog()
                    result = fc.getReplicas()
                    if not result["OK"]:
                        failed[lfn] = result["Message"]
                    url = result["Value"]["Successful"].get(lfn, {}).get(self.name, "")

                if not url:
                    failed[lfn] = "Failed to get catalog replica"
                else:
                    # Update the URL according to the current SE description
                    result = returnSingleResult(storage.updateURL(url))
                    if not result["OK"]:
                        failed[lfn] = result["Message"]
                    else:
                        urlDict[result["Value"]] = lfn
            else:
                result = storage.constructURLFromLFN(lfn, withWSUrl=True)
                if not result["OK"]:
                    errStr = result["Message"]
                    log.debug(errStr, f"for {lfn}")
                    failed[lfn] = f"{failed[lfn]} {errStr}" if lfn in failed else errStr
                else:
                    urlDict[result["Value"]] = lfn

        res = S_OK({"Successful": urlDict, "Failed": failed})
        #     res['Failed'] = failed
        return res

    def __filterPlugins(self, methodName, protocols=None, inputProtocol=None):
        """Determine the list of plugins that
         can be used for a particular action

         Args:
           method(str): method to execute
           protocols(list): specific protocols might be requested
           inputProtocol(str): in case the method is putFile, this specifies
                               the protocol given as source

        Returns:
          list: list of storage plugins
        """

        log = self.log.getSubLogger("__filterPlugins")
        log.debug(f"Filtering plugins for {methodName} (protocol = {protocols} ; inputProtocol = {inputProtocol})")

        if isinstance(protocols, str):
            protocols = [protocols]

        pluginsToUse = []

        potentialProtocols = []
        allowedProtocols = []

        if methodName in self.readMethods + self.checkMethods:
            allowedProtocols = self.localAccessProtocolList
        elif methodName in self.stageMethods:
            allowedProtocols = self.localStageProtocolList
        elif methodName in self.removeMethods + self.writeMethods:
            allowedProtocols = self.localWriteProtocolList
        else:
            # OK methods
            # If a protocol or protocol list is specified, we only use the plugins that
            # can generate such protocol
            # otherwise we return them all
            if protocols:
                setProtocol = set(protocols)
                for plugin in self.storages.values():
                    if set(plugin.protocolParameters.get("OutputProtocols", [])) & setProtocol:
                        log.debug(f"Plugin {plugin.pluginName} can generate compatible protocol")
                        pluginsToUse.append(plugin)
            else:
                pluginsToUse = list(self.storages.values())

            # The closest list for "OK" methods is the AccessProtocol preference, so we sort based on that

            pluginsToUse = sorted(
                pluginsToUse,
                key=lambda x: (
                    getIndexInList(x.protocolParameters["Protocol"], self.localAccessProtocolList),
                    x.protocolSectionName in self.remoteProtocolSections,
                ),
            )

            log.debug(f"Plugins to be used for {methodName}: {[p.protocolSectionName for p in pluginsToUse]}")
            return pluginsToUse

        log.debug(f"Allowed protocol: {allowedProtocols}")

        # if a list of protocol is specified, take it into account
        if protocols:
            potentialProtocols = list(set(allowedProtocols) & set(protocols))
        else:
            potentialProtocols = allowedProtocols

        log.debug(f"Potential protocols {potentialProtocols}")

        localSE = self.__isLocalSE()["Value"]

        for protocolSection, plugin in self.storages.items():
            # Determine whether to use this storage object
            pluginParameters = plugin.getParameters()
            isProxyPlugin = pluginParameters.get("PluginName") == "Proxy"

            if not pluginParameters:
                log.debug("Failed to get storage parameters.", f"{self.name} {protocolSection}")
                continue

            if not (protocolSection in self.remoteProtocolSections) and not localSE and not isProxyPlugin:
                # If the SE is not local then we can't use local protocols
                log.debug(f"Local protocol not appropriate for remote use: {protocolSection}.")
                continue

            if pluginParameters["Protocol"] not in potentialProtocols:
                log.debug(f"Plugin {protocolSection} not allowed for {methodName}.")
                continue

            # If we are attempting a putFile and we know the inputProtocol
            if methodName == "putFile" and inputProtocol:
                if inputProtocol not in pluginParameters["InputProtocols"]:
                    log.debug(f"Plugin {protocolSection} not appropriate for {inputProtocol} protocol as input.")
                    continue

            pluginsToUse.append(plugin)

        # sort the plugins according to the lists in the CS
        # and then favor local plugins over remote ones
        # note: False < True, so to have local plugin first,
        # we test if the plugin is in the remote list
        pluginsToUse = sorted(
            pluginsToUse,
            key=lambda x: (
                getIndexInList(x.protocolParameters["Protocol"], allowedProtocols),
                x.protocolSectionName in self.remoteProtocolSections,
            ),
        )

        log.debug(f"Plugins to be used for {methodName}: {[p.protocolSectionName for p in pluginsToUse]}")

        return pluginsToUse

    def __executeMethod(self, lfn, *args, **kwargs):
        """Forward the call to each storage in turn until one works.
        The method to be executed is stored in self.methodName

        :param lfn: string, list or dictionary
        :param *args: variable amount of non-keyword arguments. SHOULD BE EMPTY
        :param **kwargs: keyword arguments

        A special argument is 'protocols', which will be used by the StorageElement to filter
        the usable plugins. Unless the method being executed is getTransportURL, this parameter
        is removed from kwargs.
        A special kwargs is 'inputProtocol', which can be specified for putFile. It describes
        the protocol used as source protocol, since there is in principle only one.


        :returns: S_OK( { 'Failed': {lfn : reason} , 'Successful': {lfn : value} } )
                The Failed dict contains the lfn only if the operation failed on all the storages
                The Successful dict contains the value returned by the successful storages.

        """

        removedArgs = {}
        log = self.log.getSubLogger("__executeMethod")
        log.debug(f"preparing the execution of {self.methodName}")

        # args should normaly be empty to avoid problem...
        if args:
            log.debug(f"args should be empty! {args}")
            # because there is normally only one kw argument, I can move it from args to kwargs
            methDefaultArgs = list(StorageElementItem.__defaultsArguments.get(self.methodName, {}))
            if methDefaultArgs:
                kwargs[methDefaultArgs[0]] = args[0]
                args = args[1:]
            log.debug(f"put it in kwargs, but dirty and might be dangerous!args {args} kwargs {kwargs}")

        # We check the deprecated arguments
        for depArg in StorageElementItem.__deprecatedArguments:
            if depArg in kwargs:
                log.verbose(f"{depArg} is not an allowed argument anymore. Please change your code!")
                removedArgs[depArg] = kwargs[depArg]
                del kwargs[depArg]

        # Set default argument if any
        methDefaultArgs = StorageElementItem.__defaultsArguments.get(self.methodName, {})
        for argName in methDefaultArgs:
            if argName not in kwargs:
                log.debug(
                    "default argument %s for %s not present.\
         Setting value %s."
                    % (argName, self.methodName, methDefaultArgs[argName])
                )
                kwargs[argName] = methDefaultArgs[argName]

        res = checkArgumentFormat(lfn)
        if not res["OK"]:
            errStr = "Supplied lfns must be string, list of strings or a dictionary."
            log.debug(errStr)
            return res
        lfnDict = res["Value"]

        log.debug(f"Attempting to perform '{self.methodName}' operation with {len(lfnDict)} lfns.")

        res = self.isValid(operation=self.methodName)
        if not res["OK"]:
            return res
        else:
            if not self.valid:
                return S_ERROR(self.errorReason)
        # In case executing putFile, we can assume that all the source urls
        # are from the same protocol. This optional parameter, if defined
        # can be used to ignore some storage plugins and thus save time
        # and avoid fake failures showing in the accounting
        inputProtocol = kwargs.pop("inputProtocol", None)

        # the 'protocols' parameter is only given to the plugin when calling getTransportURL.
        # The other methods do not expect it.
        protocols = kwargs.get("protocols")
        if self.methodName != "getTransportURL":
            kwargs.pop("protocols", None)

        successful = {}
        failed = {}
        filteredPlugins = self.__filterPlugins(self.methodName, protocols, inputProtocol)
        if not filteredPlugins:
            return S_ERROR(
                errno.EPROTONOSUPPORT,
                "No storage plugins matching the requirements\
                                           (operation %s protocols %s inputProtocol %s)"
                % (self.methodName, protocols, inputProtocol),
            )
        # Try all of the storages one by one
        for storage in filteredPlugins:
            # Determine whether to use this storage object
            storageParameters = storage.getParameters()
            pluginName = storageParameters["PluginName"]

            if not lfnDict:
                log.debug(f"No lfns to be attempted for {pluginName} protocol.")
                continue

            log.debug(f"Generating {len(lfnDict)} protocol URLs for {pluginName}.")
            replicaDict = kwargs.pop("replicaDict", {})
            if storage.pluginName != "Proxy":
                res = self.__generateURLDict(lfnDict, storage, replicaDict=replicaDict)
                urlDict = res["Value"]["Successful"]  # url : lfn
                failed.update(res["Value"]["Failed"])
            else:
                urlDict = {lfn: lfn for lfn in lfnDict}
            if not urlDict:
                log.debug(f"__executeMethod No urls generated for protocol {pluginName}.")
            else:
                log.debug(f"Attempting to perform '{self.methodName}' for {len(urlDict)} physical files")
                fcn = None
                if hasattr(storage, self.methodName) and callable(getattr(storage, self.methodName)):
                    fcn = getattr(storage, self.methodName)
                if not fcn:
                    return S_ERROR(
                        DErrno.ENOMETH, "SE.__executeMethod: unable to invoke %s, it isn't a member function of storage"
                    )
                urlsToUse = {}  # url : the value of the lfn dictionary for the lfn of this url
                for url in urlDict:
                    urlsToUse[url] = lfnDict[urlDict[url]]

                startDate = datetime.datetime.utcnow()
                startTime = time.time()
                res = fcn(urlsToUse, *args, **kwargs)
                elapsedTime = time.time() - startTime

                self.addAccountingOperation(urlDict, startDate, elapsedTime, storageParameters, res)

                if not res["OK"]:
                    errStr = f"Completely failed to perform {self.methodName}."
                    log.verbose(errStr, f"with plugin {pluginName}: {res['Message']}")
                    for lfn in urlDict.values():
                        if lfn not in failed:
                            failed[lfn] = ""
                        failed[lfn] = f"{failed[lfn]} {res['Message']}" if failed[lfn] else res["Message"]

                else:
                    for url, lfn in urlDict.items():
                        if url not in res["Value"]["Successful"]:
                            if lfn not in failed:
                                failed[lfn] = ""
                            if url in res["Value"]["Failed"]:
                                self.log.verbose(
                                    f"Failure in plugin to perform {self.methodName}",
                                    f"Plugin: {pluginName} lfn: {lfn} error {res['Value']['Failed'][url]}",
                                )
                                failed[lfn] = (
                                    f"{failed[lfn]} {res['Value']['Failed'][url]}"
                                    if failed[lfn]
                                    else res["Value"]["Failed"][url]
                                )
                            else:
                                errStr = "No error returned from plug-in"
                                failed[lfn] = f"{failed[lfn]} {errStr}" if failed[lfn] else errStr
                        else:
                            successful[lfn] = res["Value"]["Successful"][url]
                            if lfn in failed:
                                failed.pop(lfn)
                            lfnDict.pop(lfn)

        self.dataOpSender.concludeSending()

        return S_OK({"Failed": failed, "Successful": successful})

    def __getattr__(self, name):
        """Forwards the equivalent Storage calls to __executeMethod"""
        # We take either the equivalent name, or the name itself
        self.methodName = StorageElementItem.__equivalentMethodNames.get(name, None)

        if self.methodName:
            return self.__executeMethod

        raise AttributeError(f"StorageElement does not have a method '{name}'")

    def addAccountingOperation(self, urlDict, startDate, elapsedTime, storageParameters, callRes):
        """
        Generates a DataOperationSender instance and sends the operation data filled in accountingDict.

        :param urlDict: {url:lfn} on which we attempted the operation
        :param startDate: datetime, start of the operation
        :param elapsedTime: time (seconds) the operation took
        :param storageParameters: the parameters of the plugins used to perform the operation
        :param callRes: the return of the method call, S_OK or S_ERROR

        The operation is generated with the OperationType "se.methodName"
        The TransferSize and TransferTotal for directory methods actually take into
        account the files inside the directory, and not the amount of directory given
        as parameter
        """

        if self.methodName not in (self.readMethods + self.writeMethods + self.removeMethods + self.stageMethods):
            return

        accountingDict = {}
        accountingDict["OperationType"] = f"se.{self.methodName}"
        accountingDict["User"] = getProxyInfo().get("Value", {}).get("username", "unknown")
        accountingDict["RegistrationTime"] = 0.0
        accountingDict["RegistrationOK"] = 0
        accountingDict["RegistrationTotal"] = 0

        # if it is a get method, then source and destination of the transfer should be inverted
        if self.methodName == "getFile":
            accountingDict["Destination"] = siteName()
            accountingDict["Source"] = self.name
        else:
            accountingDict["Destination"] = self.name
            accountingDict["Source"] = siteName()

        accountingDict["TransferTotal"] = 0
        accountingDict["TransferOK"] = 0
        accountingDict["TransferSize"] = 0
        accountingDict["FinalStatus"] = "Successful"
        accountingDict["Protocol"] = storageParameters.get("Protocol", "unknown")
        accountingDict["TransferTime"] = elapsedTime

        endDate = startDate + datetime.timedelta(seconds=elapsedTime)

        totalSucc = 0
        if not callRes["OK"]:
            # Everything failed
            accountingDict["TransferTotal"] = len(urlDict)
            accountingDict["FinalStatus"] = "Failed"
        else:
            succ = callRes.get("Value", {}).get("Successful", {})

            failed = callRes.get("Value", {}).get("Failed", {})

            totalSize = 0
            # We don't take len(lfns) in order to make two
            # separate entries in case of few failures
            totalSucc = len(succ)

            if self.methodName in ("putFile", "getFile"):
                # putFile and getFile return for each entry
                # in the successful dir the size of the corresponding file
                totalSize = sum(succ.values())

            elif self.methodName in ("putDirectory", "getDirectory"):
                # putDirectory and getDirectory return for each dir name
                # a dictionary with the keys 'Files' and 'Size'
                totalSize = sum(val.get("Size", 0) for val in succ.values() if isinstance(val, dict))
                totalSucc = sum(val.get("Files", 0) for val in succ.values() if isinstance(val, dict))
                accountingDict["TransferOK"] = len(succ)

            accountingDict["TransferSize"] = totalSize
            accountingDict["TransferTotal"] = totalSucc
            accountingDict["TransferOK"] = totalSucc

            if callRes["Value"]["Failed"]:
                failedAccountingDict = deepcopy(accountingDict)
                failedAccountingDict["TransferTotal"] = len(failed)
                failedAccountingDict["TransferOK"] = 0
                failedAccountingDict["TransferSize"] = 0
                failedAccountingDict["FinalStatus"] = "Failed"

                # Send also the list of failures, only if we send to monitoring
                failedRecords = []
                if "Monitoring" in self.dataOpSender.monitoringOptions:
                    for failedURL, errorMsg in failed.items():
                        failedRecord = {
                            "timestamp": int(toEpochMilliSeconds()),
                            "LFN": urlDict[failedURL],
                            "URL": failedURL,
                            "OperationType": accountingDict["OperationType"],
                            "User": accountingDict["User"],
                            "ExecutionSite": siteName(),
                            "TargetSE": self.name,
                            "Protocol": accountingDict["Protocol"],
                            "Error": str(errorMsg),
                            "Component": "StorageElement",
                            "Hostname": getFQDN(),
                        }
                        failedRecords.append(failedRecord)
                res = self.dataOpSender.sendData(
                    failedAccountingDict, startTime=startDate, endTime=endDate, failedRecords=failedRecords
                )
                if not res["OK"]:
                    self.log.error("Could not send failed accounting report", res["Message"])

        # Only send if there are successes
        if totalSucc:
            self.dataOpSender.sendData(accountingDict, commitFlag=False, startTime=startDate, endTime=endDate)


StorageElement = StorageElementCache()
