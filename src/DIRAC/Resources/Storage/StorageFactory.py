""" Storage Factory Class - creates instances of various Storage plugins from the Core DIRAC or extensions

    This Class has three public methods:

    getStorageName():  Resolves links in the CS to the target SE name.

    getStorage():      This creates a single storage stub based on the parameters passed in a dictionary.
                      This dictionary must have the following keys: 'StorageName','PluginName','Protocol'
                      Other optional keys are 'Port','Host','Path','SpaceToken'

    getStorages()      This takes a DIRAC SE definition and creates storage stubs for the protocols found in the CS.
                      By providing an optional list of protocols it is possible to limit the created stubs.
"""

from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ConfigurationSystem.Client.Helpers.Path import cfgPath
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

# Path to find Base SE
SE_BASE_CONFIG_PATH = "/Resources/StorageElementBases"
# Path to find concrete SE
SE_CONFIG_PATH = "/Resources/StorageElements"


class StorageFactory:
    def __init__(self, useProxy=False, vo=None):
        self.proxy = False
        self.proxy = useProxy
        self.resourceStatus = ResourceStatus()
        self.vo = vo
        if self.vo is None:
            result = getVOfromProxyGroup()
            if result["OK"]:
                self.vo = result["Value"]
            else:
                raise RuntimeError("Can not get the current VO context")
        self.remoteProtocolSections = []
        self.localProtocolSections = []
        self.name = ""
        self.options = {}
        self.protocols = {}
        self.storages = {}

    ###########################################################################################
    #
    # Below are public methods for obtaining storage objects
    #

    def getStorageName(self, initialName):
        return self._getConfigStorageName(initialName, "Alias")

    def getStorage(self, parameterDict, hideExceptions=False):
        """This instantiates a single storage for the details provided and doesn't check the CS."""
        # The storage name must be supplied.
        if "StorageName" in parameterDict:
            storageName = parameterDict["StorageName"]
        else:
            errStr = "StorageFactory.getStorage: StorageName must be supplied"
            gLogger.error(errStr)
            return S_ERROR(errStr)

        # PluginName must be supplied otherwise nothing will work.
        pluginName = parameterDict.get("PluginName")
        if not pluginName:
            errStr = "StorageFactory.getStorage: PluginName must be supplied"
            gLogger.error(errStr)
            return S_ERROR(errStr)

        return self.__generateStorageObject(storageName, pluginName, parameterDict, hideExceptions=hideExceptions)

    def getStorages(self, storageName, protocolSections=None, hideExceptions=False):
        """Get an instance of a Storage based on the DIRAC SE name based on the CS entries CS

        :param storageName: is the DIRAC SE name i.e. 'CERN-RAW'
        :param protocolSections: is an optional list of protocols if a sub-set is desired i.e ['SRM2','SRM1']

        :return: dictionary containing storage elements and information about them
        """
        self.remoteProtocolSections = []
        self.localProtocolSections = []
        self.name = ""
        self.options = {}
        self.protocols = {}
        self.storages = {}
        if protocolSections is None:
            protocolSections = []
        elif isinstance(protocolSections, str):
            protocolSections = [protocolSections]
        if not self.vo:
            gLogger.warn("No VO information available")

        # Get the name of the storage provided
        res = self._getConfigStorageName(storageName, "Alias")
        if not res["OK"]:
            return res
        storageName = res["Value"]
        self.name = storageName

        # In case the storage is made from a base SE, get this information
        res = self._getConfigStorageName(storageName, "BaseSE")
        if not res["OK"]:
            return res
        # If the storage is derived frmo another one, keep the information
        # We initialize the seConfigPath to SE_BASE_CONFIG_PATH if there is a derivedSE, SE_CONFIG_PATH if not
        if res["Value"] != storageName:
            derivedStorageName = storageName
            storageName = res["Value"]
            seConfigPath = SE_BASE_CONFIG_PATH
        else:
            derivedStorageName = None
            seConfigPath = SE_CONFIG_PATH

        # Get the options defined in the CS for this storage
        res = self._getConfigStorageOptions(
            storageName, derivedStorageName=derivedStorageName, seConfigPath=seConfigPath
        )

        if not res["OK"]:
            return res
        self.options = res["Value"]

        # Get the protocol specific details
        res = self._getConfigStorageProtocols(
            storageName, derivedStorageName=derivedStorageName, seConfigPath=seConfigPath
        )
        if not res["OK"]:
            return res
        self.protocols = res["Value"]

        requestLocalProtocolSections = []
        requestRemoteProtocolSections = []
        requestedProtocolDetails = []
        turlProtocols = []
        # Generate the protocol specific plug-ins
        for protocolSectionName, protocolDetails in self.protocols.items():
            # Type of plugins to use
            pluginName = protocolDetails.get("PluginName", protocolSectionName)
            # If that section is not requested, continue
            if protocolSections and protocolSectionName not in protocolSections:
                continue
            protocol = protocolDetails["Protocol"]
            result = self.__generateStorageObject(
                storageName, pluginName, protocolDetails, hideExceptions=hideExceptions
            )
            if result["OK"]:
                storageObj = result["Value"]
                storageObj.protocolSectionName = protocolSectionName
                self.storages[protocolSectionName] = storageObj
                if protocolSectionName in self.localProtocolSections:
                    turlProtocols.append(protocol)
                    requestLocalProtocolSections.append(protocolSectionName)
                if protocolSectionName in self.remoteProtocolSections:
                    requestRemoteProtocolSections.append(protocolSectionName)
                requestedProtocolDetails.append(protocolDetails)
            else:
                gLogger.info(result["Message"])

        if self.storages:
            resDict = {}
            resDict["StorageName"] = self.name
            resDict["StorageOptions"] = self.options
            resDict["StorageObjects"] = self.storages
            resDict["LocalProtocolSections"] = requestLocalProtocolSections
            resDict["RemoteProtocolSections"] = requestRemoteProtocolSections
            resDict["ProtocolOptions"] = requestedProtocolDetails
            resDict["TurlProtocols"] = turlProtocols
            return S_OK(resDict)
        else:
            errStr = "StorageFactory.getStorages: Failed to instantiate any storage protocols."
            gLogger.error(errStr, self.name)
            return S_ERROR(errStr)

    ###########################################################################################
    #
    # Below are internal methods for obtaining section/option/value configuration
    #

    def _getConfigStorageName(self, storageName, referenceType, seConfigPath=SE_CONFIG_PATH):
        """
        This gets the name of the storage the configuration service.
        If the storage is a reference to another SE the resolution is performed.

        :param storageName: is the storage section to check in the CS
        :param referenceType: corresponds to an option inside the storage section
        :param seConfigPath: the path of the storage section.
                                It can be /Resources/StorageElements or StorageElementBases

        :return: the name of the storage
        """
        configPath = f"{seConfigPath}/{storageName}"
        res = gConfig.getOptions(configPath)
        if not res["OK"]:
            errStr = "StorageFactory._getConfigStorageName: Failed to get storage options"
            gLogger.error(errStr, res["Message"])
            return S_ERROR(errStr)
        if not res["Value"]:
            errStr = "StorageFactory._getConfigStorageName: Supplied storage doesn't exist."
            gLogger.error(errStr, configPath)
            return S_ERROR(errStr)
        if referenceType in res["Value"]:
            configPath = cfgPath(seConfigPath, storageName, referenceType)
            referenceName = gConfig.getValue(configPath)
            # We first look into the BaseStorageElements section.
            # If not, we look into the StorageElements section
            # (contrary to BaseSE, it's OK for an Alias to be in the StorageElements section)
            result = self._getConfigStorageName(referenceName, "Alias", seConfigPath=SE_BASE_CONFIG_PATH)
            if not result["OK"]:
                # Since it is not in the StorageElementBases section, check in the StorageElements section
                result = self._getConfigStorageName(referenceName, "Alias", seConfigPath=SE_CONFIG_PATH)
                if not result["OK"]:
                    return result
            resolvedName = result["Value"]
        else:
            resolvedName = storageName
        return S_OK(resolvedName)

    def _getConfigStorageOptions(self, storageName, derivedStorageName=None, seConfigPath=SE_CONFIG_PATH):
        """
        Get the options associated to the StorageElement as defined in the CS

        :param storageName: is the storage section to check in the CS
        :param seConfigPath: the path of the storage section.
                                It can be /Resources/StorageElements or StorageElementBases
        :param derivedStorageName: is the storage section of a derived storage if it inherits from a base

        :return: options associated to the StorageElement as defined in the CS
        """
        optionsDict = {}

        # We first get the options of the baseSE, and then overwrite with the derivedSE
        for seName in (storageName, derivedStorageName) if derivedStorageName else (storageName,):
            storageConfigPath = cfgPath(seConfigPath, seName)
            res = gConfig.getOptions(storageConfigPath)
            if not res["OK"]:
                errStr = "StorageFactory._getStorageOptions: Failed to get storage options."
                gLogger.error(errStr, f"{seName}: {res['Message']}")
                return S_ERROR(errStr)
            for option in set(res["Value"]) - {"ReadAccess", "WriteAccess", "CheckAccess", "RemoveAccess"}:
                optionConfigPath = cfgPath(storageConfigPath, option)
                default = [] if option in ["VO", "AccessProtocols", "WriteProtocols"] else ""
                optionsDict[option] = gConfig.getValue(optionConfigPath, default)
            # We update the seConfigPath in order to find option in derivedSE now
            seConfigPath = SE_CONFIG_PATH

        # The status is that of the derived SE only
        seName = derivedStorageName if derivedStorageName else storageName
        res = self.resourceStatus.getElementStatus(seName, "StorageElement")
        if not res["OK"]:
            errStr = "StorageFactory._getStorageOptions: Failed to get storage status"
            gLogger.error(errStr, f"{seName}: {res['Message']}")
            return S_ERROR(errStr)

        # For safety, we did not add the ${statusType}Access keys
        # this requires modifications in the StorageElement class

        # We add the dictionary with the statusTypes and values
        # { 'statusType1' : 'status1', 'statusType2' : 'status2' ... }
        optionsDict.update(res["Value"][seName])

        return S_OK(optionsDict)

    def __getProtocolsSections(self, storageName, seConfigPath=SE_CONFIG_PATH):
        """
        Get the protocols of a specific storage section

        :param storageName: is the storage section to check in the CS
        :param seConfigPath: the path of the storage section.
                                It can be /Resources/StorageElements or StorageElementBases

        :return: list of protocol section names
        """
        storageConfigPath = cfgPath(seConfigPath, storageName)
        res = gConfig.getSections(storageConfigPath)
        if not res["OK"]:
            errStr = "StorageFactory._getConfigStorageProtocols: Failed to get storage sections"
            gLogger.error(errStr, f"{storageName}: {res['Message']}")
            return S_ERROR(errStr)
        protocolSections = res["Value"]
        return S_OK(protocolSections)

    def _getConfigStorageProtocols(self, storageName, derivedStorageName=None, seConfigPath=SE_CONFIG_PATH):
        """
        Make a dictionary of protocols with the information associated. Merge with a base SE if it exists

        :param storageName: is the storage section to check in the CS
        :param seConfigPath: the path of the storage section.
                                It can be /Resources/StorageElements or StorageElementBases
        :param derivedStorageName: is the storage section of a derived storage if it inherits from a base

        :return: dictionary of protocols like {protocolSection: {protocolOptions}}
        """

        # Get the sections
        res = self.__getProtocolsSections(storageName, seConfigPath=seConfigPath)
        if not res["OK"]:
            return res
        protocolSections = res["Value"]
        sortedProtocolSections = sorted(protocolSections)

        # Get the details for each section in a dictionary
        for protocolSection in sortedProtocolSections:
            res = self._getConfigStorageProtocolDetails(storageName, protocolSection, seConfigPath=seConfigPath)
            if not res["OK"]:
                return res
            self.protocols[protocolSection] = res["Value"]
        if derivedStorageName:
            # We may have parameters overwriting the baseSE protocols
            res = self.__getProtocolsSections(derivedStorageName, seConfigPath=SE_CONFIG_PATH)
            if not res["OK"]:
                return res
            for protocolSection in res["Value"]:
                res = self._getConfigStorageProtocolDetails(
                    derivedStorageName, protocolSection, seConfigPath=SE_CONFIG_PATH
                )
                if not res["OK"]:
                    return res
                detail = res["Value"]
                # If we found the plugin section from which we inherit
                inheritanceMatched = False
                for baseStorageProtocolSection in protocolSections:
                    if protocolSection == baseStorageProtocolSection:
                        inheritanceMatched = True
                        for key, val in detail.items():
                            if val:
                                self.protocols[protocolSection][key] = val
                        break
                # If not matched, consider it a new protocol
                if not inheritanceMatched:
                    self.protocols[protocolSection] = detail

        for protocolSectionName, protocolDict in self.protocols.items():
            # Now update the local and remote protocol lists.
            # A warning will be given if the Access option is not set to local or remote.
            if protocolDict["Access"].lower() == "remote":
                self.remoteProtocolSections.append(protocolSectionName)
            elif protocolDict["Access"].lower() == "local":
                self.localProtocolSections.append(protocolSectionName)
            else:
                errStr = (
                    "StorageFactory.__getProtocolDetails: The 'Access' option \
        for %s:%s is neither 'local' or 'remote'."
                    % (storageName, protocolSectionName)
                )
                gLogger.warn(errStr)
        return S_OK(self.protocols)

    def _getConfigStorageProtocolDetails(self, storageName, protocolSection, seConfigPath=SE_CONFIG_PATH):
        """
          Parse the contents of the protocol block

        :param storageName: is the storage section to check in the CS
        :param protocolSection: name of the protocol section to find information
        :param seConfigPath: the path of the storage section.
                                  It can be /Resources/StorageElements or StorageElementBases

        :return: dictionary of the protocol options
        """
        # First obtain the options that are available
        protocolConfigPath = cfgPath(seConfigPath, storageName, protocolSection)
        res = gConfig.getOptions(protocolConfigPath)
        if not res["OK"]:
            errStr = "StorageFactory.__getProtocolDetails: Failed to get protocol options."
            gLogger.error(errStr, f"{storageName}: {protocolSection}")
            return S_ERROR(errStr)
        options = res["Value"]

        # We must have certain values internally even if not supplied in CS
        protocolDict = {"Access": "", "Host": "", "Path": "", "Port": "", "Protocol": "", "SpaceToken": "", "WSUrl": ""}
        for option in options:
            configPath = cfgPath(protocolConfigPath, option)
            optionValue = gConfig.getValue(configPath, "")
            protocolDict[option] = optionValue

        # Evaluate the base path taking into account possible VO specific setting
        if self.vo:
            result = gConfig.getOptionsDict(cfgPath(protocolConfigPath, "VOPath"))
            voPath = ""
            if result["OK"]:
                voPath = result["Value"].get(self.vo, "")
            if voPath:
                protocolDict["Path"] = voPath

        return S_OK(protocolDict)

    ###########################################################################################
    #
    # Below is the method for obtaining the object instantiated for a provided storage configuration
    #

    def __generateStorageObject(self, storageName, pluginName, parameters, hideExceptions=False):
        """
        Generate a Storage Element from parameters collected

        :param storageName: is the storage section to check in the CS
        :param pluginName: name of the plugin used. Example: GFAL2_XROOT, GFAL2_SRM2...
        :param parameters: dictionary of protocol details.
        """

        storageType = pluginName
        if self.proxy:
            storageType = "Proxy"

        objectLoader = ObjectLoader()
        result = objectLoader.loadObject(f"Resources.Storage.{storageType}Storage", hideExceptions=hideExceptions)
        if not result["OK"]:
            gLogger.error(f"Failed to load storage object: {result['Message']}")
            return result

        storageClass = result["Value"]
        try:
            storage = storageClass(storageName, parameters)
        except Exception as x:
            errStr = f"StorageFactory._generateStorageObject: Failed to instantiate {storageName}: {x}"
            gLogger.exception(errStr)
            return S_ERROR(errStr)

        return S_OK(storage)
