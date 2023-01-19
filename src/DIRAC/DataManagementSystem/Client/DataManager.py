"""
:mod: DataManager

.. module: DataManager

:synopsis: DataManager links the functionalities of StorageElement and FileCatalog.

This module consists of DataManager and related classes.

"""
# # imports
from datetime import datetime, timedelta
import fnmatch
import os
import time
import errno

# # from DIRAC
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities import DErrno
from DIRAC.Core.Utilities.Adler import fileAdler, compareAdler
from DIRAC.Core.Utilities.File import makeGuid, getSize
from DIRAC.Core.Utilities.List import randomize, breakListIntoChunks
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.MonitoringSystem.Client.DataOperationSender import DataOperationSender
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

# # RSCID
def _isOlderThan(stringTime, days):
    """Check if a time stamp is older than a given number of days"""
    timeDelta = timedelta(days=days)
    maxCTime = datetime.utcnow() - timeDelta
    # st = time.strptime( stringTime, "%a %b %d %H:%M:%S %Y" )
    # cTimeStruct = datetime( st[0], st[1], st[2], st[3], st[4], st[5], st[6], None )
    cTimeStruct = stringTime
    if cTimeStruct < maxCTime:
        return True
    return False


def _initialiseAccountingDict(operation, se, files):
    """create Accounting/Monitoring record"""
    accountingDict = {}
    accountingDict["OperationType"] = operation
    result = getProxyInfo()
    if not result["OK"]:
        userName = "system"
    else:
        userName = result["Value"].get("username", "unknown")
    accountingDict["User"] = userName
    accountingDict["Protocol"] = "DataManager"
    accountingDict["RegistrationTime"] = 0.0
    accountingDict["RegistrationOK"] = 0
    accountingDict["RegistrationTotal"] = 0
    accountingDict["Destination"] = se
    accountingDict["TransferTotal"] = files
    accountingDict["TransferOK"] = files
    accountingDict["TransferSize"] = files
    accountingDict["TransferTime"] = 0.0
    accountingDict["FinalStatus"] = "Successful"
    accountingDict["Source"] = DIRAC.siteName()
    return accountingDict


class DataManager:
    """
    .. class:: DataManager

    A DataManager is taking all the actions that impact or require the FileCatalog and the StorageElement together
    """

    def __init__(self, catalogs=None, masterCatalogOnly=False, vo=False):
        """c'tor

        :param self: self reference
        :param catalogs: the list of catalog in which to perform the operations. This
                        list will be ignored if masterCatalogOnly is set to True
        :param masterCatalogOnly: if set to True, the operations will be performed only on the master catalog.
                                  The catalogs parameter will be ignored.
        :param vo: the VO for which the DataManager is created, get VO from the current proxy if not specified
        """
        self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.voName = vo

        if catalogs is None:
            catalogs = []
        catalogsToUse = FileCatalog(vo=self.voName).getMasterCatalogNames()["Value"] if masterCatalogOnly else catalogs

        self.fileCatalog = FileCatalog(catalogs=catalogsToUse, vo=self.voName)
        self.accountingClient = None
        self.resourceStatus = ResourceStatus()
        self.ignoreMissingInFC = Operations(vo=self.voName).getValue("DataManagement/IgnoreMissingInFC", False)
        self.useCatalogPFN = Operations(vo=self.voName).getValue("DataManagement/UseCatalogPFN", True)
        self.dmsHelper = DMSHelpers(vo=vo)
        self.registrationProtocol = self.dmsHelper.getRegistrationProtocols()
        self.thirdPartyProtocols = self.dmsHelper.getThirdPartyProtocols()
        self.dataOpSender = DataOperationSender()

    def setAccountingClient(self, client):
        """Set Accounting Client instance"""
        self.accountingClient = client

    def __hasAccess(self, opType, path):
        """Check if we have permission to execute given operation on the given file (if exists) or its directory"""
        if isinstance(path, str):
            paths = [path]
        else:
            paths = list(path)

        res = self.fileCatalog.hasAccess(paths, opType)
        if not res["OK"]:
            return res
        result = {"Successful": list(), "Failed": list()}
        for path in paths:
            isAllowed = res["Value"]["Successful"].get(path, False)
            if isAllowed:
                result["Successful"].append(path)
            else:
                result["Failed"].append(path)
        return S_OK(result)

    ##########################################################################
    #
    # These are the bulk removal methods
    #

    def cleanLogicalDirectory(self, lfnDir):
        """Clean the logical directory from the catalog and storage"""
        log = self.log.getSubLogger("cleanLogicalDirectory")
        if isinstance(lfnDir, str):
            lfnDir = [lfnDir]
        retDict = {"Successful": {}, "Failed": {}}
        for folder in lfnDir:
            res = self.__cleanDirectory(folder)
            if not res["OK"]:
                log.debug("Failed to clean directory.", "{} {}".format(folder, res["Message"]))
                retDict["Failed"][folder] = res["Message"]
            else:
                log.debug("Successfully removed directory.", folder)
                retDict["Successful"][folder] = res["Value"]
        return S_OK(retDict)

    def __cleanDirectory(self, folder):
        """delete all files from directory :folder: in FileCatalog and StorageElement

        :param self: self reference
        :param str folder: directory name
        """
        log = self.log.getSubLogger("__cleanDirectory")
        res = self.__hasAccess("removeDirectory", folder)
        if not res["OK"]:
            return res
        if folder not in res["Value"]["Successful"]:
            errStr = "Write access not permitted for this credential."
            log.debug(errStr, folder)
            return S_ERROR(errStr)

        res = returnSingleResult(self.fileCatalog.getDirectoryDump([folder]))

        if not res["OK"]:
            return res

        if not (res["Value"]["Files"] or res["Value"]["SubDirs"]):
            # folder is empty, just remove it and return
            res = returnSingleResult(self.fileCatalog.removeDirectory(folder, recursive=True))
            return res

        # create a list of folders so that empty folders are also deleted
        listOfFolders = res["Value"]["SubDirs"]
        listOfFiles = res["Value"]["Files"]

        res = self.removeFile(listOfFiles)
        if not res["OK"]:
            return res
        for lfn, reason in res["Value"]["Failed"].items():  # can be an iterator
            log.error("Failed to remove file found in the catalog", f"{lfn} {reason}")
        res = returnSingleResult(self.removeFile(["%s/dirac_directory" % folder]))
        if not res["OK"]:
            if not DErrno.cmpError(res, errno.ENOENT):
                log.warn("Failed to delete dirac_directory placeholder file")

        storageElements = gConfig.getValue("Resources/StorageElementGroups/SE_Cleaning_List", [])
        failed = False
        for storageElement in sorted(storageElements):
            res = self.__removeStorageDirectory(folder, storageElement)
            if not res["OK"]:
                failed = True
        if failed:
            return S_ERROR("Failed to clean storage directory at all SEs")

        for aFolder in sorted(listOfFolders, reverse=True):
            res = returnSingleResult(self.fileCatalog.removeDirectory(aFolder, recursive=True))
            log.verbose("Removed folder", f"{aFolder}: {res}")
            if not res["OK"]:
                return res

        res = returnSingleResult(self.fileCatalog.removeDirectory(folder, recursive=True))
        if not res["OK"]:
            return res
        return S_OK()

    def __removeStorageDirectory(self, directory, storageElement):
        """delete SE directory

        :param self: self reference
        :param str directory: folder to be removed
        :param str storageElement: DIRAC SE name
        """

        se = StorageElement(storageElement, vo=self.voName)
        res = returnSingleResult(se.exists(directory))

        log = self.log.getSubLogger("__removeStorageDirectory")
        if not res["OK"]:
            log.debug("Failed to obtain existance of directory", res["Message"])
            return res

        exists = res["Value"]
        if not exists:
            log.debug(f"The directory {directory} does not exist at {storageElement} ")
            return S_OK()

        res = returnSingleResult(se.removeDirectory(directory, recursive=True))
        if not res["OK"]:
            log.debug("Failed to remove storage directory", res["Message"])
            return res

        log.debug(
            "Successfully removed %d files from %s at %s" % (res["Value"]["FilesRemoved"], directory, storageElement)
        )
        return S_OK()

    def getReplicasFromDirectory(self, directory):
        """get all replicas from a given directory

        :param self: self reference
        :param mixed directory: list of directories or one directory
        """
        if isinstance(directory, str):
            directories = [directory]
        else:
            directories = directory
        res = returnSingleResult(self.fileCatalog.getDirectoryDump(directories))
        if not res["OK"]:
            return res

        lfns = res["Value"]["Files"]
        res = self.fileCatalog.getReplicas(lfns, allStatus=True)
        if not res["OK"]:
            return res
        res["Value"] = res["Value"]["Successful"]
        return res

    def getFilesFromDirectory(self, directory, days=0, wildcard="*"):
        """get all files from :directory: older than :days: days matching to :wildcard:

        :param self: self reference
        :param mixed directory: list of directories or directory name
        :param int days: ctime days
        :param str wildcard: pattern to match
        """
        if isinstance(directory, str):
            directories = [directory]
        else:
            directories = directory
        log = self.log.getSubLogger("getFilesFromDirectory")
        log.debug("Obtaining the files older than %d days in %d directories:" % (days, len(directories)))
        for folder in directories:
            log.debug(folder)
        activeDirs = directories
        allFiles = []
        while len(activeDirs) > 0:
            currentDir = activeDirs[0]
            # We only need the metadata (verbose) if a limit date is given
            res = returnSingleResult(self.fileCatalog.listDirectory(currentDir, verbose=(days != 0)))
            activeDirs.remove(currentDir)
            if not res["OK"]:
                log.debug("Error retrieving directory contents", "{} {}".format(currentDir, res["Message"]))
            else:
                dirContents = res["Value"]
                subdirs = dirContents["SubDirs"]
                files = dirContents["Files"]
                log.debug("%s: %d files, %d sub-directories" % (currentDir, len(files), len(subdirs)))
                for subdir in subdirs:
                    if (not days) or _isOlderThan(subdirs[subdir]["CreationDate"], days):
                        if subdir[0] != "/":
                            subdir = currentDir + "/" + subdir
                        activeDirs.append(subdir)
                for fileName in files:
                    fileInfo = files[fileName]
                    fileInfo = fileInfo.get("Metadata", fileInfo)
                    if (not days) or not fileInfo.get("CreationDate") or _isOlderThan(fileInfo["CreationDate"], days):
                        if wildcard == "*" or fnmatch.fnmatch(fileName, wildcard):
                            fileName = fileInfo.get("LFN", fileName)
                            allFiles.append(fileName)
        return S_OK(allFiles)

    ##########################################################################
    #
    # These are the data transfer methods
    #

    def getFile(self, lfn, destinationDir="", sourceSE=None):
        """Get a local copy of a LFN from Storage Elements.

        'lfn' is the logical file name for the desired file
        """
        log = self.log.getSubLogger("getFile")
        fileMetadata = {}
        if isinstance(lfn, list):
            lfns = lfn
        elif isinstance(lfn, str):
            lfns = [lfn]
        else:
            errStr = "Supplied lfn must be string or list of strings."
            log.debug(errStr)
            return S_ERROR(errStr)
        log.debug("Attempting to get %s files." % len(lfns))
        res = self.getActiveReplicas(lfns, getUrl=False)
        if not res["OK"]:
            return res
        failed = res["Value"]["Failed"]
        lfnReplicas = res["Value"]["Successful"]
        # If some files have replicas, check their metadata
        if lfnReplicas:
            res = self.fileCatalog.getFileMetadata(list(lfnReplicas))
            if not res["OK"]:
                return res
            failed.update(res["Value"]["Failed"])
            fileMetadata = res["Value"]["Successful"]
        successful = {}
        for lfn in fileMetadata:
            res = self.__getFile(lfn, lfnReplicas[lfn], fileMetadata[lfn], destinationDir, sourceSE=sourceSE)
            if not res["OK"]:
                failed[lfn] = res["Message"]
            else:
                successful[lfn] = res["Value"]

        return S_OK({"Successful": successful, "Failed": failed})

    def __getFile(self, lfn, replicas, metadata, destinationDir, sourceSE=None):
        """
        Method actually doing the job to get a file from storage
        """

        log = self.log.getSubLogger("__getFile")
        if not replicas:
            errStr = "No accessible replicas found"
            log.debug(errStr)
            return S_ERROR(errStr)
        # Determine the best replicas
        errTuple = ("No SE", "found")
        if sourceSE is None:
            sortedSEs = self._getSEProximity(replicas)
        else:
            if sourceSE not in replicas:
                return S_ERROR("No replica at %s" % sourceSE)
            else:
                sortedSEs = [sourceSE]

        for storageElementName in sortedSEs:
            se = StorageElement(storageElementName, vo=self.voName)

            res = returnSingleResult(se.getFile(lfn, localPath=os.path.realpath(destinationDir)))

            if not res["OK"]:
                errTuple = (
                    "Error getting file from storage:",
                    "{} from {}, {}".format(lfn, storageElementName, res["Message"]),
                )
                errToReturn = res
            else:
                localFile = os.path.realpath(os.path.join(destinationDir, os.path.basename(lfn)))
                localAdler = fileAdler(localFile)

                if metadata["Size"] != res["Value"]:
                    errTuple = (
                        "Mismatch of sizes:",
                        "downloaded = %d, catalog = %d" % (res["Value"], metadata["Size"]),
                    )
                    errToReturn = S_ERROR(DErrno.EFILESIZE, errTuple[1])

                elif (metadata["Checksum"]) and (not compareAdler(metadata["Checksum"], localAdler)):
                    errTuple = (
                        "Mismatch of checksums:",
                        "downloaded = {}, catalog = {}".format(localAdler, metadata["Checksum"]),
                    )
                    errToReturn = S_ERROR(DErrno.EBADCKS, errTuple[1])
                else:
                    return S_OK(localFile)
            # If we are here, there was an error, log it debug level
            log.debug(errTuple[0], errTuple[1])

        log.verbose("Failed to get local copy from any replicas:", "\n%s %s" % errTuple)

        return errToReturn

    def _getSEProximity(self, replicas):
        """get SE proximity"""
        siteName = DIRAC.siteName()
        self.__filterTapeSEs(replicas)
        localSEs = [se for se in self.dmsHelper.getSEsAtSite(siteName).get("Value", []) if se in replicas]
        countrySEs = []
        countryCode = str(siteName).split(".")[-1]
        res = self.dmsHelper.getSEsAtCountry(countryCode)
        if res["OK"]:
            countrySEs = [se for se in res["Value"] if se in replicas and se not in localSEs]
        sortedSEs = randomize(localSEs) + randomize(countrySEs)
        sortedSEs += randomize(se for se in replicas if se not in sortedSEs)

        return sortedSEs

    def putAndRegister(self, lfn, fileName, diracSE, guid=None, path=None, checksum=None, overwrite=False):
        """Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
        'overwrite' removes file from the file catalogue and SE before attempting upload
        """

        res = self.__hasAccess("addFile", lfn)
        if not res["OK"]:
            return res
        log = self.log.getSubLogger("putAndRegister")
        if lfn not in res["Value"]["Successful"]:
            errStr = "Write access not permitted for this credential."
            log.debug(errStr, lfn)
            return S_ERROR(errStr)

        # Check that the local file exists
        if not os.path.exists(fileName):
            errStr = "Supplied file does not exist."
            log.debug(errStr, fileName)
            return S_ERROR(errStr)
        # If the path is not provided then use the LFN path
        if not path:
            path = os.path.dirname(lfn)
        # Obtain the size of the local file
        size = getSize(fileName)
        if size == 0:
            errStr = "Supplied file is zero size."
            log.debug(errStr, fileName)
            return S_ERROR(errStr)
        # If the GUID is not given, generate it here
        if not guid:
            guid = makeGuid(fileName)
        if not checksum:
            log.debug("Checksum information not provided. Calculating adler32.")
            checksum = fileAdler(fileName)
            # Make another try
            if not checksum:
                log.debug("Checksum calculation failed, try again")
                checksum = fileAdler(fileName)
            if checksum:
                log.debug("Checksum calculated to be %s." % checksum)
            else:
                return S_ERROR(DErrno.EBADCKS, "Unable to calculate checksum")

        res = self.fileCatalog.exists({lfn: guid})
        if not res["OK"]:
            errStr = "Completely failed to determine existence of destination LFN."
            log.debug(errStr, lfn)
            return res
        if lfn not in res["Value"]["Successful"]:
            errStr = "Failed to determine existence of destination LFN."
            log.debug(errStr, lfn)
            return S_ERROR(errStr)
        if res["Value"]["Successful"][lfn]:
            if res["Value"]["Successful"][lfn] == lfn:
                if overwrite:
                    resRm = self.removeFile(lfn, force=True)
                    if not resRm["OK"]:
                        errStr = "Failed to prepare file for overwrite"
                        log.debug(errStr, lfn)
                        return resRm
                    if lfn not in resRm["Value"]["Successful"]:
                        errStr = "Failed to either delete file or LFN"
                        log.debug(errStr, lfn)
                        return S_ERROR(f"{errStr} {lfn}")
                else:
                    errStr = "The supplied LFN already exists in the File Catalog."
                    log.debug(errStr, lfn)
                    return S_ERROR("{} {}".format(errStr, res["Value"]["Successful"][lfn]))
            else:
                # If the returned LFN is different, this is the name of a file
                # with the same GUID
                errStr = "This file GUID already exists for another file"
                log.debug(errStr, res["Value"]["Successful"][lfn])
                return S_ERROR("{} {}".format(errStr, res["Value"]["Successful"][lfn]))

        ##########################################################
        #  Instantiate the destination storage element here.
        storageElement = StorageElement(diracSE, vo=self.voName)
        res = storageElement.isValid()
        if not res["OK"]:
            errStr = "The storage element is not currently valid."
            log.verbose(errStr, "{} {}".format(diracSE, res["Message"]))
            return S_ERROR("{} {}".format(errStr, res["Message"]))

        fileDict = {lfn: fileName}

        successful = {}
        failed = {}
        ##########################################################
        #  Perform the put here.
        startTime = datetime.utcnow()
        transferStartTime = time.time()
        res = returnSingleResult(storageElement.putFile(fileDict))
        putTime = time.time() - transferStartTime

        accountingDict = _initialiseAccountingDict("putAndRegister", diracSE, 1)
        accountingDict["TransferSize"] = size
        accountingDict["TransferTime"] = putTime

        if not res["OK"]:
            # We don't consider it a failure if the SE is not valid
            if not DErrno.cmpError(res, errno.EACCES):

                accountingDict["TransferOK"] = 0
                accountingDict["FinalStatus"] = "Failed"
                sendingResult = self.dataOpSender.sendData(
                    accountingDict, commitFlag=True, startTime=startTime, endTime=datetime.utcnow()
                )

                log.verbose("Committing data operation")
                if not sendingResult["OK"]:
                    log.error("Couldn't commit data operation", sendingResult["Message"])
                    return sendingResult
                log.verbose("Done committing")
                log.debug("putAndRegister: Sending  took %.1f seconds" % (time.time() - transferStartTime))

            errStr = "Failed to put file to Storage Element."
            log.debug(errStr, "{}: {}".format(fileName, res["Message"]))
            return S_ERROR("{} {}".format(errStr, res["Message"]))
        successful[lfn] = {"put": putTime}

        ###########################################################
        # Perform the registration here
        destinationSE = storageElement.storageElementName()
        res = returnSingleResult(storageElement.getURL(lfn, protocol=self.registrationProtocol))
        if not res["OK"]:
            errStr = "Failed to generate destination PFN."
            log.debug(errStr, res["Message"])
            return S_ERROR("{} {}".format(errStr, res["Message"]))
        destUrl = res["Value"]

        fileTuple = (lfn, destUrl, size, destinationSE, guid, checksum)
        registerDict = {
            "LFN": lfn,
            "PFN": destUrl,
            "Size": size,
            "TargetSE": destinationSE,
            "GUID": guid,
            "Addler": checksum,
        }
        startTime = time.time()
        res = self.registerFile(fileTuple)
        registerTime = time.time() - startTime

        accountingDict["RegistrationTotal"] = 1
        accountingDict["RegistrationTime"] = registerTime

        if not res["OK"]:
            errStr = "Completely failed to register file."
            log.debug(errStr, res["Message"])
            failed[lfn] = {"register": registerDict}
            accountingDict["FinalStatus"] = "Failed"

        elif lfn in res["Value"]["Failed"]:
            errStr = "Failed to register file."
            log.debug(errStr, "{} {}".format(lfn, res["Value"]["Failed"][lfn]))
            accountingDict["FinalStatus"] = "Failed"
            failed[lfn] = {"register": registerDict}
        else:
            successful[lfn]["register"] = registerTime
            accountingDict["RegistrationOK"] = 1

        # Send to Monitoring/Accounting
        startTime = time.time()
        sendingResult = self.dataOpSender.sendData(accountingDict, commitFlag=True)
        log.verbose("Committing data operation")
        if not sendingResult["OK"]:
            log.error("Couldn't commit data operation", sendingResult["Message"])
            return sendingResult
        log.verbose("Done committing")
        log.debug("putAndRegister: Sending took %.1f seconds" % (time.time() - startTime))

        return S_OK({"Successful": successful, "Failed": failed})

    def replicateAndRegister(self, lfn, destSE, sourceSE="", destPath="", localCache="", catalog=""):
        """Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
        """
        log = self.log.getSubLogger("replicateAndRegister")
        successful = {}
        failed = {}
        log.debug(f"Attempting to replicate {lfn} to {destSE}.")
        startReplication = time.time()
        res = self.__replicate(lfn, destSE, sourceSE, destPath, localCache)
        replicationTime = time.time() - startReplication
        if not res["OK"]:
            errStr = "Completely failed to replicate file."
            log.debug(errStr, res["Message"])
            return S_ERROR("{} {}".format(errStr, res["Message"]))
        if not res["Value"]:
            # The file was already present at the destination SE
            log.debug(f"{lfn} already present at {destSE}.")
            successful[lfn] = {"replicate": 0, "register": 0}
            resDict = {"Successful": successful, "Failed": failed}
            return S_OK(resDict)
        successful[lfn] = {"replicate": replicationTime}

        destPfn = res["Value"]["DestPfn"]
        destSE = res["Value"]["DestSE"]
        log.debug(f"Attempting to register {destPfn} at {destSE}.")
        replicaTuple = (lfn, destPfn, destSE)
        startRegistration = time.time()
        res = self.registerReplica(replicaTuple, catalog=catalog)
        registrationTime = time.time() - startRegistration
        if not res["OK"]:
            # Need to return to the client that the file was replicated but not
            # registered
            errStr = "Completely failed to register replica."
            log.debug(errStr, res["Message"])
            failed[lfn] = {"Registration": {"LFN": lfn, "TargetSE": destSE, "PFN": destPfn}}
        else:
            if lfn in res["Value"]["Successful"]:
                log.debug("Successfully registered replica.")
                successful[lfn]["register"] = registrationTime
            else:
                errStr = "Failed to register replica."
                log.debug(errStr, res["Value"]["Failed"][lfn])
                failed[lfn] = {"Registration": {"LFN": lfn, "TargetSE": destSE, "PFN": destPfn}}
        return S_OK({"Successful": successful, "Failed": failed})

    def replicate(self, lfn, destSE, sourceSE="", destPath="", localCache=""):
        """Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
        """
        log = self.log.getSubLogger("replicate")
        log.debug(f"Attempting to replicate {lfn} to {destSE}.")
        res = self.__replicate(lfn, destSE, sourceSE, destPath, localCache)
        if not res["OK"]:
            errStr = "Replication failed."
            log.debug(errStr, f"{lfn} {destSE}")
            return res
        if not res["Value"]:
            # The file was already present at the destination SE
            log.debug(f"{lfn} already present at {destSE}.")
            return res
        return S_OK(lfn)

    def __getSERealName(self, storageName):
        """get the base name of an SE possibly defined as an alias"""
        rootConfigPath = "/Resources/StorageElements"
        configPath = f"{rootConfigPath}/{storageName}"
        res = gConfig.getOptions(configPath)
        if not res["OK"]:
            errStr = "Failed to get storage options"
            return S_ERROR(errStr)
        if not res["Value"]:
            errStr = "Supplied storage doesn't exist."
            return S_ERROR(errStr)
        if "Alias" in res["Value"]:
            configPath += "/Alias"
            aliasName = gConfig.getValue(configPath)
            result = self.__getSERealName(aliasName)
            if not result["OK"]:
                return result
            resolvedName = result["Value"]
        else:
            resolvedName = storageName
        return S_OK(resolvedName)

    def __isSEInList(self, seName, seList):
        """Check whether an SE is in a list of SEs... All could be aliases"""
        seSet = set()
        for se in seList:
            res = self.__getSERealName(se)
            if res["OK"]:
                seSet.add(res["Value"])
        return self.__getSERealName(seName).get("Value") in seSet

    def __replicate(self, lfn, destSEName, sourceSEName="", destPath="", localCache=""):
        """Replicate a LFN to a destination SE.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' if cannot do third party transfer, we do get and put through this local directory
        """

        log = self.log.getSubLogger("__replicate")

        ###########################################################
        # Check that we have write permissions to this directory.
        res = self.__hasAccess("addReplica", lfn)
        if not res["OK"]:
            return res
        if lfn not in res["Value"]["Successful"]:
            errStr = "__replicate: Write access not permitted for this credential."
            log.debug(errStr, lfn)
            return S_ERROR(errStr)

        # Check that the destination storage element is sane and resolve its name
        log.debug("Verifying destination StorageElement validity (%s)." % (destSEName))

        destStorageElement = StorageElement(destSEName, vo=self.voName)
        res = destStorageElement.isValid()
        if not res["OK"]:
            errStr = "The storage element is not currently valid."
            log.verbose(errStr, "{} {}".format(destSEName, res["Message"]))
            return S_ERROR("{} {}".format(errStr, res["Message"]))

        # Get the real name of the SE
        destSEName = destStorageElement.storageElementName()

        ###########################################################
        # Check whether the destination storage element is banned
        log.verbose("Determining whether %s ( destination ) is Write-banned." % destSEName)

        if not destStorageElement.status()["Write"]:
            infoStr = "Supplied destination Storage Element is not currently allowed for Write."
            log.debug(infoStr, destSEName)
            return S_ERROR(infoStr)

        # Get the LFN replicas from the file catalog
        log.debug("Attempting to obtain replicas for %s." % (lfn))
        res = returnSingleResult(self.getReplicas(lfn, getUrl=False))
        if not res["OK"]:
            errStr = "Failed to get replicas for LFN."
            log.debug(errStr, "{} {}".format(lfn, res["Message"]))
            return S_ERROR("{} {}".format(errStr, res["Message"]))

        log.debug("Successfully obtained replicas for LFN.")
        lfnReplicas = res["Value"]

        ###########################################################
        # If the file catalog size is zero fail the transfer
        log.debug("Attempting to obtain size for %s." % lfn)
        res = returnSingleResult(self.fileCatalog.getFileSize(lfn))
        if not res["OK"]:
            errStr = "Failed to get size for LFN."
            log.debug(errStr, "{} {}".format(lfn, res["Message"]))
            return S_ERROR("{} {}".format(errStr, res["Message"]))

        catalogSize = res["Value"]

        if catalogSize == 0:
            errStr = "Registered file size is 0."
            log.debug(errStr, lfn)
            return S_ERROR(errStr)

        log.debug("File size determined to be %s." % catalogSize)

        ###########################################################
        # If the LFN already exists at the destination we have nothing to do
        if self.__isSEInList(destSEName, lfnReplicas):
            log.debug("__replicate: LFN is already registered at %s." % destSEName)
            return S_OK()

        ###########################################################
        # If the source is specified, check that it is in the replicas

        if sourceSEName:
            log.debug("Determining whether source Storage Element specified is sane.")

            if sourceSEName not in lfnReplicas:
                errStr = "LFN does not exist at supplied source SE."
                log.error(errStr, f"{lfn} {sourceSEName}")
                return S_ERROR(errStr)

        # If sourceSE is specified, then we consider this one only, otherwise
        # we consider them all

        possibleSourceSEs = [sourceSEName] if sourceSEName else lfnReplicas

        # We sort the possibileSourceSEs with the SEs that are on the same site than the destination first
        # reverse = True because True > False
        possibleSourceSEs = sorted(
            possibleSourceSEs,
            key=lambda x: self.dmsHelper.isSameSiteSE(x, destSEName).get("Value", False),
            reverse=True,
        )

        # In case we manage to find SEs that would work as a source, but we can't negotiate a protocol
        # we will do a get and put using one of this sane SE
        possibleIntermediateSEs = []

        # Take into account the destination path
        if destPath:
            destPath = f"{destPath}/{os.path.basename(lfn)}"
        else:
            destPath = lfn

        for candidateSEName in possibleSourceSEs:

            log.debug("Consider %s as a source" % candidateSEName)

            # Check that the candidate is active
            if not self.__checkSEStatus(candidateSEName, status="Read"):
                log.debug("%s is currently not allowed as a source." % candidateSEName)
                continue
            else:
                log.debug("%s is available for use." % candidateSEName)

            candidateSE = StorageElement(candidateSEName, vo=self.voName)

            # Check that the SE is valid
            res = candidateSE.isValid()
            if not res["OK"]:
                log.verbose(
                    "The storage element is not currently valid.", "{} {}".format(candidateSEName, res["Message"])
                )
                continue
            else:
                log.debug("The storage is currently valid", candidateSEName)

            # Check that the file size corresponds to the one in the FC
            res = returnSingleResult(candidateSE.getFileSize(lfn))
            if not res["OK"]:
                log.debug("could not get fileSize on %s" % candidateSEName, res["Message"])
                continue
            seFileSize = res["Value"]

            if seFileSize != catalogSize:
                log.debug("Catalog size and physical file size mismatch.", f"{catalogSize} {seFileSize}")
                continue
            else:
                log.debug("Catalog size and physical size match")

            res = destStorageElement.negociateProtocolWithOtherSE(candidateSE, protocols=self.thirdPartyProtocols)

            if not res["OK"]:
                log.debug("Error negotiating replication protocol", res["Message"])
                continue

            replicationProtocols = res["Value"]

            if not replicationProtocols:
                possibleIntermediateSEs.append(candidateSE)
                log.debug("No protocol suitable for replication found")
                continue

            log.debug("Found common protocols", replicationProtocols)

            # THIS WOULD NOT WORK IF PROTO == file !!
            # Why did I write that comment ?!

            # We try the protocols one by one
            # That obviously assumes that there is an overlap and not only
            # a compatibility between the  output protocols of the source
            # and the input protocols of the destination.
            # But that is the only way to make sure we are not replicating
            # over ourselves.
            for compatibleProtocol in replicationProtocols:

                # Compare the urls to make sure we are not overwriting
                res = returnSingleResult(candidateSE.getURL(lfn, protocol=compatibleProtocol))
                if not res["OK"]:
                    log.debug("Cannot get sourceURL", res["Message"])
                    continue

                sourceURL = res["Value"]

                destURL = ""
                res = returnSingleResult(destStorageElement.getURL(destPath, protocol=compatibleProtocol))
                if not res["OK"]:

                    # for some protocols, in particular srm
                    # you might get an error because the file does not exist
                    # which is exactly what we want
                    # in that case, we just keep going with the comparison
                    # since destURL will be an empty string
                    if not DErrno.cmpError(res, errno.ENOENT):
                        log.debug("Cannot get destURL", res["Message"])
                        continue
                    log.debug("File does not exist: Expected error for TargetSE !!")
                else:
                    destURL = res["Value"]

                if sourceURL == destURL:
                    log.debug("Same source and destination, give up")
                    continue

                # Attempt the transfer
                res = returnSingleResult(
                    destStorageElement.replicateFile(
                        {destPath: sourceURL}, sourceSize=catalogSize, inputProtocol=compatibleProtocol
                    )
                )

                if not res["OK"]:
                    log.debug("Replication failed", f"{lfn} from {candidateSEName} to {destSEName}.")
                    continue

                log.debug("Replication successful.", res["Value"])

                res = returnSingleResult(destStorageElement.getURL(destPath, protocol=self.registrationProtocol))
                if not res["OK"]:
                    log.debug("Error getting the registration URL", res["Message"])
                    # it's maybe pointless to try the other candidateSEs...
                    continue

                registrationURL = res["Value"]

                return S_OK({"DestSE": destSEName, "DestPfn": registrationURL})

        # If we are here, that means that we could not make a third party transfer.
        # Check if we have some sane SEs from which we could do a get/put

        localDir = os.path.realpath(localCache if localCache else ".")
        localFile = os.path.join(localDir, os.path.basename(lfn))

        log.debug("Will try intermediate transfer from %s sources" % len(possibleIntermediateSEs))

        for candidateSE in possibleIntermediateSEs:

            res = returnSingleResult(candidateSE.getFile(lfn, localPath=localDir))
            if not res["OK"]:
                log.debug("Error getting the file from %s" % candidateSE.name, res["Message"])
                continue

            res = returnSingleResult(destStorageElement.putFile({destPath: localFile}))

            # Remove the local file whatever happened
            try:
                os.remove(localFile)
            except OSError as e:
                log.error("Error removing local file", f"{localFile} {e}")

            if not res["OK"]:
                log.debug("Error putting file coming from %s" % candidateSE.name, res["Message"])
                # if the put is the problem, it's maybe pointless to try the other
                # candidateSEs...
                continue

            # get URL with default protocol to return it
            res = returnSingleResult(destStorageElement.getURL(destPath, protocol=self.registrationProtocol))
            if not res["OK"]:
                log.debug("Error getting the registration URL", res["Message"])
                # it's maybe pointless to try the other candidateSEs...
                continue

            registrationURL = res["Value"]
            return S_OK({"DestSE": destSEName, "DestPfn": registrationURL})

        # If here, we are really doomed
        errStr = "Failed to replicate with all sources."
        log.debug(errStr, lfn)
        return S_ERROR(errStr)

    ###################################################################
    #
    # These are the file catalog write methods
    #

    def registerFile(self, fileTuple, catalog=""):
        """Register a file or a list of files

        :param self: self reference
        :param tuple fileTuple: (lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum )
        :param str catalog: catalog name
        """
        log = self.log.getSubLogger("registerFile")
        if isinstance(fileTuple, (list, set)):
            fileTuples = fileTuple
        elif isinstance(fileTuple, tuple):
            fileTuples = [fileTuple]
        for fileTuple in fileTuples:
            if not isinstance(fileTuple, tuple):
                errStr = "Supplied file info must be tuple or list of tuples."
                log.debug(errStr)
                return S_ERROR(errStr)
        if not fileTuples:
            return S_OK({"Successful": [], "Failed": {}})
        log.debug("Attempting to register %s files." % len(fileTuples))
        res = self.__registerFile(fileTuples, catalog)
        if not res["OK"]:
            errStr = "Completely failed to register files."
            log.debug(errStr, res["Message"])
            return res
        return res

    def __registerFile(self, fileTuples, catalog):
        """register file to catalog"""

        fileDict = {}

        for lfn, physicalFile, fileSize, storageElementName, fileGuid, checksum in fileTuples:
            fileDict[lfn] = {
                "PFN": physicalFile,
                "Size": fileSize,
                "SE": storageElementName,
                "GUID": fileGuid,
                "Checksum": checksum,
            }

        if catalog:
            fileCatalog = FileCatalog(catalog, vo=self.voName)
            if not fileCatalog.isOK():
                return S_ERROR("Can't get FileCatalog %s" % catalog)
        else:
            fileCatalog = self.fileCatalog

        res = fileCatalog.addFile(fileDict)
        if not res["OK"]:
            errStr = "Completely failed to register files."
            self.log.getSubLogger("__registerFile").debug(errStr, res["Message"])

        return res

    def registerReplica(self, replicaTuple, catalog=""):
        """Register a replica (or list of) supplied in the replicaTuples.

        'replicaTuple' is a tuple or list of tuples of the form (lfn,pfn,se)
        """
        log = self.log.getSubLogger("registerReplica")
        if isinstance(replicaTuple, (list, set)):
            replicaTuples = replicaTuple
        elif isinstance(replicaTuple, tuple):
            replicaTuples = [replicaTuple]
        for replicaTuple in replicaTuples:
            if not isinstance(replicaTuple, tuple):
                errStr = "Supplied file info must be tuple or list of tuples."
                log.debug(errStr)
                return S_ERROR(errStr)
        if not replicaTuples:
            return S_OK({"Successful": [], "Failed": {}})
        log.debug("Attempting to register %s replicas." % len(replicaTuples))
        res = self.__registerReplica(replicaTuples, catalog)
        if not res["OK"]:
            errStr = "Completely failed to register replicas."
            log.debug(errStr, res["Message"])
            return res
        return res

    def __registerReplica(self, replicaTuples, catalog):
        """register replica to catalogue"""
        log = self.log.getSubLogger("__registerReplica")
        seDict = {}
        for lfn, url, storageElementName in replicaTuples:
            seDict.setdefault(storageElementName, []).append((lfn, url))
        failed = {}
        replicaTuples = []
        for storageElementName, replicaTuple in seDict.items():  # can be an iterator
            destStorageElement = StorageElement(storageElementName, vo=self.voName)
            res = destStorageElement.isValid()
            if not res["OK"]:
                errStr = "The storage element is not currently valid."
                log.verbose(errStr, "{} {}".format(storageElementName, res["Message"]))
                for lfn, url in replicaTuple:
                    failed[lfn] = errStr
            else:
                storageElementName = destStorageElement.storageElementName()
                for lfn, url in replicaTuple:
                    res = returnSingleResult(destStorageElement.getURL(lfn, protocol=self.registrationProtocol))
                    if not res["OK"]:
                        failed[lfn] = res["Message"]
                    else:
                        replicaTuple = (lfn, res["Value"], storageElementName, False)
                        replicaTuples.append(replicaTuple)
        log.debug("Successfully resolved %s replicas for registration." % len(replicaTuples))
        # HACK!
        replicaDict = {}
        for lfn, url, se, _master in replicaTuples:
            replicaDict[lfn] = {"SE": se, "PFN": url}

        if catalog:
            fileCatalog = FileCatalog(catalog, vo=self.voName)
            res = fileCatalog.addReplica(replicaDict)
        else:
            res = self.fileCatalog.addReplica(replicaDict)
        if not res["OK"]:
            errStr = "Completely failed to register replicas."
            log.debug(errStr, res["Message"])
            return S_ERROR("{} {}".format(errStr, res["Message"]))
        failed.update(res["Value"]["Failed"])
        successful = res["Value"]["Successful"]
        resDict = {"Successful": successful, "Failed": failed}
        return S_OK(resDict)

    ###################################################################
    #
    # These are the removal methods for physical and catalogue removal
    #

    def removeFile(self, lfn, force=None):
        """Remove the file (all replicas) from Storage Elements and file catalogue

        'lfn' is the file to be removed
        """
        log = self.log.getSubLogger("removeFile")
        if not lfn:
            return S_OK({"Successful": {}, "Failed": {}})
        if force is None:
            force = self.ignoreMissingInFC
        if isinstance(lfn, (list, dict, set, tuple)):
            lfns = list(lfn)
        else:
            lfns = [lfn]
        for lfn in lfns:
            if not isinstance(lfn, str):
                errStr = "Supplied lfns must be string or list of strings."
                log.debug(errStr)
                return S_ERROR(errStr)

        successful = {}
        failed = {}
        if not lfns:
            return S_OK({"Successful": successful, "Failed": failed})
        # First check if the file exists in the FC
        res = self.fileCatalog.exists(lfns)
        if not res["OK"]:
            return res
        success = res["Value"]["Successful"]
        lfns = [lfn for lfn in success if success[lfn]]
        if force:
            # Files that don't exist are removed successfully
            successful = dict.fromkeys((lfn for lfn in success if not success[lfn]), True)
        else:
            failed = dict.fromkeys((lfn for lfn in success if not success[lfn]), "No such file or directory")
        # Check that we have write permissions to this directory and to the file.
        if lfns:
            res = self.__hasAccess("removeFile", lfns)
            if not res["OK"]:
                return res
            if res["Value"]["Failed"]:
                errStr = "Write access not permitted for this credential."
                log.debug(errStr, "for %d files" % len(res["Value"]["Failed"]))
                failed.update(dict.fromkeys(res["Value"]["Failed"], errStr))

            lfns = res["Value"]["Successful"]

            if lfns:
                log.debug("Attempting to remove %d files from Storage and Catalogue. Get replicas first" % len(lfns))
                res = self.fileCatalog.getReplicas(lfns, allStatus=True)
                if not res["OK"]:
                    errStr = "DataManager.removeFile: Completely failed to get replicas for lfns."
                    log.debug(errStr, res["Message"])
                    return res
                lfnDict = res["Value"]["Successful"]

                for lfn, reason in res["Value"]["Failed"].items():  # can be an iterator
                    # Ignore files missing in FC if force is set
                    if reason == "No such file or directory" and force:
                        successful[lfn] = True
                    elif reason == "File has zero replicas":
                        lfnDict[lfn] = {}
                    else:
                        failed[lfn] = reason

                res = self.__removeFile(lfnDict)
                if not res["OK"]:
                    # This can never happen
                    return res
                failed.update(res["Value"]["Failed"])
                successful.update(res["Value"]["Successful"])

        self.dataOpSender.concludeSending()
        return S_OK({"Successful": successful, "Failed": failed})

    def __removeFile(self, lfnDict):
        """remove file"""
        storageElementDict = {}
        # # sorted and reversed
        for lfn, repDict in sorted(lfnDict.items(), reverse=True):
            for se in repDict:
                storageElementDict.setdefault(se, []).append(lfn)
        failed = {}
        successful = {}
        for storageElementName in sorted(storageElementDict):
            lfns = storageElementDict[storageElementName]
            res = self.__removeReplica(storageElementName, lfns, replicaDict=lfnDict)
            if not res["OK"]:
                errStr = res["Message"]
                for lfn in lfns:
                    failed[lfn] = failed.setdefault(lfn, "") + " %s" % errStr
            else:
                for lfn, errStr in res["Value"]["Failed"].items():  # can be an iterator
                    failed[lfn] = failed.setdefault(lfn, "") + " %s" % errStr

        completelyRemovedFiles = set(lfnDict) - set(failed)
        if completelyRemovedFiles:
            res = self.fileCatalog.removeFile(list(completelyRemovedFiles))
            if not res["OK"]:
                failed.update(
                    dict.fromkeys(completelyRemovedFiles, "Failed to remove file from the catalog: %s" % res["Message"])
                )
            else:
                failed.update(res["Value"]["Failed"])
                successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def removeReplica(self, storageElementName, lfn):
        """Remove replica at the supplied Storage Element from Storage Element then file catalogue

        'storageElementName' is the storage where the file is to be removed
        'lfn' is the file to be removed
        """
        log = self.log.getSubLogger("removeReplica")
        if isinstance(lfn, (list, dict, set, tuple)):
            lfns = set(lfn)
        else:
            lfns = {lfn}
        for lfn in lfns:
            if not isinstance(lfn, str):
                errStr = "Supplied lfns must be string or list of strings."
                log.debug(errStr)
                return S_ERROR(errStr)

        successful = {}
        failed = {}
        if not lfns:
            return S_OK({"Successful": successful, "Failed": failed})

        # Check that we have write permissions to this file.
        res = self.__hasAccess("removeReplica", lfns)
        if not res["OK"]:
            log.debug("Error in __verifyWritePermisison", res["Message"])
            return res
        if res["Value"]["Failed"]:
            errStr = "Write access not permitted for this credential."
            log.debug(errStr, "for %d files" % len(res["Value"]["Failed"]))
            failed.update(dict.fromkeys(res["Value"]["Failed"], errStr))
            lfns -= set(res["Value"]["Failed"])

        if not lfns:
            log.debug("Permission denied for all files")
        else:
            log.debug(f"Will remove {len(lfns)} lfns at {storageElementName}.")
            res = self.fileCatalog.getReplicas(list(lfns), allStatus=True)
            if not res["OK"]:
                errStr = "Completely failed to get replicas for lfns."
                log.debug(errStr, res["Message"])
                return res
            failed.update(res["Value"]["Failed"])
            replicaDict = res["Value"]["Successful"]
            lfnsToRemove = set()
            for lfn, repDict in replicaDict.items():  # can be an iterator
                if storageElementName not in repDict:
                    # The file doesn't exist at the storage element so don't have to
                    # remove it
                    successful[lfn] = True
                elif len(repDict) == 1:
                    # The file has only a single replica so don't remove
                    log.debug("The replica you are trying to remove is the only one.", f"{lfn} @ {storageElementName}")
                    failed[lfn] = "Failed to remove sole replica"
                else:
                    lfnsToRemove.add(lfn)
            if lfnsToRemove:
                res = self.__removeReplica(storageElementName, lfnsToRemove, replicaDict=replicaDict)
                if not res["OK"]:
                    log.debug("Failed in __removeReplica", res["Message"])
                    return res
                failed.update(res["Value"]["Failed"])
                successful.update(res["Value"]["Successful"])
                self.dataOpSender.concludeSending()
        return S_OK({"Successful": successful, "Failed": failed})

    def __removeReplica(self, storageElementName, lfns, replicaDict=None):
        """remove replica
        Remove the replica from the storageElement, and then from the catalog

        :param storageElementName : The name of the storage Element
        :param lfns : list of lfn we want to remove
        :param replicaDict : cache of fc.getReplicas(lfns) : { lfn { se : catalog url } }

        """
        log = self.log.getSubLogger("__removeReplica")
        failed = {}
        successful = {}
        replicaDict = replicaDict if replicaDict else {}

        lfnsToRemove = set()
        for lfn in lfns:
            res = self.__hasAccess("removeReplica", lfn)
            if not res["OK"]:
                log.debug("Error in __verifyWritePermission", res["Message"])
                return res
            if lfn not in res["Value"]["Successful"]:
                errStr = "Write access not permitted for this credential."
                log.debug(errStr, lfn)
                failed[lfn] = errStr
            else:
                lfnsToRemove.add(lfn)

        # Remove physical replicas first
        res = self.__removePhysicalReplica(storageElementName, lfnsToRemove, replicaDict=replicaDict)
        if not res["OK"]:
            errStr = "Failed to remove physical replicas."
            log.debug(errStr, res["Message"])
            return res
        failed.update(res["Value"]["Failed"])

        # Here we use the FC PFN...
        replicaTuples = [
            (lfn, replicaDict[lfn][storageElementName], storageElementName) for lfn in res["Value"]["Successful"]
        ]
        if replicaTuples:
            res = self.__removeCatalogReplica(replicaTuples)
            if not res["OK"]:
                errStr = "Completely failed to remove physical files."
                log.debug(errStr, res["Message"])
                failed.update(dict.fromkeys((lfn for lfn, _pfn, _se in replicaTuples), res["Message"]))
                successful = {}
            else:
                failed.update(res["Value"]["Failed"])
                successful = res["Value"]["Successful"]
        return S_OK({"Successful": successful, "Failed": failed})

    def removeReplicaFromCatalog(self, storageElementName, lfn):
        """remove :lfn: replica from :storageElementName: SE

        :param self: self reference
        :param str storageElementName: SE name
        :param mixed lfn: a single LFN or list of LFNs
        """
        # FIXME: this method is dangerous ans should eventually be removed as well
        # as the script dirac-dms-remove-catalog-replicas
        log = self.log.getSubLogger("removeReplicaFromCatalog")
        # Remove replica from the file catalog 'lfn' are the file
        # to be removed 'storageElementName' is the storage where the file is to
        # be removed
        if isinstance(lfn, (list, dict, set, tuple)):
            lfns = list(lfn)
        else:
            lfns = [lfn]
        for lfn in lfns:
            if not isinstance(lfn, str):
                errStr = "Supplied lfns must be string or list of strings."
                log.debug(errStr)
                return S_ERROR(errStr)
        successful = {}
        failed = {}
        if not lfns:
            return S_OK({"Successful": successful, "Failed": failed})
        log.debug(f"Will remove catalogue entry for {len(lfns)} lfns at {storageElementName}.")
        res = self.fileCatalog.getReplicas(lfns, allStatus=True)
        if not res["OK"]:
            errStr = "Completely failed to get replicas for lfns."
            log.debug(errStr, res["Message"])
            return res
        failed = {}
        successful = {}
        for lfn, reason in res["Value"]["Failed"].items():  # can be an iterator
            if reason in ("No such file or directory", "File has zero replicas"):
                successful[lfn] = True
            else:
                failed[lfn] = reason
        replicaTuples = []
        for lfn, repDict in res["Value"]["Successful"].items():  # can be an iterator
            if storageElementName not in repDict:
                # The file doesn't exist at the storage element so don't have to remove
                # it
                successful[lfn] = True
            else:
                replicaTuples.append((lfn, repDict[storageElementName], storageElementName))
        log.debug(f"Resolved {len(replicaTuples)} pfns for catalog removal at {storageElementName}.")
        res = self.__removeCatalogReplica(replicaTuples)
        failed.update(res["Value"]["Failed"])
        successful.update(res["Value"]["Successful"])
        resDict = {"Successful": successful, "Failed": failed}
        return S_OK(resDict)

    def __removeCatalogReplica(self, replicaTuples):
        """remove replica form catalogue
        :param replicaTuples : list of (lfn, catalogPFN, se)
        """
        log = self.log.getSubLogger("__removeCatalogReplica")

        startTime = datetime.utcnow()
        registrationStartTime = time.time()
        # HACK!
        replicaDict = {}
        for lfn, pfn, se in replicaTuples:
            replicaDict[lfn] = {"SE": se, "PFN": pfn}
        res = self.fileCatalog.removeReplica(replicaDict)
        endTime = datetime.utcnow()
        accountingDict = _initialiseAccountingDict("removeCatalogReplica", "", len(replicaTuples))
        accountingDict["RegistrationTime"] = time.time() - registrationStartTime

        if not res["OK"]:
            accountingDict["RegistrationOK"] = 0
            accountingDict["FinalStatus"] = "Failed"
            self.dataOpSender.sendData(accountingDict, startTime=startTime, endTime=endTime)

            errStr = "Completely failed to remove replica: "
            log.debug(errStr, res["Message"])
            return S_ERROR("{} {}".format(errStr, res["Message"]))

        success = res["Value"]["Successful"]
        failed = res["Value"]["Failed"]
        for lfn, error in list(failed.items()):
            # Ignore error if file doesn't exist
            # This assumes all catalogs return an error as { catalog : error }
            for catalog, err in list(error.items()):
                if "no such file" in err.lower():
                    success.setdefault(lfn, {}).update({catalog: True})
                    error.pop(catalog)
            if not failed[lfn]:
                failed.pop(lfn)
            else:
                log.error("Failed to remove replica.", f"{lfn} {error}")

        # Only for logging information
        if success:
            log.debug("Removed %d replicas" % len(success))
            for lfn in success:
                log.debug("Successfully removed replica.", lfn)

        accountingDict["RegistrationOK"] = len(success)
        self.dataOpSender.sendData(accountingDict, startTime=startTime, endTime=endTime)
        self.dataOpSender.concludeSending()
        return res

    def __removePhysicalReplica(self, storageElementName, lfnsToRemove, replicaDict=None):
        """remove replica from storage element

        :param storageElementName : name of the storage Element
        :param lfnsToRemove : set of lfn to removes
        :param replicaDict : cache of fc.getReplicas, to be passed to the SE
        """
        log = self.log.getSubLogger("__removePhysicalReplica")
        log.debug(f"Attempting to remove {len(lfnsToRemove)} pfns at {storageElementName}.")
        storageElement = StorageElement(storageElementName, vo=self.voName)
        res = storageElement.isValid()
        if not res["OK"]:
            errStr = "The storage element is not currently valid."
            log.verbose(errStr, "{} {}".format(storageElementName, res["Message"]))
            return S_ERROR("{} {}".format(errStr, res["Message"]))

        startTime = datetime.utcnow()
        transferStartTime = time.time()
        lfnsToRemove = list(lfnsToRemove)
        ret = storageElement.getFileSize(lfnsToRemove, replicaDict=replicaDict)
        deletedSizes = ret.get("Value", {}).get("Successful", {})
        res = storageElement.removeFile(lfnsToRemove, replicaDict=replicaDict)
        endTime = datetime.utcnow()
        accountingDict = _initialiseAccountingDict("removePhysicalReplica", storageElementName, len(lfnsToRemove))
        accountingDict["TransferTime"] = time.time() - transferStartTime

        if not res["OK"]:
            accountingDict["TransferOK"] = 0
            accountingDict["FinalStatus"] = "Failed"
            self.dataOpSender.sendData(accountingDict, startTime=startTime, endTime=endTime)

            log.debug("Failed to remove replicas.", res["Message"])
        else:
            for lfn, value in list(res["Value"]["Failed"].items()):
                if "No such file or directory" in value:
                    res["Value"]["Successful"][lfn] = lfn
                    res["Value"]["Failed"].pop(lfn)
            for lfn in res["Value"]["Successful"]:
                res["Value"]["Successful"][lfn] = True

            deletedSize = sum(deletedSizes.get(lfn, 0) for lfn in res["Value"]["Successful"])

            accountingDict["TransferSize"] = deletedSize
            accountingDict["TransferOK"] = len(res["Value"]["Successful"])
            self.dataOpSender.sendData(accountingDict, startTime=startTime, endTime=endTime)

            infoStr = "Successfully issued accounting removal request."
            log.debug(infoStr)
        self.dataOpSender.concludeSending()
        return res

    #########################################################################
    #
    # File transfer methods
    #

    def put(self, lfn, fileName, diracSE, path=None):
        """Put a local file to a Storage Element

        :param self: self reference
        :param str lfn: LFN
        :param str fileName: the full path to the local file
        :param str diracSE: the Storage Element to which to put the file
        :param str path: the path on the storage where the file will be put (if not provided the LFN will be used)

        """
        log = self.log.getSubLogger("put")
        # Check that the local file exists
        if not os.path.exists(fileName):
            errStr = "Supplied file does not exist."
            log.debug(errStr, fileName)
            return S_ERROR(errStr)
        # If the path is not provided then use the LFN path
        if not path:
            path = os.path.dirname(lfn)
        # Obtain the size of the local file
        size = getSize(fileName)
        if size == 0:
            errStr = "Supplied file is zero size."
            log.debug(errStr, fileName)
            return S_ERROR(errStr)

        ##########################################################
        #  Instantiate the destination storage element here.
        storageElement = StorageElement(diracSE, vo=self.voName)
        res = storageElement.isValid()
        if not res["OK"]:
            errStr = "The storage element is not currently valid."
            log.verbose(errStr, "{} {}".format(diracSE, res["Message"]))
            return S_ERROR("{} {}".format(errStr, res["Message"]))
        fileDict = {lfn: fileName}

        successful = {}
        failed = {}
        ##########################################################
        #  Perform the put here.
        startTime = time.time()
        res = returnSingleResult(storageElement.putFile(fileDict))
        putTime = time.time() - startTime
        if not res["OK"]:
            errStr = "Failed to put file to Storage Element."
            failed[lfn] = res["Message"]
            log.debug(errStr, "{}: {}".format(fileName, res["Message"]))
        else:
            log.debug("Put file to storage in %s seconds." % putTime)
            successful[lfn] = res["Value"]
        resDict = {"Successful": successful, "Failed": failed}
        return S_OK(resDict)

    #########################################################################
    #
    # File catalog methods
    #

    def getActiveReplicas(self, lfns, getUrl=True, diskOnly=False, preferDisk=False):
        """Get all the replicas for the SEs which are in Active status for reading."""
        return self.getReplicas(
            lfns, allStatus=False, getUrl=getUrl, diskOnly=diskOnly, preferDisk=preferDisk, active=True
        )

    def __filterTapeReplicas(self, replicaDict, diskOnly=False):
        """
        Check a replica dictionary for disk replicas:
        If there is a disk replica, removetape replicas, else keep all
        The input argument is modified
        """
        seList = {se for ses in replicaDict["Successful"].values() for se in ses}  # can be an iterator
        # Get a cache of SE statuses for long list of replicas
        seStatus = {
            se: (self.__checkSEStatus(se, status="DiskSE"), self.__checkSEStatus(se, status="TapeSE")) for se in seList
        }
        # Beware, there is a del below
        for lfn, replicas in list(replicaDict["Successful"].items()):
            self.__filterTapeSEs(replicas, diskOnly=diskOnly, seStatus=seStatus)
            # If diskOnly, one may not have any replica in the end, set Failed
            if diskOnly and not replicas:
                del replicaDict["Successful"][lfn]
                replicaDict["Failed"][lfn] = "No disk replicas"
        return

    def __filterReplicasForJobs(self, replicaDict):
        """Remove the SEs that are not to be used for jobs, and archive SEs if there are others
        The input argument is modified
        """
        seList = {se for ses in replicaDict["Successful"].values() for se in ses}  # can be an iterator
        # Get a cache of SE statuses for long list of replicas
        seStatus = {se: (self.dmsHelper.isSEForJobs(se), self.dmsHelper.isSEArchive(se)) for se in seList}
        # Beware, there is a del below
        for lfn, replicas in list(replicaDict["Successful"].items()):
            otherThanArchive = {se for se in replicas if not seStatus[se][1]}
            for se in list(replicas):
                # Remove the SE if it should not be used for jobs or if it is an
                # archive and there are other SEs
                if not seStatus[se][0] or (otherThanArchive and seStatus[se][1]):
                    replicas.pop(se)
            # If in the end there is no replica, set Failed
            if not replicas:
                del replicaDict["Successful"][lfn]
                replicaDict["Failed"][lfn] = "No replicas for jobs"
        return

    def __filterTapeSEs(self, replicas, diskOnly=False, seStatus=None):
        """Remove the tape SEs as soon as there is one disk SE or diskOnly is requested
        The input argument is modified
        """
        # Build the SE status cache if not existing
        if seStatus is None:
            seStatus = {
                se: (self.__checkSEStatus(se, status="DiskSE"), self.__checkSEStatus(se, status="TapeSE"))
                for se in replicas
            }

        for se in replicas:  # There is a del below but we then return!
            # First find a disk replica, otherwise do nothing unless diskOnly is set
            if diskOnly or seStatus[se][0]:
                # There is one disk replica, remove tape replicas and exit loop
                for se in list(replicas):  # Beware: there is a pop below
                    if seStatus[se][1]:
                        replicas.pop(se)
                return
        return

    def checkActiveReplicas(self, replicaDict):
        """
        Check a replica dictionary for active replicas, and verify input structure first
        """
        if not isinstance(replicaDict, dict):
            return S_ERROR("Wrong argument type %s, expected a dictionary" % type(replicaDict))

        for key in ["Successful", "Failed"]:
            if key not in replicaDict:
                return S_ERROR('Missing key "%s" in replica dictionary' % key)
            if not isinstance(replicaDict[key], dict):
                return S_ERROR("Wrong argument type %s, expected a dictionary" % type(replicaDict[key]))

        activeDict = {"Successful": {}, "Failed": replicaDict["Failed"].copy()}
        for lfn, replicas in replicaDict["Successful"].items():  # can be an iterator
            if not isinstance(replicas, dict):
                activeDict["Failed"][lfn] = "Wrong replica info"
            else:
                activeDict["Successful"][lfn] = replicas.copy()
        self.__filterActiveReplicas(activeDict)
        return S_OK(activeDict)

    def __filterActiveReplicas(self, replicaDict):
        """
        Check a replica dictionary for active replicas
        The input dict is modified, no returned value
        """
        seList = {se for ses in replicaDict["Successful"].values() for se in ses}  # can be an iterator
        # Get a cache of SE statuses for long list of replicas
        seStatus = {se: self.__checkSEStatus(se, status="Read") for se in seList}
        for replicas in replicaDict["Successful"].values():  # can be an iterator
            for se in list(replicas):  # Beware: there is a pop below
                if not seStatus[se]:
                    replicas.pop(se)
        return

    def __checkSEStatus(self, se, status="Read"):
        """returns the value of a certain SE status flag (access or other)"""
        return StorageElement(se, vo=self.voName).status().get(status, False)

    def getReplicas(self, lfns, allStatus=True, getUrl=True, diskOnly=False, preferDisk=False, active=False):
        """get replicas from catalogue and filter if requested
        Warning: all filters are independent, hence active and preferDisk should be set if using forJobs
        """
        catalogReplicas = {}
        failed = {}
        for lfnChunk in breakListIntoChunks(lfns, 1000):
            res = self.fileCatalog.getReplicas(lfnChunk, allStatus=allStatus)
            if res["OK"]:
                catalogReplicas.update(res["Value"]["Successful"])
                failed.update(res["Value"]["Failed"])
            else:
                return res
        if not getUrl:
            for lfn in catalogReplicas:
                catalogReplicas[lfn] = dict.fromkeys(catalogReplicas[lfn], True)
        elif not self.useCatalogPFN:
            se_lfn = {}

            # We group the query to getURL by storage element to gain in speed
            for lfn in catalogReplicas:
                for se in catalogReplicas[lfn]:
                    se_lfn.setdefault(se, []).append(lfn)

            for se in se_lfn:
                seObj = StorageElement(se, vo=self.voName)
                succPfn = (
                    seObj.getURL(se_lfn[se], protocol=self.registrationProtocol).get("Value", {}).get("Successful", {})
                )
                for lfn in succPfn:
                    catalogReplicas[lfn][se] = succPfn[lfn]

        result = {"Successful": catalogReplicas, "Failed": failed}
        if active:
            self.__filterActiveReplicas(result)
        if diskOnly or preferDisk:
            self.__filterTapeReplicas(result, diskOnly=diskOnly)
        return S_OK(result)

    def getReplicasForJobs(self, lfns, allStatus=False, getUrl=True, diskOnly=False):
        """get replicas useful for jobs"""
        # Call getReplicas with no filter and enforce filters in this method
        result = self.getReplicas(lfns, allStatus=allStatus, getUrl=getUrl)
        if not result["OK"]:
            return result
        replicaDict = result["Value"]
        # For jobs replicas must be active
        self.__filterActiveReplicas(replicaDict)
        # For jobs, give preference to disk replicas but not only
        self.__filterTapeReplicas(replicaDict, diskOnly=diskOnly)
        # don't use SEs excluded for jobs (e.g. Failover)
        self.__filterReplicasForJobs(replicaDict)
        return S_OK(replicaDict)

    # 3
    # Methods from the catalogToStorage. It would all work with the direct call to the SE, but this checks
    # first if the replica is known to the catalog

    def __executeIfReplicaExists(self, storageElementName, lfn, method, **kwargs):
        """a simple wrapper that allows replica querying then perform the StorageElement operation

        :param self: self reference
        :param str storageElementName: DIRAC SE name
        :param mixed lfn: a LFN str, list of LFNs or dict with LFNs as keys
        """
        log = self.log.getSubLogger("__executeIfReplicaExists")
        # # default value
        kwargs = kwargs if kwargs else {}
        # # get replicas for lfn
        res = FileCatalog(vo=self.voName).getReplicas(lfn)
        if not res["OK"]:
            errStr = "Completely failed to get replicas for LFNs."
            log.debug(errStr, res["Message"])
            return res
        # # returned dict, get failed replicase
        retDict = {"Failed": res["Value"]["Failed"], "Successful": {}}
        # # print errors
        for lfn, reason in retDict["Failed"].items():  # can be an iterator
            log.error("_callReplicaSEFcn: Failed to get replicas for file.", f"{lfn} {reason}")
        # # good replicas
        lfnReplicas = res["Value"]["Successful"]
        # # store PFN to LFN mapping
        lfnList = []
        for lfn, replicas in lfnReplicas.items():  # can be an iterator
            if storageElementName in replicas:
                lfnList.append(lfn)
            else:
                errStr = "File hasn't got replica at supplied Storage Element."
                log.error(errStr, f"{lfn} {storageElementName}")
                retDict["Failed"][lfn] = errStr

        if "replicaDict" not in kwargs:
            kwargs["replicaDict"] = lfnReplicas

        # # call StorageElement function at least
        se = StorageElement(storageElementName, vo=self.voName)
        fcn = getattr(se, method)
        res = fcn(lfnList, **kwargs)
        # # check result
        if not res["OK"]:
            errStr = "Failed to execute %s StorageElement method." % method
            log.error(errStr, res["Message"])
            return res

        # # filter out failed and successful
        retDict["Successful"].update(res["Value"]["Successful"])
        retDict["Failed"].update(res["Value"]["Failed"])

        return S_OK(retDict)

    def getReplicaIsFile(self, lfn, storageElementName):
        """determine whether the supplied lfns are files at the supplied StorageElement

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "isFile")

    def getReplicaSize(self, lfn, storageElementName):
        """get the size of files for the lfns at the supplied StorageElement

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "getFileSize")

    def getReplicaAccessUrl(self, lfn, storageElementName, protocol=False):
        """get the access url for lfns at the supplied StorageElement

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "getURL", protocol=protocol)

    def getReplicaMetadata(self, lfn, storageElementName):
        """get the file metadata for lfns at the supplied StorageElement

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "getFileMetadata")

    def prestageReplica(self, lfn, storageElementName, lifetime=86400):
        """issue a prestage requests for the lfns at the supplied StorageElement

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param int lifetime: 24h in seconds
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "prestageFile", lifetime=lifetime)

    def pinReplica(self, lfn, storageElementName, lifetime=86400):
        """pin the lfns at the supplied StorageElement

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param int lifetime: 24h in seconds
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "pinFile", lifetime=lifetime)

    def releaseReplica(self, lfn, storageElementName):
        """release pins for the lfns at the supplied StorageElement

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "releaseFile")

    def getReplica(self, lfn, storageElementName, localPath=False):
        """copy replicas from DIRAC SE to local directory

        :param self: self reference
        :param mixed lfn: LFN string, list if LFNs or dict with LFNs as keys
        :param str storageElementName: DIRAC SE name
        :param mixed localPath: path in the local file system, if False, os.getcwd() will be used
        :param bool singleFile: execute for the first LFN only
        """
        return self.__executeIfReplicaExists(storageElementName, lfn, "getFile", localPath=localPath)
