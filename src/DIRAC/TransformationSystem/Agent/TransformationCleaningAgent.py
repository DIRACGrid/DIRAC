"""TransformationCleaningAgent cleans up finalised transformations.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN TransformationCleaningAgent
  :end-before: ##END
  :dedent: 2
  :caption: TransformationCleaningAgent options

"""
# # imports
import ast
import errno
import os
import re
from datetime import datetime, timedelta

# # from DIRAC
from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.DErrno import cmpError
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.TransformationSystem.Client import TransformationStatus
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.Service.JobPolicy import (
    RIGHT_DELETE,
    RIGHT_KILL,
)
from DIRAC.WorkloadManagementSystem.DB.StatusUtils import kill_delete_jobs

# # agent's name
AGENT_NAME = "Transformation/TransformationCleaningAgent"


class TransformationCleaningAgent(AgentModule):
    """
    .. class:: TransformationCleaningAgent

    :param ~DIRAC.DataManagementSystem.Client.DataManager.DataManager dm: DataManager instance
    :param ~TransformationClient.TransformationClient transClient: TransformationClient instance
    :param ~FileCatalogClient.FileCatalogClient metadataClient: FileCatalogClient instance

    """

    def __init__(self, *args, **kwargs):
        """c'tor"""
        AgentModule.__init__(self, *args, **kwargs)

        self.shifterProxy = None

        # # transformation client
        self.transClient = None
        # # request client
        self.reqClient = None
        # # file catalog client
        self.metadataClient = None
        # # JobDB
        self.jobDB = None

        # # transformations types
        self.transformationTypes = None
        # # directory locations
        self.directoryLocations = ["TransformationDB", "MetadataCatalog"]
        # # transformation metadata
        self.transfidmeta = "TransformationID"
        # # archive periof in days
        self.archiveAfter = 7
        # # transformation log SEs
        self.logSE = "LogSE"
        # # enable/disable execution
        self.enableFlag = "True"

        self.dataProcTTypes = ["MCSimulation", "Merge"]
        self.dataManipTTypes = ["Replication", "Removal"]

    def initialize(self):
        """agent initialisation

        reading and setting config opts

        :param self: self reference
        """
        # # shifter proxy
        # See cleanContent method: this proxy will be used ALSO when the file catalog used
        # is the DIRAC File Catalog (DFC).
        # This is possible because of unset of the "UseServerCertificate" option
        self.shifterProxy = self.am_getOption("shifterProxy", self.shifterProxy)

        # # transformations types
        self.dataProcTTypes = Operations().getValue("Transformations/DataProcessing", self.dataProcTTypes)
        self.dataManipTTypes = Operations().getValue("Transformations/DataManipulation", self.dataManipTTypes)
        agentTSTypes = self.am_getOption("TransformationTypes", [])
        if agentTSTypes:
            self.transformationTypes = sorted(agentTSTypes)
        else:
            self.transformationTypes = sorted(self.dataProcTTypes + self.dataManipTTypes)
        self.log.info(f"Will consider the following transformation types: {str(self.transformationTypes)}")
        # # directory locations
        self.directoryLocations = sorted(self.am_getOption("DirectoryLocations", self.directoryLocations))
        self.log.info(f"Will search for directories in the following locations: {str(self.directoryLocations)}")
        # # transformation metadata
        self.transfidmeta = self.am_getOption("TransfIDMeta", self.transfidmeta)
        self.log.info(f"Will use {self.transfidmeta} as metadata tag name for TransformationID")
        # # archive periof in days
        self.archiveAfter = self.am_getOption("ArchiveAfter", self.archiveAfter)  # days
        self.log.info("Will archive Completed transformations after %d days" % self.archiveAfter)
        # # transformation log SEs
        self.logSE = Operations().getValue("/LogStorage/LogSE", self.logSE)
        self.log.info(f"Will remove logs found on storage element: {self.logSE}")

        # # transformation client
        self.transClient = TransformationClient()
        # # request client
        self.reqClient = ReqClient()
        # # file catalog client
        self.metadataClient = FileCatalogClient()
        # # job DB
        self.jobDB = JobDB()

        return S_OK()

    #############################################################################
    def execute(self):
        """execution in one agent's cycle

        :param self: self reference
        """

        self.enableFlag = self.am_getOption("EnableFlag", self.enableFlag)
        if self.enableFlag != "True":
            self.log.info("TransformationCleaningAgent is disabled by configuration option EnableFlag")
            return S_OK("Disabled via CS flag")

        # Obtain the transformations in Cleaning status and remove any mention of the jobs/files
        res = self.transClient.getTransformations(
            {"Status": TransformationStatus.CLEANING, "Type": self.transformationTypes}
        )
        if res["OK"]:
            for transDict in res["Value"]:
                if self.shifterProxy:
                    self._executeClean(transDict)
                else:
                    self.log.info(
                        f"Cleaning transformation {transDict['TransformationID']} with {transDict['Author']}, {transDict['AuthorGroup']}"
                    )
                    executeWithUserProxy(self._executeClean)(
                        transDict, proxyUserName=transDict["Author"], proxyUserGroup=transDict["AuthorGroup"]
                    )
        else:
            self.log.error("Failed to get transformations", res["Message"])

        # Obtain the transformations in RemovingFiles status and removes the output files
        res = self.transClient.getTransformations({"Status": "RemovingFiles", "Type": self.transformationTypes})
        if res["OK"]:
            for transDict in res["Value"]:
                if self.shifterProxy:
                    self._executeRemoval(transDict)
                else:
                    self.log.info(
                        "Removing files for transformation %(TransformationID)s with %(Author)s, %(AuthorGroup)s"
                        % transDict
                    )
                    executeWithUserProxy(self._executeRemoval)(
                        transDict, proxyUserName=transDict["Author"], proxyUserGroup=transDict["AuthorGroup"]
                    )
        else:
            self.log.error("Could not get the transformations", res["Message"])

        # Obtain the transformations in Completed status and archive if inactive for X days
        olderThanTime = datetime.utcnow() - timedelta(days=self.archiveAfter)
        res = self.transClient.getTransformations(
            {"Status": TransformationStatus.COMPLETED, "Type": self.transformationTypes},
            older=olderThanTime,
            timeStamp="LastUpdate",
        )
        if res["OK"]:
            for transDict in res["Value"]:
                if self.shifterProxy:
                    self._executeArchive(transDict)
                else:
                    self.log.info(
                        "Archiving files for transformation %(TransformationID)s with %(Author)s, %(AuthorGroup)s"
                        % transDict
                    )
                    executeWithUserProxy(self._executeArchive)(
                        transDict, proxyUserName=transDict["Author"], proxyUserGroup=transDict["AuthorGroup"]
                    )
        else:
            self.log.error("Could not get the transformations", res["Message"])
        return S_OK()

    def finalize(self):
        """Only at finalization: will clean ancient transformations (remnants)

            1) get the transformation IDs of jobs that are older than 1 year
            2) find the status of those transformations. Those "Cleaned" and "Archived" will be
               cleaned and archived (again)

        Why doing this here? Basically, it's a race:

        1) the production manager submits a transformation
        2) the TransformationAgent, and a bit later the WorkflowTaskAgent, put such transformation in their internal queue,
           so eventually during their (long-ish) cycle they'll work on it.
        3) 1 minute after creating the transformation, the production manager cleans it (by hand, for whatever reason).
           So, the status is changed to "Cleaning"
        4) the TransformationCleaningAgent cleans what has been created (maybe, nothing),
           then sets the transformation status to "Cleaned" or "Archived"
        5) a bit later the TransformationAgent, and later the WorkflowTaskAgent, kick in,
           creating tasks and jobs for a production that's effectively cleaned (but these 2 agents don't know yet).

        Of course, one could make one final check in TransformationAgent or WorkflowTaskAgent,
        but these 2 agents are already doing a lot of stuff, and are pretty heavy.
        So, we should just clean from time to time.
        What I added here is done only when the agent finalize, and it's quite light-ish operation anyway.
        """
        res = self.jobDB.getDistinctJobAttributes("JobGroup", None, datetime.utcnow() - timedelta(days=365))
        if not res["OK"]:
            self.log.error("Failed to get job groups", res["Message"])
            return res
        transformationIDs = res["Value"]
        if transformationIDs:
            res = self.transClient.getTransformations({"TransformationID": transformationIDs})
            if not res["OK"]:
                self.log.error("Failed to get transformations", res["Message"])
                return res
            transformations = res["Value"]
            toClean = []
            toArchive = []
            for transDict in transformations:
                if transDict["Status"] == TransformationStatus.CLEANED:
                    toClean.append(transDict)
                if transDict["Status"] == TransformationStatus.ARCHIVED:
                    toArchive.append(transDict)

            for transDict in toClean:
                if self.shifterProxy:
                    self._executeClean(transDict)
                else:
                    self.log.info(
                        f"Cleaning transformation {transDict['TransformationID']} with {transDict['Author']}, {transDict['AuthorGroup']}"
                    )
                    executeWithUserProxy(self._executeClean)(
                        transDict, proxyUserName=transDict["Author"], proxyUserGroup=transDict["AuthorGroup"]
                    )

            for transDict in toArchive:
                if self.shifterProxy:
                    self._executeArchive(transDict)
                else:
                    self.log.info(
                        "Archiving files for transformation %(TransformationID)s with %(Author)s, %(AuthorGroup)s"
                        % transDict
                    )
                    executeWithUserProxy(self._executeArchive)(
                        transDict, proxyUserName=transDict["Author"], proxyUserGroup=transDict["AuthorGroup"]
                    )

            # Remove JobIDs that were unknown to the TransformationSystem
            jobGroupsToCheck = [str(transDict["TransformationID"]).zfill(8) for transDict in toClean + toArchive]
            res = self.jobDB.selectJobs({"JobGroup": jobGroupsToCheck})
            if not res["OK"]:
                return res
            jobIDsToRemove = [int(jobID) for jobID in res["Value"]]
            res = self.__removeWMSTasks(jobIDsToRemove)
            if not res["OK"]:
                return res

        return S_OK()

    def _executeClean(self, transDict):
        """Clean transformation."""
        # if transformation is of type `Replication` or `Removal`, there is nothing to clean.
        # We just archive
        if transDict["Type"] in self.dataManipTTypes:
            res = self.archiveTransformation(transDict["TransformationID"])
            if not res["OK"]:
                self.log.error(
                    "Problems archiving transformation", f"{transDict['TransformationID']}: {res['Message']}"
                )
        else:
            res = self.cleanTransformation(transDict["TransformationID"])
            if not res["OK"]:
                self.log.error("Problems cleaning transformation", f"{transDict['TransformationID']}: {res['Message']}")

    def _executeRemoval(self, transDict):
        """Remove files from given transformation."""
        res = self.removeTransformationOutput(transDict["TransformationID"])
        if not res["OK"]:
            self.log.error("Problems removing transformation", f"{transDict['TransformationID']}: {res['Message']}")

    def _executeArchive(self, transDict):
        """Archive the given transformation."""
        res = self.archiveTransformation(transDict["TransformationID"])
        if not res["OK"]:
            self.log.error("Problems archiving transformation", f"{transDict['TransformationID']}: {res['Message']}")

        return S_OK()

    #############################################################################
    #
    # Get the transformation directories for checking
    #

    def getTransformationDirectories(self, transID):
        """get the directories for the supplied transformation from the transformation system.
            These directories are used by removeTransformationOutput and cleanTransformation for removing output.

        :param self: self reference
        :param int transID: transformation ID
        """
        self.log.verbose("Cleaning Transformation directories of transformation", transID)
        directories = []
        if "TransformationDB" in self.directoryLocations:
            res = self.transClient.getTransformationParameters(transID, ["OutputDirectories"])
            if not res["OK"]:
                self.log.error("Failed to obtain transformation directories", res["Message"])
                return res
            transDirectories = []
            if res["Value"]:
                if not isinstance(res["Value"], list):
                    try:
                        transDirectories = ast.literal_eval(res["Value"])
                    except Exception:
                        # It can happen if the res['Value'] is '/a/b/c' instead of '["/a/b/c"]'
                        transDirectories.append(res["Value"])
                else:
                    transDirectories = res["Value"]
            directories = self._addDirs(transID, transDirectories, directories)

        if "MetadataCatalog" in self.directoryLocations:
            res = self.metadataClient.findDirectoriesByMetadata({self.transfidmeta: transID})
            if not res["OK"]:
                self.log.error("Failed to obtain metadata catalog directories", res["Message"])
                return res
            transDirectories = res["Value"]
            directories = self._addDirs(transID, transDirectories, directories)

        if not directories:
            self.log.info("No output directories found")
        directories = sorted(directories)
        return S_OK(directories)

    @classmethod
    def _addDirs(cls, transID, newDirs, existingDirs):
        """append unique :newDirs: list to :existingDirs: list

        :param self: self reference
        :param int transID: transformationID
        :param list newDirs: src list of paths
        :param list existingDirs: dest list of paths
        """
        for folder in newDirs:
            transStr = str(transID).zfill(8)
            if re.search(transStr, str(folder)):
                if folder not in existingDirs:
                    existingDirs.append(os.path.normpath(folder))
        return existingDirs

    #############################################################################
    #
    # These are the methods for performing the cleaning of catalogs and storage
    #

    def cleanContent(self, directory, transID):
        """wipe out everything from catalog under folder :directory:

        :param self: self reference
        :params str directory: folder name
        """
        self.log.verbose("Cleaning Catalog contents")
        res = self.__getCatalogDirectoryContents([directory])
        if not res["OK"]:
            return res
        filesFound = res["Value"]
        if not filesFound:
            self.log.info(f"No files are registered in the catalog directory {directory}")
            return S_OK()
        self.log.info("Attempting to remove possible remnants from the catalog and storage", f"(n={len(filesFound)})")

        return self.__submitRemovalRequests(filesFound, transID)

    def __getCatalogDirectoryContents(self, directories):
        """get catalog contents under paths :directories:

        :param self: self reference
        :param list directories: list of paths in catalog
        """
        self.log.info(f"Obtaining the catalog contents for {len(directories)} directories:")
        for directory in directories:
            self.log.info(directory)
        activeDirs = directories
        allFiles = {}
        fc = FileCatalog()
        while activeDirs:
            currentDir = activeDirs[0]
            res = returnSingleResult(fc.listDirectory(currentDir))
            activeDirs.remove(currentDir)
            if not res["OK"] and "Directory does not exist" in res["Message"]:  # FIXME: DFC should return errno
                self.log.info(f"The supplied directory {currentDir} does not exist")
            elif not res["OK"]:
                if "No such file or directory" in res["Message"]:
                    self.log.info(f"{currentDir}: {res['Message']}")
                else:
                    self.log.error(f"Failed to get directory {currentDir} content", res["Message"])
            else:
                dirContents = res["Value"]
                activeDirs.extend(dirContents["SubDirs"])
                allFiles.update(dirContents["Files"])
        self.log.info("", f"Found {len(allFiles)} files")
        return S_OK(list(allFiles))

    def cleanTransformationLogFiles(self, directory):
        """clean up transformation logs from directory :directory:

        :param self: self reference
        :param str directory: folder name
        """
        self.log.verbose("Removing log files found in the directory", directory)
        res = returnSingleResult(StorageElement(self.logSE).removeDirectory(directory, recursive=True))
        if not res["OK"]:
            if cmpError(res, errno.ENOENT):  # No such file or directory
                self.log.warn("Transformation log directory does not exist", directory)
                return S_OK()
            self.log.error("Failed to remove log files", res["Message"])
            return res
        self.log.info("Successfully removed transformation log directory")
        return S_OK()

    #############################################################################
    #
    # These are the functional methods for archiving and cleaning transformations
    #

    def removeTransformationOutput(self, transID):
        """This just removes any mention of the output data from the catalog and storage"""
        self.log.info(f"Removing output data for transformation {transID}")
        res = self.getTransformationDirectories(transID)
        if not res["OK"]:
            self.log.error("Problem obtaining directories for transformation", f"{transID} with result '{res}'")
            return S_OK()
        directories = res["Value"]
        for directory in directories:
            if not re.search("/LOG/", directory):
                res = self.cleanContent(directory, transID)
                if not res["OK"]:
                    return res

        self.log.info(
            "Removed %d directories from the catalog \
      and its files from the storage for transformation %s"
            % (len(directories), transID)
        )
        # Clean ALL the possible remnants found in the metadata catalog
        res = self.cleanMetadataCatalogFiles(transID)
        if not res["OK"]:
            return res
        self.log.info("Successfully removed output of transformation", transID)
        # Change the status of the transformation to RemovedFiles
        res = self.transClient.setTransformationParameter(transID, "Status", "RemovedFiles")
        if not res["OK"]:
            self.log.error(f"Failed to update status of transformation {transID} to RemovedFiles", res["Message"])
            return res
        self.log.info(f"Updated status of transformation {transID} to RemovedFiles")
        return S_OK()

    def archiveTransformation(self, transID):
        """This just removes job from the jobDB and the transformation DB

        :param self: self reference
        :param int transID: transformation ID
        """
        self.log.info(f"Archiving transformation {transID}")
        # Clean the jobs in the WMS and any failover requests found
        res = self.cleanTransformationTasks(transID)
        if not res["OK"]:
            return res
        # Clean the transformation DB of the files and job information
        res = self.transClient.cleanTransformation(transID)
        if not res["OK"]:
            return res
        self.log.info("Successfully archived transformation %d" % transID)
        # Change the status of the transformation to archived
        res = self.transClient.setTransformationParameter(transID, "Status", "Archived")
        if not res["OK"]:
            self.log.error(f"Failed to update status of transformation {transID} to Archived", res["Message"])
            return res
        self.log.info(f"Updated status of transformation {transID} to Archived")
        return S_OK()

    def cleanTransformation(self, transID):
        """This removes what was produced by the supplied transformation,
        leaving only some info and log in the transformation DB.
        """
        self.log.info("Cleaning transformation", transID)
        res = self.getTransformationDirectories(transID)
        if not res["OK"]:
            self.log.error(
                "Problem obtaining directories for transformation",
                f"{transID} with result '{res['Message']}'",
            )
            return S_OK()
        directories = res["Value"]
        # Clean the jobs in the WMS and any failover requests found
        res = self.cleanTransformationTasks(transID)
        if not res["OK"]:
            return res
        # Clean the log files for the jobs
        for directory in directories:
            if re.search("/LOG/", directory):
                res = self.cleanTransformationLogFiles(directory)
                if not res["OK"]:
                    return res
            res = self.cleanContent(directory, transID)
            if not res["OK"]:
                return res

        # Clean ALL the possible remnants found
        res = self.cleanMetadataCatalogFiles(transID)
        if not res["OK"]:
            return res
        # Clean the transformation DB of the files and job information
        res = self.transClient.cleanTransformation(transID)
        if not res["OK"]:
            return res
        self.log.info("Successfully cleaned transformation", transID)
        res = self.transClient.setTransformationParameter(transID, "Status", "Cleaned")
        if not res["OK"]:
            self.log.error(f"Failed to update status of transformation {transID} to Cleaned", res["Message"])
            return res
        self.log.info("Updated status of transformation", f"{transID} to Cleaned")
        return S_OK()

    def cleanMetadataCatalogFiles(self, transID):
        """wipe out files from catalog"""
        res = self.metadataClient.findFilesByMetadata({self.transfidmeta: transID})
        if not res["OK"]:
            return res
        fileToRemove = res["Value"]
        if not fileToRemove:
            self.log.info("No files found for transID", transID)
            return S_OK()

        return self.__submitRemovalRequests(fileToRemove, transID)

    #############################################################################
    #
    # These are the methods for removing the jobs from the WMS and transformation DB
    #

    def cleanTransformationTasks(self, transID):
        """clean tasks from WMS, or from the RMS if it is a DataManipulation transformation"""
        self.log.verbose("Cleaning Transformation tasks of transformation", transID)
        res = self.__getTransformationExternalIDs(transID)
        if not res["OK"]:
            return res
        externalIDs = res["Value"]
        if externalIDs:
            res = self.transClient.getTransformationParameters(transID, ["Type"])
            if not res["OK"]:
                self.log.error("Failed to determine transformation type")
                return res
            transType = res["Value"]
            if transType in self.dataProcTTypes:
                res = self.__removeWMSTasks(externalIDs)
            else:
                res = self.__removeRequests(externalIDs)
            if not res["OK"]:
                return res
        return S_OK()

    def __getTransformationExternalIDs(self, transID):
        """collect all ExternalIDs for transformation :transID:

        :param self: self reference
        :param int transID: transforamtion ID
        """
        res = self.transClient.getTransformationTasks(condDict={"TransformationID": transID})
        if not res["OK"]:
            self.log.error("Failed to get externalIDs for transformation %d" % transID, res["Message"])
            return res
        externalIDs = [taskDict["ExternalID"] for taskDict in res["Value"]]
        self.log.info(f"Found {len(externalIDs)} tasks for transformation")
        return S_OK(externalIDs)

    def __removeRequests(self, requestIDs):
        """This will remove requests from the RMS system -"""
        rIDs = [int(int(j)) for j in requestIDs if int(j)]
        for reqID in rIDs:
            self.reqClient.cancelRequest(reqID)

        return S_OK()

    def __removeWMSTasks(self, transJobIDs):
        """delete jobs (mark their status as "JobStatus.DELETED") and their requests from the system

        :param self: self reference
        :param list trasnJobIDs: job IDs
        """
        # Prevent 0 job IDs
        jobIDs = [int(j) for j in transJobIDs if int(j)]
        allRemove = True
        for jobList in breakListIntoChunks(jobIDs, 1000):
            res = kill_delete_jobs(RIGHT_KILL, jobList, force=True)
            if res["OK"]:
                self.log.info(f"Successfully killed {len(jobList)} jobs from WMS")
            elif ("InvalidJobIDs" in res) and ("NonauthorizedJobIDs" not in res) and ("FailedJobIDs" not in res):
                self.log.info("Found jobs which did not exist in the WMS", f"(n={len(res['InvalidJobIDs'])})")
            elif "NonauthorizedJobIDs" in res:
                self.log.error("Failed to kill jobs because not authorized", f"(n={len(res['NonauthorizedJobIDs'])})")
                allRemove = False
            elif "FailedJobIDs" in res:
                self.log.error("Failed to kill jobs", f"(n={len(res['FailedJobIDs'])})")
                allRemove = False

            res = kill_delete_jobs(RIGHT_DELETE, jobList, force=True)
            if res["OK"]:
                self.log.info("Successfully deleted jobs from WMS", f"(n={len(jobList)})")
            elif ("InvalidJobIDs" in res) and ("NonauthorizedJobIDs" not in res) and ("FailedJobIDs" not in res):
                self.log.info("Found jobs which did not exist in the WMS", f"(n={len(res['InvalidJobIDs'])})")
            elif "NonauthorizedJobIDs" in res:
                self.log.error("Failed to delete jobs because not authorized", f"(n={len(res['NonauthorizedJobIDs'])})")
                allRemove = False
            elif "FailedJobIDs" in res:
                self.log.error("Failed to delete jobs", f"(n={len(res['FailedJobIDs'])})")
                allRemove = False

        if not allRemove:
            return S_ERROR("Failed to delete all remnants from WMS")
        self.log.info("Successfully deleted all tasks from the WMS")

        if not jobIDs:
            self.log.info("JobIDs not present, unable to delete associated requests.")
            return S_OK()

        failed = 0
        failoverRequests = {}
        res = self.reqClient.getRequestIDsForJobs(jobIDs)
        if not res["OK"]:
            self.log.error("Failed to get requestID for jobs.", res["Message"])
            return res
        failoverRequests.update(res["Value"]["Successful"])
        if not failoverRequests:
            return S_OK()
        for jobID, requestID in res["Value"]["Successful"].items():
            # Put this check just in case, tasks must have associated jobs
            if jobID == 0 or jobID == "0":
                continue
            res = self.reqClient.cancelRequest(requestID)
            if not res["OK"]:
                self.log.error("Failed to remove request from RequestDB", res["Message"])
                failed += 1
            else:
                self.log.verbose("Removed request %s associated to job %d." % (requestID, jobID))

        if failed:
            self.log.info("Successfully removed requests", "(n=%d)" % (len(failoverRequests) - failed))
            self.log.info("Failed to remove requests", "(n=%d)" % failed)
            return S_ERROR("Failed to remove all the request from RequestDB")
        self.log.info("Successfully removed all the associated failover requests")
        return S_OK()

    def __submitRemovalRequests(self, lfns, transID=0):
        """Create removal requests for given lfns.

        :param list lfns: list of lfns to be removed
        :param int transID: transformationID, only used in RequestName
        :returns: S_ERROR/S_OK
        """

        # These imports are used only in this function
        import time
        from hashlib import md5

        for index, lfnList in enumerate(breakListIntoChunks(lfns, 300)):
            oRequest = Request()
            requestName = f"TCA_{transID}_{index}_{md5(repr(time.time()).encode()).hexdigest()[:5]}"
            oRequest.RequestName = requestName
            oOperation = Operation()
            oOperation.Type = "RemoveFile"
            oOperation.TargetSE = "All"
            resMeta = self.metadataClient.getFileMetadata(lfnList)
            if not resMeta["OK"]:
                self.log.error("Cannot get file metadata", resMeta["Message"])
                return resMeta
            if resMeta["Value"]["Failed"]:
                self.log.warning(
                    "Could not get the file metadata of the following, so skipping them:", resMeta["Value"]["Failed"]
                )

            for lfn, lfnInfo in resMeta["Value"]["Successful"].items():
                rarFile = File()
                rarFile.LFN = lfn
                rarFile.ChecksumType = "ADLER32"
                rarFile.Size = lfnInfo["Size"]
                rarFile.Checksum = lfnInfo["Checksum"]
                rarFile.GUID = lfnInfo["GUID"]
                oOperation.addFile(rarFile)

            oRequest.addOperation(oOperation)
            isValid = RequestValidator().validate(oRequest)
            if not isValid["OK"]:
                self.log.error("Request is not valid:", isValid["Message"])
                return isValid
            result = self.reqClient.putRequest(oRequest)
            if not result["OK"]:
                self.log.error("Failed to submit Request: ", result["Message"])
                return result
            self.log.info(
                "RemoveFiles request %d submitted for %d LFNs" % (result["Value"], len(resMeta["Value"]["Successful"]))
            )

        # after the for loop
        return S_OK()
