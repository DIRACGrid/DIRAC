import copy
import os
import time
from io import StringIO

from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.DErrno import ETSUKN
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Interfaces.API.Job import Job
from DIRAC.TransformationSystem.Client import TransformationFilesStatus
from DIRAC.TransformationSystem.Client.TaskManager import TaskBase
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient


class WorkflowTasks(TaskBase):
    """Handles jobs"""

    def __init__(
        self,
        transClient=None,
        logger=None,
        submissionClient=None,
        jobMonitoringClient=None,
        outputDataModule=None,
        jobClass=None,
        opsH=None,
        destinationPlugin=None,
        owner=None,
        ownerGroup=None,
    ):
        """Generates some default objects.
        jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works:
        VOs can pass in their job class extension, if present
        """

        if not logger:
            logger = gLogger.getSubLogger(self.__class__.__name__)

        super().__init__(transClient, logger)

        useCertificates = bool(bool(owner) and bool(ownerGroup))
        if not submissionClient:
            self.submissionClient = WMSClient(
                useCertificates=useCertificates,
                delegatedDN=getDNForUsername(owner)["Value"][0] if owner else None,
                delegatedGroup=ownerGroup,
            )
        else:
            self.submissionClient = submissionClient

        if not jobMonitoringClient:
            self.jobMonitoringClient = JobMonitoringClient()
        else:
            self.jobMonitoringClient = jobMonitoringClient

        if not jobClass:
            self.jobClass = Job
        else:
            self.jobClass = jobClass

        if not opsH:
            self.opsH = Operations()
        else:
            self.opsH = opsH

        if not outputDataModule:
            self.outputDataModule = self.opsH.getValue("Transformations/OutputDataModule", "")
        else:
            self.outputDataModule = outputDataModule

        if not destinationPlugin:
            self.destinationPlugin = self.opsH.getValue("Transformations/DestinationPlugin", "BySE")
        else:
            self.destinationPlugin = destinationPlugin

        self.destinationPlugin_o = None

        self.outputDataModule_o = None
        self.objectLoader = ObjectLoader()

        self.parametricSequencedKeys = ["JOB_ID", "PRODUCTION_ID", "InputData"]

    def prepareTransformationTasks(self, transBody, taskDict, owner="", ownerGroup="", bulkSubmissionFlag=False):
        """Prepare tasks, given a taskDict, that is created (with some manipulation) by the DB
            jobClass is by default "DIRAC.Interfaces.API.Job.Job". An extension of it also works.


        :param str transBody: transformation job template
        :param dict taskDict: dictionary of per task parameters
        :param str owner: owner of the transformation
        :param str ownerGroup: group of the owner of the transformation
        :param bool bulkSubmissionFlag: flag for using bulk submission or not

        :return: S_OK/S_ERROR with updated taskDict
        """

        if (not owner) or (not ownerGroup):
            res = getProxyInfo(False, False)
            if not res["OK"]:
                return res
            proxyInfo = res["Value"]
            owner = proxyInfo["username"]
            ownerGroup = proxyInfo["group"]

        if bulkSubmissionFlag:
            return self._prepareTasksBulk(transBody, taskDict, owner, ownerGroup)
        # not a bulk submission
        return self._prepareTasks(transBody, taskDict, owner, ownerGroup)

    def _prepareTasksBulk(self, transBody, taskDict, owner, ownerGroup):
        """Prepare transformation tasks with a single job object for bulk submission

        :param str transBody: transformation job template
        :param dict taskDict: dictionary of per task parameters
        :param str owner: owner of the transformation
        :param str ownerGroup: group of the owner of the transformation

        :return: S_OK/S_ERROR with updated taskDict
        """
        if taskDict:
            transID = list(taskDict.values())[0]["TransformationID"]
        else:
            return S_OK({})

        method = "_prepareTasksBulk"
        startTime = time.time()

        # Prepare the bulk Job object with common parameters
        oJob = self.jobClass(transBody)
        self._logVerbose(f"Setting job owner:group to {owner}:{ownerGroup}", transID=transID, method=method)
        oJob.setOwner(owner)
        oJob.setOwnerGroup(ownerGroup)

        try:
            site = oJob.workflow.findParameter("Site").getValue()
        except AttributeError:
            site = None
        jobType = oJob.workflow.findParameter("JobType").getValue()
        transGroup = str(transID).zfill(8)

        # Verify that the JOB_ID parameter is added to the workflow
        if not oJob.workflow.findParameter("JOB_ID"):
            oJob._addParameter(oJob.workflow, "JOB_ID", "string", "00000000", "Initial JOB_ID")

        if oJob.workflow.findParameter("PRODUCTION_ID"):
            oJob._setParamValue("PRODUCTION_ID", str(transID).zfill(8))  # pylint: disable=protected-access
        else:
            oJob._addParameter(
                oJob.workflow,  # pylint: disable=protected-access
                "PRODUCTION_ID",
                "string",
                str(transID).zfill(8),
                "Production ID",
            )
        oJob.setType(jobType)
        self._logVerbose(f"Adding default transformation group of {transGroup}", transID=transID, method=method)
        oJob.setJobGroup(transGroup)

        clinicPath = self._checkSickTransformations(transID)
        if clinicPath:
            self._handleHospital(oJob, clinicPath)

        # Collect per job parameters sequences
        paramSeqDict = {}
        # tasks must be sorted because we use bulk submission and we must find the correspondance
        for taskID in sorted(taskDict):
            paramsDict = taskDict[taskID]
            seqDict = {}

            if site is not None:
                paramsDict["Site"] = site
            paramsDict["JobType"] = jobType

            # Handle destination site
            sites = self._handleDestination(paramsDict)
            if not sites:
                self._logError("Could not get a list a sites", transID=transID, method=method)
                return S_ERROR(ETSUKN, "Can not evaluate destination site")
            self._logVerbose("Setting Site: ", str(sites), transID=transID, method=method)
            seqDict["Site"] = sites

            seqDict["JobName"] = self._transTaskName(transID, taskID)
            seqDict["JOB_ID"] = str(taskID).zfill(8)

            self._logDebug(
                f"TransID: {transID}, TaskID: {taskID}, paramsDict: {str(paramsDict)}",
                transID=transID,
                method=method,
            )

            inputData = self._handleInputsBulk(seqDict, paramsDict, transID)

            outputParameterList = []
            if self.outputDataModule:
                res = self.getOutputData(
                    {
                        "Job": oJob._toXML(),  # pylint: disable=protected-access
                        "TransformationID": transID,
                        "TaskID": taskID,
                        "InputData": inputData,
                    }
                )
                if not res["OK"]:
                    self._logError("Failed to generate output data", res["Message"], transID=transID, method=method)
                    continue
                for name, output in res["Value"].items():
                    seqDict[name] = output
                    outputParameterList.append(name)
                    if oJob.workflow.findParameter(name):
                        oJob._setParamValue(name, "%%(%s)s" % name)  # pylint: disable=protected-access
                    else:
                        oJob._addParameter(
                            oJob.workflow, name, "JDL", "%%(%s)s" % name, name  # pylint: disable=protected-access
                        )

            for pName, seq in seqDict.items():
                paramSeqDict.setdefault(pName, []).append(seq)

        for paramName, paramSeq in paramSeqDict.items():
            if paramName in self.parametricSequencedKeys + outputParameterList:
                res = oJob.setParameterSequence(paramName, paramSeq, addToWorkflow=paramName)
            else:
                res = oJob.setParameterSequence(paramName, paramSeq)
            if not res["OK"]:
                return res

        if taskDict:
            self._logInfo(f"Prepared {len(taskDict)} tasks", transID=transID, method=method, reftime=startTime)

        taskDict["BulkJobObject"] = oJob
        return S_OK(taskDict)

    def _prepareTasks(self, transBody, taskDict, owner, ownerGroup):
        """Prepare transformation tasks with a job object per task

        :param str transBody: transformation job template
        :param dict taskDict: dictionary of per task parameters
        :param owner: owner of the transformation
        :param str ownerGroup: group of the owner of the transformation

        :return:  S_OK/S_ERROR with updated taskDict
        """
        if taskDict:
            transID = list(taskDict.values())[0]["TransformationID"]
        else:
            return S_OK({})

        method = "_prepareTasks"
        startTime = time.time()

        oJobTemplate = self.jobClass(transBody)
        oJobTemplate.setOwner(owner)
        oJobTemplate.setOwnerGroup(ownerGroup)

        try:
            site = oJobTemplate.workflow.findParameter("Site").getValue()
        except AttributeError:
            site = None
        jobType = oJobTemplate.workflow.findParameter("JobType").getValue()
        templateOK = False
        getOutputDataTiming = 0.0

        for taskID, paramsDict in taskDict.items():
            # Create a job for each task and add it to the taskDict
            if not templateOK:
                templateOK = True
                # Update the template with common information
                self._logVerbose(f"Job owner:group to {owner}:{ownerGroup}", transID=transID, method=method)
                transGroup = str(transID).zfill(8)
                self._logVerbose(f"Adding default transformation group of {transGroup}", transID=transID, method=method)
                oJobTemplate.setJobGroup(transGroup)
                if oJobTemplate.workflow.findParameter("PRODUCTION_ID"):
                    oJobTemplate._setParamValue("PRODUCTION_ID", str(transID).zfill(8))
                else:
                    oJobTemplate._addParameter(
                        oJobTemplate.workflow, "PRODUCTION_ID", "string", str(transID).zfill(8), "Production ID"
                    )
                if not oJobTemplate.workflow.findParameter("JOB_ID"):
                    oJobTemplate._addParameter(oJobTemplate.workflow, "JOB_ID", "string", "00000000", "Initial JOB_ID")

            if site is not None:
                paramsDict["Site"] = site
            paramsDict["JobType"] = jobType
            # Now create the job from the template
            oJob = copy.deepcopy(oJobTemplate)
            constructedName = self._transTaskName(transID, taskID)
            self._logVerbose(f"Setting task name to {constructedName}", transID=transID, method=method)
            oJob.setName(constructedName)
            oJob._setParamValue("JOB_ID", str(taskID).zfill(8))
            inputData = None

            self._logDebug(
                f"TransID: {transID}, TaskID: {taskID}, paramsDict: {str(paramsDict)}",
                transID=transID,
                method=method,
            )

            # These helper functions do the real job
            sites = self._handleDestination(paramsDict)
            if not sites:
                self._logError("Could not get a list a sites", transID=transID, method=method)
                paramsDict["TaskObject"] = ""
                continue

            self._logDebug("Setting Site: ", str(sites), transID=transID, method=method)
            res = oJob.setDestination(sites)
            if not res["OK"]:
                self._logError(f"Could not set the site: {res['Message']}", transID=transID, method=method)
                paramsDict["TaskObject"] = ""
                continue

            self._handleInputs(oJob, paramsDict)
            self._handleRest(oJob, paramsDict)

            clinicPath = self._checkSickTransformations(transID)
            if clinicPath:
                self._handleHospital(oJob, clinicPath)

            paramsDict["TaskObject"] = ""
            if self.outputDataModule:
                getOutputDataTiming -= time.time()
                res = self.getOutputData(
                    {"Job": oJob._toXML(), "TransformationID": transID, "TaskID": taskID, "InputData": inputData}
                )
                getOutputDataTiming += time.time()
                if not res["OK"]:
                    self._logError("Failed to generate output data", res["Message"], transID=transID, method=method)
                    continue
                for name, output in res["Value"].items():
                    oJob._addJDLParameter(name, ";".join(output))
            paramsDict["TaskObject"] = oJob
        if taskDict:
            self._logVerbose(
                f"Average getOutputData time: {getOutputDataTiming / len(taskDict):.1f} per task",
                transID=transID,
                method=method,
            )
            self._logInfo(f"Prepared {len(taskDict)} tasks", transID=transID, method=method, reftime=startTime)
        return S_OK(taskDict)

    #############################################################################

    def _handleDestination(self, paramsDict):
        """Handle Sites and TargetSE in the parameters"""

        try:
            sites = ["ANY"]
            if paramsDict["Site"]:
                # 'Site' comes from the XML and therefore is ; separated
                sites = fromChar(paramsDict["Site"], sepChar=";")
        except KeyError:
            pass

        if not self.destinationPlugin_o:
            result = self.objectLoader.loadObject(self.pluginLocation)
            if not result["OK"]:
                self._logFatal("Could not generate a destination plugin object")
                return result
            taskManagerPluginClass = result["Value"]
            self.destinationPlugin_o = taskManagerPluginClass(f"{self.destinationPlugin}", operationsHelper=self.opsH)

        self.destinationPlugin_o.setParameters(paramsDict)
        destSites = self.destinationPlugin_o.run()
        if not destSites:
            return sites

        # Now we need to make the AND with the sites, if defined
        if sites != ["ANY"]:
            # Need to get the AND
            destSites &= set(sites)

        return list(destSites)

    def _handleInputs(self, oJob, paramsDict):
        """set job inputs (+ metadata)"""
        inputData = paramsDict.get("InputData")
        transID = paramsDict["TransformationID"]
        if inputData:
            self._logVerbose(f"Setting input data to {inputData}", transID=transID, method="_handleInputs")
            res = oJob.setInputData(inputData)
            if not res["OK"]:
                self._logError(f"Could not set the inputs: {res['Message']}", transID=transID, method="_handleInputs")

    def _handleInputsBulk(self, seqDict, paramsDict, transID):
        """set job inputs (+ metadata)"""
        method = "_handleInputsBulk"
        if seqDict:
            self._logVerbose(f"Setting job input data to {seqDict}", transID=transID, method=method)

        # Handle Input Data
        inputData = paramsDict.get("InputData")
        if inputData:
            if isinstance(inputData, str):
                inputData = inputData.replace(" ", "").split(";")
            self._logVerbose(f"Setting input data {inputData} to {seqDict}", transID=transID, method=method)
            seqDict["InputData"] = inputData

        for paramName, paramValue in paramsDict.items():
            if paramName not in ("InputData", "Site", "TargetSE"):
                if paramValue:
                    self._logVerbose(f"Setting {paramName} to {paramValue}", transID=transID, method=method)
                    seqDict[paramName] = paramValue

        return inputData

    def _handleRest(self, oJob, paramsDict):
        """add as JDL parameters all the other parameters that are not for inputs or destination"""
        transID = paramsDict["TransformationID"]
        for paramName, paramValue in paramsDict.items():
            if paramName not in ("InputData", "Site", "TargetSE"):
                if paramValue:
                    self._logDebug(f"Setting {paramName} to {paramValue}", transID=transID, method="_handleRest")
                    oJob._addJDLParameter(paramName, paramValue)

    def _checkSickTransformations(self, transID):
        """Check if the transformation is in the transformations to be processed at Hospital or Clinic"""
        transID = int(transID)
        clinicPath = "Hospital"
        if transID in {int(x) for x in self.opsH.getValue(os.path.join(clinicPath, "Transformations"), [])}:
            return clinicPath
        if "Clinics" in self.opsH.getSections("Hospital").get("Value", []):
            basePath = os.path.join("Hospital", "Clinics")
            clinics = self.opsH.getSections(basePath)["Value"]
            for clinic in clinics:
                clinicPath = os.path.join(basePath, clinic)
                if transID in {int(x) for x in self.opsH.getValue(os.path.join(clinicPath, "Transformations"), [])}:
                    return clinicPath
        return None

    def _handleHospital(self, oJob, clinicPath):
        """Optional handle of hospital/clinic jobs"""
        if not clinicPath:
            return
        oJob.setInputDataPolicy("download", dataScheduling=False)

        # Check first for a clinic, if not it must be the general hospital
        hospitalSite = self.opsH.getValue(os.path.join(clinicPath, "ClinicSite"), "")
        hospitalCEs = self.opsH.getValue(os.path.join(clinicPath, "ClinicCE"), [])
        # If not found, get the hospital parameters
        if not hospitalSite:
            hospitalSite = self.opsH.getValue("Hospital/HospitalSite", "DIRAC.JobDebugger.ch")
        if not hospitalCEs:
            hospitalCEs = self.opsH.getValue("Hospital/HospitalCEs", [])

        oJob.setDestination(hospitalSite)
        if hospitalCEs:
            oJob._addJDLParameter("GridCE", hospitalCEs)

    #############################################################################

    def getOutputData(self, paramDict):
        """Get the list of job output LFNs from the provided plugin"""
        if not self.outputDataModule_o:
            result = self.objectLoader.loadObject(self.outputDataModule)
            if not result["OK"]:
                return result
            objectClass = result["Value"]

            self.outputDataModule_o = objectClass()

        # This is the "argument" to the module, set it and then execute
        self.outputDataModule_o.paramDict = paramDict
        return self.outputDataModule_o.execute()

    def submitTransformationTasks(self, taskDict):
        """Submit the tasks"""
        if "BulkJobObject" in taskDict:
            return self.__submitTransformationTasksBulk(taskDict)
        return self.__submitTransformationTasks(taskDict)

    def __submitTransformationTasksBulk(self, taskDict):
        """Submit jobs in one go with one parametric job"""
        if not taskDict:
            return S_OK(taskDict)
        startTime = time.time()

        method = "__submitTransformationTasksBulk"

        oJob = taskDict.pop("BulkJobObject")
        # we can only do this, once the job has been popped, or we _might_ crash
        transID = list(taskDict.values())[0]["TransformationID"]
        if oJob is None:
            self._logError("no bulk Job object found", transID=transID, method=method)
            return S_ERROR(ETSUKN, "No bulk job object provided for submission")

        result = self.submitTaskToExternal(oJob)
        if not result["OK"]:
            self._logError("Failed to submit tasks to external", transID=transID, method=method)
            return result

        jobIDList = result["Value"]
        if len(jobIDList) != len(taskDict):
            for task in taskDict.values():
                task["Success"] = False
            return S_ERROR(ETSUKN, "Submitted less number of jobs than requested tasks")
        # Get back correspondence with tasks sorted by ID
        for jobID, taskID in zip(jobIDList, sorted(taskDict)):
            taskDict[taskID]["ExternalID"] = jobID
            taskDict[taskID]["Success"] = True

        submitted = len(jobIDList)
        self._logInfo(
            "Submitted %d tasks to WMS in %.1f seconds" % (submitted, time.time() - startTime),
            transID=transID,
            method=method,
        )
        return S_OK(taskDict)

    def __submitTransformationTasks(self, taskDict):
        """Submit jobs one by one"""
        method = "__submitTransformationTasks"
        submitted = 0
        failed = 0
        startTime = time.time()
        for task in taskDict.values():
            transID = task["TransformationID"]
            if not task["TaskObject"]:
                task["Success"] = False
                failed += 1
                continue
            res = self.submitTaskToExternal(task["TaskObject"])
            if res["OK"]:
                task["ExternalID"] = res["Value"]
                task["Success"] = True
                submitted += 1
            else:
                self._logError("Failed to submit task to WMS", res["Message"], transID=transID, method=method)
                task["Success"] = False
                failed += 1
        if submitted:
            self._logInfo(
                "Submitted %d tasks to WMS in %.1f seconds" % (submitted, time.time() - startTime),
                transID=transID,
                method=method,
            )
        if failed:
            self._logError("Failed to submit %d tasks to WMS." % (failed), transID=transID, method=method)
        return S_OK(taskDict)

    def submitTaskToExternal(self, job):
        """Submits a single job (which can be a bulk one) to the WMS."""
        if isinstance(job, str):
            try:
                oJob = self.jobClass(job)
            except Exception as x:  # pylint: disable=broad-except
                self._logException("Failed to create job object", "", x)
                return S_ERROR("Failed to create job object")
        elif isinstance(job, self.jobClass):
            oJob = job
        else:
            self._logError("No valid job description found")
            return S_ERROR("No valid job description found")

        workflowFileObject = StringIO(oJob._toXML())
        jdl = oJob._toJDL(jobDescriptionObject=workflowFileObject)
        return self.submissionClient.submitJob(jdl, workflowFileObject)

    def updateTransformationReservedTasks(self, taskDicts):
        transID = None
        jobNames = [self._transTaskName(taskDict["TransformationID"], taskDict["TaskID"]) for taskDict in taskDicts]
        res = self.jobMonitoringClient.getJobs({"JobName": jobNames})
        if not res["OK"]:
            self._logError(
                "Failed to get task from WMS",
                res["Message"],
                transID=transID,
                method="updateTransformationReservedTasks",
            )
            return res
        jobNameIDs = {}
        for wmsID in res["Value"]:
            res = self.jobMonitoringClient.getJobSummary(int(wmsID))
            if not res["OK"]:
                self._logWarn(
                    "Failed to get task summary from WMS",
                    res["Message"],
                    transID=transID,
                    method="updateTransformationReservedTasks",
                )
            else:
                jobNameIDs[res["Value"]["JobName"]] = int(wmsID)

        noTask = list(set(jobNames) - set(jobNameIDs))
        return S_OK({"NoTasks": noTask, "TaskNameIDs": jobNameIDs})

    def getSubmittedTaskStatus(self, taskDicts):
        """
        Check the status of a list of tasks and return lists of taskIDs for each new status
        """
        method = "getSubmittedTaskStatus"

        if taskDicts:
            wmsIDs = [int(taskDict["ExternalID"]) for taskDict in taskDicts if int(taskDict["ExternalID"])]
            transID = taskDicts[0]["TransformationID"]
        else:
            return S_OK({})
        res = self.jobMonitoringClient.getJobsStatus(wmsIDs)
        if not res["OK"]:
            self._logWarn("Failed to get job status from the WMS system", transID=transID, method=method)
            return res
        statusDict = res["Value"]
        updateDict = {}
        for taskDict in taskDicts:
            taskID = taskDict["TaskID"]
            wmsID = int(taskDict["ExternalID"])
            if not wmsID:
                continue
            oldStatus = taskDict["ExternalStatus"]
            newStatus = statusDict.get(wmsID, {}).get("Status", "Removed")
            if oldStatus != newStatus:
                if newStatus == "Removed":
                    self._logVerbose(
                        "Production/Job %d/%d removed from WMS while it is in %s status" % (transID, taskID, oldStatus),
                        transID=transID,
                        method=method,
                    )
                    newStatus = "Failed"
                self._logVerbose(
                    "Setting job status for Production/Job %d/%d to %s" % (transID, taskID, newStatus),
                    transID=transID,
                    method=method,
                )
                updateDict.setdefault(newStatus, []).append(taskID)
        return S_OK(updateDict)

    def getSubmittedFileStatus(self, fileDicts):
        """
        Check the status of a list of files and return the new status of each LFN
        """
        if not fileDicts:
            return S_OK({})

        method = "getSubmittedFileStatus"

        # All files are from the same transformation
        transID = fileDicts[0]["TransformationID"]
        taskFiles = {}
        for fileDict in fileDicts:
            jobName = self._transTaskName(transID, fileDict["TaskID"])
            taskFiles.setdefault(jobName, {})[fileDict["LFN"]] = fileDict["Status"]

        res = self.updateTransformationReservedTasks(fileDicts)
        if not res["OK"]:
            self._logWarn("Failed to obtain taskIDs for files", transID=transID, method=method)
            return res
        noTasks = res["Value"]["NoTasks"]
        taskNameIDs = res["Value"]["TaskNameIDs"]

        updateDict = {}
        for jobName in noTasks:
            for lfn, oldStatus in taskFiles[jobName].items():
                if oldStatus != TransformationFilesStatus.UNUSED:
                    updateDict[lfn] = TransformationFilesStatus.UNUSED

        res = self.jobMonitoringClient.getJobsStatus(list(taskNameIDs.values()))
        if not res["OK"]:
            self._logWarn("Failed to get job status from the WMS system", transID=transID, method=method)
            return res
        statusDict = res["Value"]
        for jobName, wmsID in taskNameIDs.items():
            jobStatus = statusDict.get(wmsID, {}).get("Status")
            newFileStatus = {
                "Done": TransformationFilesStatus.PROCESSED,
                "Completed": TransformationFilesStatus.PROCESSED,
                "Failed": TransformationFilesStatus.UNUSED,
            }.get(jobStatus)
            if newFileStatus:
                for lfn, oldStatus in taskFiles[jobName].items():
                    if newFileStatus != oldStatus:
                        updateDict[lfn] = newFileStatus
        return S_OK(updateDict)
