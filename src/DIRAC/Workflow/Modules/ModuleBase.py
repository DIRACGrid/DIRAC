""" ModuleBase - contains the base class for workflow modules. Defines several common utility methods.

    The modules defined within this package are developed in a way to be executed by a DIRAC.Core.Worfklow.Worfklow.
    In particular, a DIRAC.Core.Workflow.Worfklow object will only call the "execute" function, that is defined here.

    These modules, inspired by the LHCb experience, give the possibility to define simple user and production jobs.
    Many VOs might want to extend this package. And actually, for some cases, it will be necessary. For example,
    defining the LFN output at runtime (within the "UploadOutputs" module is a VO specific operation

    The DIRAC APIs are used to create Jobs that make use of these modules.
"""
import os
import copy

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.TransformationSystem.Client.FileReport import FileReport
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
from DIRAC.DataManagementSystem.Client.DataManager import DataManager


class ModuleBase:
    """Base class for Modules - works only within DIRAC workflows

    This module, inheriting by "object", can use cooperative methods, very useful here.
    """

    #############################################################################

    def __init__(self, loggerIn=None):
        """Initialization of module base.

        loggerIn is a logger object that can be passed so that the logging will be more clear.
        """

        if not loggerIn:
            self.log = gLogger.getSubLogger(self.__class__.__name__)
        else:
            self.log = loggerIn

        # These 2 are used in many places, so it's good to have them available here.
        self.opsH = Operations()
        self.dm = DataManager()

        # Some job parameters
        self.production_id = 0
        self.prod_job_id = 0
        self.jobID = 0
        self.step_number = 0
        self.step_id = 0
        self.jobType = ""
        self.executable = ""
        self.command = None

        self.workflowStatus = {}
        self.stepStatus = {}
        self.workflow_commons = {}
        self.step_commons = {}

        self.applicationName = ""
        self.applicationVersion = ""
        self.outputDataFileMask = []
        self.stepName = ""
        self.stepInputData = []
        self.applicationLog = "applicationLog.txt"

        self.appSteps = []
        self.inputDataList = []
        self.InputData = []
        self.inputDataType = ""

        # These are useful objects (see the getFileReporter(), getJobReporter() and getRequestContainer() functions)
        self.fileReport = None
        self.jobReport = None
        self.request = None

    #############################################################################

    def execute(self):
        """Function called by all super classes. This is the only function that Workflow will call automatically.

        The design adopted here is that all the modules are inheriting from this class,
        and will NOT override this function. Instead, the inherited modules will override the following functions:
        _resolveInputVariables()
        _initialize()
        _setCommand()
        _executeCommand()
        _execute()
        that are called here exactly in this order.
        Each implementation of these functions, in the subclasses, should never return S_OK, S_ERROR.
        This choice has been made for convenience of coding, and for the high level of inheritance implemented here.
        Instead, they should return:
        - None when no issues arise
        - a RuntimeError exception when there are issues
        - a GracefulTermination exception (defined also here) when the module should be terminated gracefully

        The various parameters in input to this method are used almost only for testing purposes.
        """

        if not self.production_id:
            # self.PRODUCTION_ID is always set by the workflow
            self.production_id = int(self.workflow_commons["PRODUCTION_ID"])

        if not self.prod_job_id:
            # self.JOB_ID is set by the workflow, but this is not the WMS job id, but the transformation (production) task id
            self.prod_job_id = int(self.workflow_commons["JOB_ID"])

        if not self.jobID:
            # this is the real wms job ID
            self.jobID = int(os.environ.get("JOBID", self.jobID))

        if not self.step_number:
            # self.STEP_NUMBER is always set by the workflow
            self.step_number = int(self.STEP_NUMBER)  # pylint: disable=no-member

        if not self.step_id:
            self.step_id = "%d_%d_%d" % (self.production_id, self.prod_job_id, self.step_number)

        try:
            # This is what has to be extended in the modules
            self._resolveInputVariables()
            self._initialize()
            self._setCommand()
            self._executeCommand()
            self._execute()
            self._finalize()

        # If everything is OK
        except GracefulTermination as status:
            self.setApplicationStatus(status)
            self.log.info(status)
            return S_OK(status)

        # This catches everything that is voluntarily thrown within the modules, so an error
        except RuntimeError as rte:
            if len(rte.args) > 1:
                # In this case the RuntimeError is supposed to return in rte[1] an error code (possibly from DErrno)
                self.log.error(rte.args[0])
                self.setApplicationStatus(rte.args[0])
                # rte.args[1] may be a shell exit code, not an error code: If we pass it to S_ERROR directly
                # it will prefix the error message with the wrong description in that case.
                # Instead we set the Errno manually afterwards which leaves the message unchanged.
                res = S_ERROR(rte.args[0])
                res["Errno"] = rte.args[1]
                return res

            # If we are here it is just a string
            self.log.error(rte)
            self.setApplicationStatus(rte)
            return S_ERROR(rte)

        # This catches everything that is not voluntarily thrown (here, really writing an exception)
        except BaseException as exc:  # pylint: disable=broad-except
            self.log.exception(exc)
            self.setApplicationStatus(exc)
            return S_ERROR(exc)

        finally:
            self.finalize()

    def _resolveInputVariables(self):
        """By convention the module input parameters are resolved here.
        fileReport, jobReport, and request objects are instantiated/recorded here.

        This will also call the resolution of the input workflow.
        The resolution of the input step should instead be done on a step basis.

        NB: Never forget to call this base method when extending it.
        """

        self.log.verbose("workflow_commons = ", self.workflow_commons)
        self.log.verbose("step_commons = ", self.step_commons)

        if not self.fileReport:
            self.fileReport = self._getFileReporter()
        if not self.jobReport:
            self.jobReport = self._getJobReporter()
        if not self.request:
            self.request = self._getRequestContainer()

        self._resolveInputWorkflow()

    def _initialize(self):
        """TBE

        For initializing the module, whatever operation this can be
        """
        pass

    def _setCommand(self):
        """TBE

        For "executors" modules, set the command to be used in the self.command variable.
        """
        pass

    def _executeCommand(self):
        """TBE

        For "executors" modules, executes self.command as set in the _setCommand() method
        """
        pass

    def _execute(self):
        """TBE

        Executes, whatever this means for the module implementing it
        """
        pass

    def _finalize(self, status=""):
        """TBE

        By default, the module finalizes correctly
        """

        if not status:
            status = f"{str(self.__class__)} correctly finalized"

        raise GracefulTermination(status)

    #############################################################################

    def finalize(self):
        """Just finalizing the module execution by flushing the logs. This will be done always."""

        self.log.info("===== Terminating " + str(self.__class__) + " ===== ")

    #############################################################################

    def _getJobReporter(self):
        """just return the job reporter (object, always defined by dirac-jobexec)"""

        if "JobReport" in self.workflow_commons:
            return self.workflow_commons["JobReport"]
        jobReport = JobReport(self.jobID)
        self.workflow_commons["JobReport"] = jobReport
        return jobReport

    #############################################################################

    def _getFileReporter(self):
        """just return the file reporter (object)"""

        if "FileReport" in self.workflow_commons:
            return self.workflow_commons["FileReport"]
        fileReport = FileReport()
        self.workflow_commons["FileReport"] = fileReport
        return fileReport

    #############################################################################

    def _getRequestContainer(self):
        """just return the RequestContainer reporter (object)"""

        if "Request" in self.workflow_commons:
            return self.workflow_commons["Request"]
        request = Request()
        self.workflow_commons["Request"] = request
        return request

    #############################################################################

    def _resolveInputWorkflow(self):
        """Resolve the input variables that are in the workflow_commons"""

        self.jobType = self.workflow_commons.get("JobType", self.jobType)

        self.InputData = ""
        if "InputData" in self.workflow_commons and self.workflow_commons["InputData"]:
            self.InputData = self.workflow_commons["InputData"]

        if "ParametricInputData" in self.workflow_commons:
            pID = copy.deepcopy(self.workflow_commons["ParametricInputData"])
            if pID:
                if isinstance(pID, list):
                    pID = ";".join(pID)
                #      self.InputData += ';' + pID
                self.InputData = pID
                self.InputData = self.InputData.rstrip(";")

        if self.InputData == ";":
            self.InputData = ""

        self.inputDataList = [lfn.strip("LFN:") for lfn in self.InputData.split(";") if lfn]

        self.appSteps = self.workflow_commons.get("appSteps", self.appSteps)

        if "outputDataFileMask" in self.workflow_commons:
            self.outputDataFileMask = self.workflow_commons["outputDataFileMask"]
            if isinstance(self.outputDataFileMask, str):
                self.outputDataFileMask = [
                    i.lower().strip() for i in self.outputDataFileMask.split(";")
                ]  # pylint: disable=no-member

    #############################################################################

    def _resolveInputStep(self):
        """Resolve the input variables for an application step"""

        self.stepName = self.step_commons["STEP_INSTANCE_NAME"]

        self.executable = self.step_commons.get("executable", "Unknown")

        self.applicationName = self.step_commons.get("applicationName", "Unknown")

        self.applicationVersion = self.step_commons.get("applicationVersion", "Unknown")

        self.applicationLog = self.step_commons.get(
            "applicationLog", self.step_commons.get("logFile", self.applicationLog)
        )

        self.inputDataType = self.step_commons.get("inputDataType", self.inputDataType)

        stepInputData = []
        if "inputData" in self.step_commons and self.step_commons["inputData"]:
            stepInputData = self.step_commons["inputData"]
        elif self.InputData:
            stepInputData = copy.deepcopy(self.InputData)
        if stepInputData:
            stepInputData = self._determineStepInputData(
                stepInputData,
            )
            self.stepInputData = [sid.strip("LFN:") for sid in stepInputData]

    #############################################################################

    def _determineStepInputData(self, inputData):
        """determine the input data for the step"""
        if inputData == "previousStep":
            stepIndex = self.appSteps.index(self.stepName)
            previousStep = self.appSteps[stepIndex - 1]

            stepInputData = []
            for outputF in self.workflow_commons["outputList"]:
                try:
                    if (
                        outputF["stepName"] == previousStep
                        and outputF["outputDataType"].lower() == self.inputDataType.lower()
                    ):
                        stepInputData.append(outputF["outputDataName"])
                except KeyError:
                    raise RuntimeError(f"Can't find output of step {previousStep}")

            return stepInputData

        return [x.strip("LFN:") for x in inputData.split(";")]

    #############################################################################

    def setApplicationStatus(self, status, sendFlag=True):
        """Wraps around setJobApplicationStatus of state update client"""
        if not self._WMSJob():
            return 0  # e.g. running locally prior to submission

        if self._checkWFAndStepStatus(noPrint=True):
            # The application status won't be updated in case the workflow or the step is failed already
            if not isinstance(status, str):
                status = str(status)
            self.log.verbose(f"setJobApplicationStatus({self.jobID}, {status})")
            jobStatus = self.jobReport.setApplicationStatus(status, sendFlag)
            if not jobStatus["OK"]:
                self.log.warn(jobStatus["Message"])

    #############################################################################

    def _WMSJob(self):
        """Check if this job is running via WMS"""
        return True if self.jobID else False

    #############################################################################

    def _enableModule(self):
        """Enable module if it's running via WMS"""
        if not self._WMSJob():
            self.log.info("No WMS JobID found, disabling module via control flag")
            return False
        else:
            self.log.verbose("Found WMS JobID = %d" % self.jobID)
            return True

    #############################################################################

    def _checkWFAndStepStatus(self, noPrint=False):
        """Check the WF and Step status"""
        if not self.workflowStatus["OK"] or not self.stepStatus["OK"]:
            if not noPrint:
                self.log.info("Skip this module, failure detected in a previous step :")
                self.log.info(f"Workflow status : {self.workflowStatus}")
                self.log.info(f"Step Status : {self.stepStatus}")
            return False
        else:
            return True

    #############################################################################

    def setJobParameter(self, name, value, sendFlag=True):
        """Wraps around setJobParameter of state update client"""
        if not self._WMSJob():
            return 0  # e.g. running locally prior to submission

        self.log.verbose("setJobParameter(%d,%s,%s)" % (self.jobID, name, value))

        jobParam = self.jobReport.setJobParameter(str(name), str(value), sendFlag)
        if not jobParam["OK"]:
            self.log.warn(jobParam["Message"])

    #############################################################################

    def getCandidateFiles(self, outputList, outputLFNs, fileMask, stepMask=""):
        """Returns list of candidate files to upload, check if some outputs are missing.

        outputList has the following structure:
          [ {'outputDataType':'','outputDataSE':'','outputDataName':''} , {...} ]

        outputLFNs is the list of output LFNs for the job

        fileMask is the output file extensions to restrict the outputs to

        returns dictionary containing type, SE and LFN for files restricted by mask
        """
        fileInfo = {}

        for outputFile in outputList:
            if "outputDataType" in outputFile and "outputDataSE" in outputFile and "outputDataName" in outputFile:
                fname = outputFile["outputDataName"]
                fileSE = outputFile["outputDataSE"]
                fileType = outputFile["outputDataType"]
                fileInfo[fname] = {"type": fileType, "workflowSE": fileSE}
            else:
                self.log.error("Ignoring malformed output data specification", str(outputFile))

        for lfn in outputLFNs:
            if os.path.basename(lfn) in fileInfo.keys():
                fileInfo[os.path.basename(lfn)]["lfn"] = lfn
                self.log.verbose(f"Found LFN {lfn} for file {os.path.basename(lfn)}")

        # check local existance
        self._checkLocalExistance(list(fileInfo))

        # Select which files have to be uploaded: in principle all
        candidateFiles = self._applyMask(fileInfo, fileMask, stepMask)

        # Sanity check all final candidate metadata keys are present (return S_ERROR if not)
        self._checkSanity(candidateFiles)

        return candidateFiles

    #############################################################################

    def _applyMask(self, candidateFilesIn, fileMask, stepMask):
        """Select which files have to be uploaded: in principle all"""
        candidateFiles = copy.deepcopy(candidateFilesIn)

        if fileMask and not isinstance(fileMask, list):
            fileMask = [fileMask]
        if isinstance(stepMask, int):
            stepMask = str(stepMask)
        if stepMask and not isinstance(stepMask, list):
            stepMask = [stepMask]

        if fileMask and fileMask != [""]:
            for fileName, metadata in list(candidateFiles.items()):
                if metadata["type"].lower() not in fileMask:  # and ( fileName.split( '.' )[-1] not in fileMask ) ):
                    del candidateFiles[fileName]
                    self.log.info(
                        "Output file %s was produced but will not be treated (fileMask is %s)"
                        % (fileName, ", ".join(fileMask))
                    )
        else:
            self.log.info("No outputDataFileMask provided, the files with all the extensions will be considered")

        if stepMask and stepMask != [""]:
            # This supposes that the LFN contains the step ID
            for fileName, metadata in list(candidateFiles.items()):
                if fileName.split("_")[-1].split(".")[0] not in stepMask:
                    del candidateFiles[fileName]
                    self.log.info(
                        "Output file %s was produced but will not be treated (stepMask is %s)"
                        % (fileName, ", ".join(stepMask))
                    )
        else:
            self.log.info("No outputDataStep provided, the files output of all the steps will be considered")

        return candidateFiles

    #############################################################################

    def _checkSanity(self, candidateFiles):
        """Sanity check all final candidate metadata keys are present"""

        notPresentKeys = []

        mandatoryKeys = ["type", "workflowSE", "lfn"]  # filedict is used for requests
        for fileName, metadata in candidateFiles.items():
            for key in mandatoryKeys:
                if key not in metadata:
                    notPresentKeys.append((fileName, key))

        if notPresentKeys:
            for fileName_keys in notPresentKeys:
                self.log.error(f"File {fileName_keys[0]} has missing {fileName_keys[1]}")
            raise ValueError

    #############################################################################

    def _checkLocalExistance(self, fileList):
        """Check that the list of output files are present locally"""

        notPresentFiles = []

        for fileName in fileList:
            if not os.path.exists(fileName):
                notPresentFiles.append(fileName)

        if notPresentFiles:
            self.log.error(f"Output data file list {notPresentFiles} does not exist locally")
            raise os.error

    #############################################################################

    def generateFailoverFile(self):
        """Retrieve the accumulated reporting request, and produce a JSON file that is consumed by the JobWrapper"""
        reportRequest = None
        result = self.jobReport.generateForwardDISET()
        if not result["OK"]:
            self.log.warn(f"Could not generate Operation for job report with result:\n{result}")
        else:
            reportRequest = result["Value"]
        if reportRequest:
            self.log.info("Populating request with job report information")
            self.request.addOperation(reportRequest)

        accountingReport = None
        if "AccountingReport" in self.workflow_commons:
            accountingReport = self.workflow_commons["AccountingReport"]
        if accountingReport:
            result = accountingReport.commit()
            if not result["OK"]:
                self.log.error("!!! Both accounting and RequestDB are down? !!!")
                return result

        if len(self.request):
            isValid = RequestValidator().validate(self.request)
            if not isValid["OK"]:
                raise RuntimeError(f"Failover request is not valid: {isValid['Message']}")
            else:
                requestJSON = self.request.toJSON()
                if requestJSON["OK"]:
                    self.log.info("Creating failover request for deferred operations for job %d" % self.jobID)
                    request_string = str(requestJSON["Value"])
                    self.log.debug(request_string)
                    # Write out the request string
                    fname = "%d_%d_request.json" % (self.production_id, self.prod_job_id)
                    jsonFile = open(fname, "w")
                    jsonFile.write(request_string)
                    jsonFile.close()
                    self.log.info(f"Created file containing failover request {fname}")
                    result = self.request.getDigest()
                    if result["OK"]:
                        self.log.info(f"Digest of the request: {result['Value']}")
                    else:
                        self.log.error("No digest? That's not sooo important, anyway:", result["Message"])
                else:
                    raise RuntimeError(requestJSON["Message"])

    #############################################################################


#############################################################################


class GracefulTermination(Exception):
    pass


#############################################################################
