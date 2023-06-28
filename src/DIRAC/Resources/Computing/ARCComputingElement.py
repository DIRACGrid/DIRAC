""" ARC Computing Element

Leverage the arc Python library to interact with ARC gridftp services.

**Configuration Parameters**

Configuration for the ARCComputingElement submission can be done via the configuration system.

ARCLogLevel:
   Log level of the ARC logging library. Possible values are: `DEBUG`, `VERBOSE`, `INFO`, `WARNING`, `ERROR`, `FATAL`

EndpointType:
   Name of the protocol to use to interact with ARC services: Emies and Gridftp are supported.
   Gridftp communicates with gridftpd services providing authentication and encryption for file transfers.
   ARC developers are going to drop it in the future.
   Emies is another protocol that allows to interact with A-REX services that provide additional features
   (support of OIDC tokens).

Host:
   The host for the ARC CE, used to overwrite the CE name.

Preamble:
   Line that should be executed just before the executable file.

WorkingDirectory:
   Directory where the pilot log files are stored locally. For instance::

     /opt/dirac/pro/runit/WorkloadManagement/SiteDirectorArc

XRSLExtraString:
   Default additional string for ARC submit files. Should be written in the following format::

     (key = "value")

   Please note that for ARC & ARC6, any times (such as wall or CPU time) in the XRSL should be specified in minutes.
   For AREX, these times should be given in seconds (see https://www.nordugrid.org/arc/arc6/users/xrsl.html?#cputime).


XRSLMPExtraString:
   Default additional string for ARC submit files for multi-processor jobs. Should be written in the following format::

     (key = "value")

   The XRSLExtraString note about times also applies to this configuration option.

**Code Documentation**
"""
import os
import stat
import sys
import uuid

import arc  # Has to work if this module is called #pylint: disable=import-error
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.PilotBundle import writeScript
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


MANDATORY_PARAMETERS = ["Queue"]  # Mandatory for ARC CEs in GLUE2?
# See https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#rest-interface-job-states
# We let "Deleted, Hold, Undefined" for the moment as we are not sure whether they are still used
# "None" is a special case: it is returned when the job ID is not found in the system
STATES_MAP = {
    "Accepting": PilotStatus.WAITING,
    "Accepted": PilotStatus.WAITING,
    "Preparing": PilotStatus.WAITING,
    "Prepared": PilotStatus.WAITING,
    "Submitting": PilotStatus.WAITING,
    "Queuing": PilotStatus.WAITING,
    "Running": PilotStatus.RUNNING,
    "Held": PilotStatus.RUNNING,
    "Exitinglrms": PilotStatus.RUNNING,
    "Other": PilotStatus.RUNNING,
    "Executed": PilotStatus.RUNNING,
    "Finishing": PilotStatus.RUNNING,
    "Finished": PilotStatus.DONE,
    "Failed": PilotStatus.FAILED,
    "Killing": PilotStatus.ABORTED,
    "Killed": PilotStatus.ABORTED,
    "Wiped": PilotStatus.ABORTED,
    "Deleted": PilotStatus.ABORTED,
    "Hold": PilotStatus.FAILED,
    "None": PilotStatus.ABORTED,
    "Undefined": PilotStatus.UNKNOWN,
}


def prepareProxyToken(func):
    """Decorator to set up proxy or token as necessary"""

    def wrapper(*args, **kwargs):
        # Get the reference to the CE class object
        self = args[0]

        # Prepare first the proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

        # Set the token in the environment if needed
        if self.token:
            os.environ["BEARER_TOKEN"] = self.token["access_token"]

        return func(*args, **kwargs)

    return wrapper


class ARCComputingElement(ComputingElement):
    _arcLevels = ["DEBUG", "VERBOSE", "INFO", "WARNING", "ERROR", "FATAL"]

    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        self.submittedJobs = 0
        self.mandatoryParameters = MANDATORY_PARAMETERS
        self.pilotProxy = ""
        self.queue = ""
        self.arcQueue = ""
        self.gridEnv = ""
        self.ceHost = self.ceName
        self.endpointType = "Gridftp"
        self.usercfg = arc.common.UserConfig()
        self.preamble = ""

        # set the timeout to the default 20 seconds in case the UserConfig constructor did not
        self.usercfg.Timeout(20)  # pylint: disable=pointless-statement
        self.gridEnv = ""

        # Used in getJobStatus
        self.mapStates = STATES_MAP
        # Extra XRSL info
        self.xrslExtraString = ""
        self.xrslMPExtraString = ""

    #############################################################################

    def _getARCJob(self, jobID):
        """Create an ARC Job with all the needed / possible parameters defined.
        By the time we come here, the environment variable X509_USER_PROXY should already be set
        """
        j = arc.Job()
        j.JobID = str(jobID)
        j.IDFromEndpoint = os.path.basename(j.JobID)

        if self.endpointType == "Gridftp":
            statURL = f"ldap://{self.ceHost}:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid={jobID})"
            j.JobStatusURL = arc.URL(str(statURL))
            j.JobStatusInterfaceName = "org.nordugrid.ldapng"

            mangURL = f"gsiftp://{self.ceHost}:2811/jobs/"
            j.JobManagementURL = arc.URL(str(mangURL))
            j.JobManagementInterfaceName = "org.nordugrid.gridftpjob"

            j.ServiceInformationURL = j.JobManagementURL
            j.ServiceInformationInterfaceName = "org.nordugrid.ldapng"
        else:
            commonURL = f"https://{self.ceHost}:8443/arex"
            j.JobStatusURL = arc.URL(str(commonURL))
            j.JobStatusInterfaceName = "org.ogf.glue.emies.activitymanagement"

            j.JobManagementURL = arc.URL(str(commonURL))
            j.JobManagementInterfaceName = "org.ogf.glue.emies.activitymanagement"

            j.ServiceInformationURL = arc.URL(str(commonURL))
            j.ServiceInformationInterfaceName = "org.ogf.glue.emies.resourceinfo"

        j.PrepareHandler(self.usercfg)
        return j

    #############################################################################
    def _addCEConfigDefaults(self):
        """Method to make sure all necessary Configuration Parameters are defined"""
        # First assure that any global parameters are loaded
        ComputingElement._addCEConfigDefaults(self)

    #############################################################################
    def _writeXRSL(self, executableFile, inputs, outputs):
        """Create the JDL for submission

        :param str executableFile: executable to wrap in a XRSL file
        :param list inputs: path of the dependencies to include along with the executable
        :param list outputs: path of the outputs that we want to get at the end of the execution
        """
        diracStamp = uuid.uuid4().hex
        # Evaluate the number of processors to allocate
        nProcessors = self.ceParameters.get("NumberOfProcessors", 1)

        xrslMPAdditions = ""
        if nProcessors and nProcessors > 1:
            xrslMPAdditions = """
(count = %(processors)u)
(countpernode = %(processorsPerNode)u)
%(xrslMPExtraString)s
      """ % {
                "processors": nProcessors,
                "processorsPerNode": nProcessors,  # This basically says that we want all processors on the same node
                "xrslMPExtraString": self.xrslMPExtraString,
            }

        # Dependencies that have to be embedded along with the executable
        xrslInputs = ""
        executables = []
        for inputFile in inputs:
            inputFileBaseName = os.path.basename(inputFile)
            if os.access(inputFile, os.X_OK):
                # Files that would need execution rights on the remote worker node
                executables.append(inputFileBaseName)
            xrslInputs += f'({inputFileBaseName} "{inputFile}")'

        # Executables are added to the XRSL
        xrslExecutables = ""
        if executables:
            xrslExecutables = f"(executables={' '.join(executables)})"

        # Output files to retrieve once the execution is complete
        xrslOutputs = f'("{diracStamp}.out" "") ("{diracStamp}.err" "")'
        for outputFile in outputs:
            xrslOutputs += f'({outputFile} "")'

        xrsl = """
&(executable="{executable}")
(inputFiles=({executable} "{executableFile}") {xrslInputAdditions})
(stdout="{diracStamp}.out")
(stderr="{diracStamp}.err")
(environment=("DIRAC_PILOT_STAMP" "{diracStamp}"))
(outputFiles={xrslOutputFiles})
(queue={queue})
{xrslMPAdditions}
{xrslExecutables}
{xrslExtraString}
    """.format(
            executableFile=executableFile,
            executable=os.path.basename(executableFile),
            xrslInputAdditions=xrslInputs,
            diracStamp=diracStamp,
            queue=self.arcQueue,
            xrslOutputFiles=xrslOutputs,
            xrslMPAdditions=xrslMPAdditions,
            xrslExecutables=xrslExecutables,
            xrslExtraString=self.xrslExtraString,
        )

        return xrsl, diracStamp

    def _bundlePreamble(self, executableFile):
        """Bundle the preamble with the executable file"""
        wrapperContent = f"{self.preamble}\n./{executableFile}"

        # We need to make sure the executable file can be executed by the wrapper
        # By adding the execution mode to the file, the file will be processed as an "executable" in the XRSL
        # This is done in _writeXRSL()
        if not os.access(executableFile, os.X_OK):
            os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH)

        return writeScript(wrapperContent, os.getcwd())

    #############################################################################
    def _reset(self):
        # Assume that the ARC queues are always of the format nordugrid-<batchSystem>-<queue>
        # And none of our supported batch systems have a "-" in their name
        self.queue = self.ceParameters.get("CEQueueName", self.ceParameters["Queue"])
        self.arcQueue = self.queue.split("-", 2)[2]

        self.ceHost = self.ceParameters.get("Host", self.ceHost)
        self.gridEnv = self.ceParameters.get("GridEnv", self.gridEnv)

        # extra XRSL data (should respect the XRSL format)
        self.xrslExtraString = self.ceParameters.get("XRSLExtraString", self.xrslExtraString)
        self.xrslMPExtraString = self.ceParameters.get("XRSLMPExtraString", self.xrslMPExtraString)

        self.preamble = self.ceParameters.get("Preamble", self.preamble)

        # ARC endpoint types (Gridftp, Emies)
        endpointType = self.ceParameters.get("EndpointType", self.endpointType)
        if endpointType not in ["Gridftp", "Emies"]:
            self.log.warn("Unknown ARC endpoint, change to default", self.endpointType)
        else:
            self.endpointType = endpointType

        # ARCLogLevel to enable/disable logs coming from the ARC client
        # Because the ARC logger works independently from the standard logging library,
        # it needs a specific initialization flag
        # Expected values are: ["", "DEBUG", "VERBOSE", "INFO", "WARNING", "ERROR" and "FATAL"]
        # Modifying the ARCLogLevel of an ARCCE instance would impact all existing instances within a same process.
        logLevel = self.ceParameters.get("ARCLogLevel", "")
        if logLevel:
            arc.Logger.getRootLogger().removeDestinations()
            if logLevel not in self._arcLevels:
                self.log.warn("ARCLogLevel input is not known:", f"{logLevel} not in {self._arcLevels}")
            else:
                logstdout = arc.LogStream(sys.stdout)
                logstdout.setFormat(arc.ShortFormat)
                arc.Logger.getRootLogger().addDestination(logstdout)
                arc.Logger.getRootLogger().setThreshold(getattr(arc, logLevel))

        return S_OK()

    #############################################################################
    @prepareProxyToken
    def submitJob(self, executableFile, proxy, numberOfJobs=1, inputs=None, outputs=None):
        """Method to submit job"""
        if not inputs:
            inputs = []
        if not outputs:
            outputs = []

        self.log.verbose(f"Executable file path: {executableFile}")
        if self.preamble:
            inputs.append(executableFile)
            executableFile = self._bundlePreamble(executableFile)

        batchIDList = []
        stampDict = {}

        if self.endpointType == "Gridftp":
            endpoint = arc.Endpoint(str(self.ceHost + ":2811/jobs"), arc.Endpoint.JOBSUBMIT, "org.nordugrid.gridftpjob")
        else:
            endpoint = arc.Endpoint(
                str("https://" + self.ceHost + ":8443/arex"),
                arc.Endpoint.JOBSUBMIT,
                "org.ogf.glue.emies.activitycreation",
            )

        # Submit jobs iteratively for now. Tentatively easier than mucking around with the JobSupervisor class
        for __i in range(numberOfJobs):
            # The basic job description
            jobdescs = arc.JobDescriptionList()
            # Get the job into the ARC way
            xrslString, diracStamp = self._writeXRSL(executableFile, inputs, outputs)
            self.log.debug(f"XRSL string submitted : {xrslString}")
            self.log.debug(f"DIRAC stamp for job : {diracStamp}")

            # The arc bindings don't accept unicode objects in Python 2 so xrslString must be explicitly cast
            result = arc.JobDescription.Parse(str(xrslString), jobdescs)
            if not result:
                self.log.error("Invalid job description", f"{xrslString!r}, message={result.str()}")
                break
            # Submit the job
            jobs = arc.JobList()  # filled by the submit process
            submitter = arc.Submitter(self.usercfg)
            result = submitter.Submit(endpoint, jobdescs, jobs)
            # Save info or else ..else.
            if result == arc.SubmissionStatus.NONE:
                # Job successfully submitted
                pilotJobReference = jobs[0].JobID
                batchIDList.append(pilotJobReference)
                stampDict[pilotJobReference] = diracStamp
                self.log.debug(f"Successfully submitted job {pilotJobReference} to CE {self.ceHost}")
            else:
                self._analyzeSubmissionError(result)
                break  # Boo hoo *sniff*

        if self.preamble:
            os.unlink(executableFile)

        if batchIDList:
            result = S_OK(batchIDList)
            result["PilotStampDict"] = stampDict
        else:
            result = S_ERROR("No pilot references obtained from the ARC job submission")
        return result

    def _analyzeSubmissionError(self, result):
        """Provide further information about the submission error

        :param arc.SubmissionStatus result: submission error
        """
        message = "Failed to submit job because "
        if result.isSet(arc.SubmissionStatus.NOT_IMPLEMENTED):  # pylint: disable=no-member
            self.log.warn(f"{message} feature not implemented on CE? (weird I know - complain to site admins")
        if result.isSet(arc.SubmissionStatus.NO_SERVICES):  # pylint: disable=no-member
            self.log.warn(f"{message} no services are running on CE? (open GGUS ticket to site admins")
        if result.isSet(arc.SubmissionStatus.ENDPOINT_NOT_QUERIED):  # pylint: disable=no-member
            self.log.warn(f"{message} endpoint was not even queried. (network ..?)")
        if result.isSet(arc.SubmissionStatus.BROKER_PLUGIN_NOT_LOADED):  # pylint: disable=no-member
            self.log.warn(f"{message} BROKER_PLUGIN_NOT_LOADED : ARC library installation problem?")
        if result.isSet(arc.SubmissionStatus.DESCRIPTION_NOT_SUBMITTED):  # pylint: disable=no-member
            self.log.warn(f"{message} Job not submitted - incorrect job description? (missing field in XRSL string?)")
        if result.isSet(arc.SubmissionStatus.SUBMITTER_PLUGIN_NOT_LOADED):  # pylint: disable=no-member
            self.log.warn(f"{message} SUBMITTER_PLUGIN_NOT_LOADED : ARC library installation problem?")
        if result.isSet(arc.SubmissionStatus.AUTHENTICATION_ERROR):  # pylint: disable=no-member
            self.log.warn(
                f"{message} authentication error - screwed up / expired proxy? Renew / upload pilot proxy on machine?"
            )
        if result.isSet(arc.SubmissionStatus.ERROR_FROM_ENDPOINT):  # pylint: disable=no-member
            self.log.warn(f"{message} some error from the CE - possibly CE problems?")
        self.log.warn(f"{message} ... maybe above messages will give a hint.")

    #############################################################################
    @prepareProxyToken
    def killJob(self, jobIDList):
        """Kill the specified jobs"""

        jobList = list(jobIDList)
        if isinstance(jobIDList, str):
            jobList = [jobIDList]

        self.log.debug(f"Killing jobs {jobIDList}")
        jobs = []
        for jobID in jobList:
            jobs.append(self._getARCJob(jobID))

        # JobSupervisor is able to aggregate jobs to perform bulk operations and thus minimizes the communication overhead
        # We still need to create chunks to avoid timeout in the case there are too many jobs to supervise
        for chunk in breakListIntoChunks(jobs, 100):
            job_supervisor = arc.JobSupervisor(self.usercfg, chunk)
            if not job_supervisor.Cancel():
                errorString = " - ".join(jobList).strip()
                return S_ERROR(f"Failed to kill at least one of these jobs: {errorString}. CE(?) not reachable?")

        return S_OK()

    #############################################################################
    @prepareProxyToken
    def getCEStatus(self):
        """Method to return information on running and pending jobs.
        We hope to satisfy both instances that use robot proxies and those which use proper configurations.
        """

        # Try to find out which VO we are running for.
        vo = ""
        res = getVOfromProxyGroup()
        if res["OK"]:
            vo = res["Value"]

        result = S_OK()
        result["SubmittedJobs"] = 0

        if not vo:
            # Presumably the really proper way forward once the infosys-discuss WG comes up with a solution
            # and it is implemented. Needed for DIRAC instances which use robot certificates for pilots.
            if self.endpointType == "Gridftp":
                endpoints = [
                    arc.Endpoint(
                        str("ldap://" + self.ceHost + "/MDS-Vo-name=local,o=grid"),
                        arc.Endpoint.COMPUTINGINFO,
                        "org.nordugrid.ldapng",
                    )
                ]
            else:
                endpoints = [
                    arc.Endpoint(
                        str("https://" + self.ceHost + ":8443/arex"),
                        arc.Endpoint.COMPUTINGINFO,
                        "org.ogf.glue.emies.resourceinfo",
                    )
                ]

            retriever = arc.ComputingServiceRetriever(self.usercfg, endpoints)
            retriever.wait()  # Takes a bit of time to get and parse the ldap information
            targets = retriever.GetExecutionTargets()
            ceStats = targets[0].ComputingShare
            self.log.debug(f"Running jobs for CE {self.ceHost} : {ceStats.RunningJobs}")
            self.log.debug(f"Waiting jobs for CE {self.ceHost} : {ceStats.WaitingJobs}")
            result["RunningJobs"] = ceStats.RunningJobs
            result["WaitingJobs"] = ceStats.WaitingJobs
        else:
            # The system which works properly at present for ARC CEs that are configured correctly.
            # But for this we need the VO to be known - ask me (Raja) for the whole story if interested.
            # cmd = 'ldapsearch -x -LLL -H ldap://%s:2135 -b mds-vo-name=resource,o=grid "(GlueVOViewLocalID=%s)"' % (
            #     self.ceHost, vo.lower())
            if not self.queue:
                self.log.error("ARCComputingElement: No queue ...")
                res = S_ERROR(f"Unknown queue ({self.queue}) failure for site {self.ceHost}")
                return res
            cmd1 = f"ldapsearch -x -o ldif-wrap=no -LLL -H ldap://{self.ceHost}:2135  -b 'o=glue' "
            cmd2 = f'"(&(objectClass=GLUE2MappingPolicy)(GLUE2PolicyRule=vo:{vo.lower()}))"'
            cmd3 = f" | grep GLUE2MappingPolicyShareForeignKey | grep {self.arcQueue}"
            cmd4 = " | sed 's/GLUE2MappingPolicyShareForeignKey: /GLUE2ShareID=/' "
            cmd5 = f" | xargs -L1 ldapsearch -x -o ldif-wrap=no -LLL -H ldap://{self.ceHost}:2135 -b 'o=glue' "
            cmd6 = " | egrep '(ShareWaiting|ShareRunning)'"
            res = shellCall(0, cmd1 + cmd2 + cmd3 + cmd4 + cmd5 + cmd6)
            if not res["OK"]:
                self.log.debug(f"Could not query CE {self.ceHost} - is it down?")
                return res
            try:
                ldapValues = res["Value"][1].split("\n")
                running = [lValue for lValue in ldapValues if "GLUE2ComputingShareRunningJobs" in lValue]
                waiting = [lValue for lValue in ldapValues if "GLUE2ComputingShareWaitingJobs" in lValue]
                result["RunningJobs"] = int(running[0].split(":")[1])
                result["WaitingJobs"] = int(waiting[0].split(":")[1])
            except IndexError:
                res = S_ERROR(f"Unknown ldap failure for site {self.ceHost}")
                return res

        return result

    #############################################################################
    @prepareProxyToken
    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""

        jobTmpList = list(jobIDList)
        if isinstance(jobIDList, str):
            jobTmpList = [jobIDList]

        # Pilots are stored with a DIRAC stamp (":::XXXXX") appended
        jobList = []
        for j in jobTmpList:
            if ":::" in j:
                job = j.split(":::")[0]
            else:
                job = j
            jobList.append(job)

        jobs = []
        for jobID in jobList:
            jobs.append(self._getARCJob(jobID))

        # JobSupervisor is able to aggregate jobs to perform bulk operations and thus minimizes the communication overhead
        # We still need to create chunks to avoid timeout in the case there are too many jobs to supervise
        jobsUpdated = []
        for chunk in breakListIntoChunks(jobs, 100):
            job_supervisor = arc.JobSupervisor(self.usercfg, chunk)
            job_supervisor.Update()
            jobsUpdated.extend(job_supervisor.GetAllJobs())

        resultDict = {}
        jobsToRenew = []
        jobsToCancel = []
        for job in jobsUpdated:
            jobID = job.JobID
            self.log.debug(f"Retrieving status for job {jobID}")
            arcState = job.State.GetGeneralState()
            self.log.debug(f"ARC status for job {jobID} is {arcState}")
            if arcState:  # Meaning arcState is filled. Is this good python?
                resultDict[jobID] = self.mapStates[arcState]
                # Renew proxy only of jobs which are running or queuing
                if arcState in ("Running", "Queuing"):
                    nearExpiry = arc.Time() + arc.Period(10000)  # 2 hours, 46 minutes and 40 seconds
                    if job.ProxyExpirationTime < nearExpiry:
                        # Jobs to renew are aggregated to perform bulk operations
                        jobsToRenew.append(job)
                        self.log.debug(
                            f"Renewing proxy for job {jobID} whose proxy expires at {job.ProxyExpirationTime}"
                        )
                if arcState == "Hold":
                    # Jobs to cancel are aggregated to perform bulk operations
                    # Cancel held jobs so they don't sit in the queue forever
                    jobsToCancel.append(job)
                    self.log.debug(f"Killing held job {jobID}")
            else:
                resultDict[jobID] = PilotStatus.UNKNOWN
            # If done - is it really done? Check the exit code
            if resultDict[jobID] == PilotStatus.DONE:
                exitCode = int(job.ExitCode)
                if exitCode:
                    resultDict[jobID] = PilotStatus.FAILED
            self.log.debug(f"DIRAC status for job {jobID} is {resultDict[jobID]}")

        # JobSupervisor is able to aggregate jobs to perform bulk operations and thus minimizes the communication overhead
        # We still need to create chunks to avoid timeout in the case there are too many jobs to supervise
        for chunk in breakListIntoChunks(jobsToRenew, 100):
            job_supervisor_renew = arc.JobSupervisor(self.usercfg, chunk)
            if not job_supervisor_renew.Renew():
                self.log.warn("At least one of the jobs failed to renew its credentials")

        for chunk in breakListIntoChunks(jobsToCancel, 100):
            job_supervisor_cancel = arc.JobSupervisor(self.usercfg, chunk)
            if not job_supervisor_cancel.Cancel():
                self.log.warn("At least one of the jobs failed to be cancelled")

        if not resultDict:
            return S_ERROR("No job statuses returned")

        return S_OK(resultDict)

    #############################################################################
    @prepareProxyToken
    def getJobOutput(self, jobID, workingDirectory=None):
        """Get the specified job standard output and error files.
        Standard output and error are returned as strings.
        If further outputs are retrieved, they are stored in workingDirectory.
        """

        if jobID.find(":::") != -1:
            pilotRef, stamp = jobID.split(":::")
        else:
            pilotRef = jobID
            stamp = ""
        if not stamp:
            return S_ERROR(f"Pilot stamp not defined for {pilotRef}")

        job = self._getARCJob(pilotRef)

        arcID = os.path.basename(pilotRef)
        self.log.debug(f"Retrieving pilot logs for {pilotRef}")
        if not workingDirectory:
            if "WorkingDirectory" in self.ceParameters:
                workingDirectory = os.path.join(self.ceParameters["WorkingDirectory"], arcID)
            else:
                workingDirectory = arcID
        outFileName = os.path.join(workingDirectory, f"{stamp}.out")
        errFileName = os.path.join(workingDirectory, f"{stamp}.err")
        self.log.debug(f"Working directory for pilot output {workingDirectory}")

        # Retrieve the job output:
        # last parameter allows downloading the outputs even if workingDirectory already exists
        isItOkay = job.Retrieve(self.usercfg, arc.URL(str(workingDirectory)), True)
        if isItOkay:
            output = None
            error = None
            try:
                with open(outFileName) as outFile:
                    output = outFile.read()
                os.unlink(outFileName)
                with open(errFileName) as errFile:
                    error = errFile.read()
                os.unlink(errFileName)
            except OSError as e:
                self.log.error("Error downloading outputs", repr(e).replace(",)", ")"))
                return S_ERROR("Error downloading outputs")
            self.log.debug(f"Pilot output = {output}")
            self.log.debug(f"Pilot error = {error}")
        else:
            job.Update()
            arcState = job.State.GetGeneralState()
            if arcState != "Undefined":
                return S_ERROR(f"Failed to retrieve output for {jobID} as job is not finished (maybe not started yet)")
            self.log.debug(
                "Could not retrieve pilot output for %s - either permission / proxy error or could not connect to CE"
                % pilotRef
            )
            return S_ERROR(f"Failed to retrieve output for {jobID}")

        return S_OK((output, error))
