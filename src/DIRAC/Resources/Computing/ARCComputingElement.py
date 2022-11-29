""" ARC Computing Element
    Using the ARC API now

**Configuration Parameters**

Configuration for the ARCComputingElement submission can be done via the configuration system.

XRSLExtraString:
   Default additional string for ARC submit files. Should be written in the following format::

     (key = "value")

XRSLMPExtraString:
   Default additional string for ARC submit files for multi-processor jobs. Should be written in the following format::

     (key = "value")

Host:
   The host for the ARC CE, used to overwrite the CE name.

WorkingDirectory:
   Directory where the pilot log files are stored locally. For instance::

     /opt/dirac/pro/runit/WorkloadManagement/SiteDirectorArc

EndpointType:
   Name of the protocol to use to interact with ARC services: Emies and Gridftp are supported.
   Gridftp communicates with gridftpd services providing authentication and encryption for file transfers.
   ARC developers are going to drop it in the future.
   Emies is another protocol that allows to interact with A-REX services that provide additional features
   (support of OIDC tokens).

Preamble:
   Line that should be executed just before the executable file.

**Code Documentation**
"""
import os
import stat
import sys

import arc  # Has to work if this module is called #pylint: disable=import-error
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.PilotBundle import writeScript
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


MANDATORY_PARAMETERS = ["Queue"]  # Mandatory for ARC CEs in GLUE2?
# See https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#rest-interface-job-states
# We let "Deleted, Hold, Undefined" for the moment as we are not sure whether they are still used
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
    "Undefined": PilotStatus.UNKNOWN,
}


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
    def _writeXRSL(self, executableFile, inputs=None, outputs=None, executables=None):
        """Create the JDL for submission

        :param str executableFile: executable to wrap in a XRSL file
        :param str/list inputs: path of the dependencies to include along with the executable
        :param str/list outputs: path of the outputs that we want to get at the end of the execution
        :param str/list executables: path to inputs that should have execution mode on the remote worker node
        """
        diracStamp = makeGuid()[:8]
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

        # Files that would need execution rights on the remote worker node
        xrslExecutables = ""
        if executables:
            if not isinstance(executables, list):
                executables = [executables]
            xrslExecutables = "(executables=%s)" % " ".join(map(os.path.basename, executables))
            # Add them to the inputFiles
            if not inputs:
                inputs = []
            if not isinstance(inputs, list):
                inputs = [inputs]
            inputs += executables

        # Dependencies that have to be embedded along with the executable
        xrslInputs = ""
        if inputs:
            if not isinstance(inputs, list):
                inputs = [inputs]
            for inputFile in inputs:
                xrslInputs += f'({os.path.basename(inputFile)} "{inputFile}")'

        # Output files to retrieve once the execution is complete
        xrslOutputs = f'("{diracStamp}.out" "") ("{diracStamp}.err" "")'
        if outputs:
            if not isinstance(outputs, list):
                outputs = [outputs]
            for outputFile in outputs:
                xrslOutputs += f'({outputFile} "")'

        xrsl = """
&(executable="{executable}")
(inputFiles=({executable} "{executableFile}") {xrslInputAdditions})
(stdout="{diracStamp}.out")
(stderr="{diracStamp}.err")
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
            arc.Logger_getRootLogger().removeDestinations()
            if logLevel not in self._arcLevels:
                self.log.warn("ARCLogLevel input is not known:", f"{logLevel} not in {self._arcLevels}")
            else:
                logstdout = arc.LogStream(sys.stdout)
                logstdout.setFormat(arc.ShortFormat)
                arc.Logger_getRootLogger().addDestination(logstdout)
                arc.Logger_getRootLogger().setThreshold(getattr(arc, logLevel))

        return S_OK()

    #############################################################################
    def submitJob(self, executableFile, proxy, numberOfJobs=1, inputs=None, outputs=None):
        """Method to submit job"""
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

        self.log.verbose(f"Executable file path: {executableFile}")
        if not os.access(executableFile, 5):
            os.chmod(executableFile, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH)

        executables = None
        if self.preamble:
            executables = [executableFile]
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
            xrslString, diracStamp = self._writeXRSL(executableFile, inputs, outputs, executables)
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
            self.log.warn("%s feature not implemented on CE? (weird I know - complain to site admins" % message)
        if result.isSet(arc.SubmissionStatus.NO_SERVICES):  # pylint: disable=no-member
            self.log.warn("%s no services are running on CE? (open GGUS ticket to site admins" % message)
        if result.isSet(arc.SubmissionStatus.ENDPOINT_NOT_QUERIED):  # pylint: disable=no-member
            self.log.warn("%s endpoint was not even queried. (network ..?)" % message)
        if result.isSet(arc.SubmissionStatus.BROKER_PLUGIN_NOT_LOADED):  # pylint: disable=no-member
            self.log.warn("%s BROKER_PLUGIN_NOT_LOADED : ARC library installation problem?" % message)
        if result.isSet(arc.SubmissionStatus.DESCRIPTION_NOT_SUBMITTED):  # pylint: disable=no-member
            self.log.warn("%s Job not submitted - incorrect job description? (missing field in XRSL string?)" % message)
        if result.isSet(arc.SubmissionStatus.SUBMITTER_PLUGIN_NOT_LOADED):  # pylint: disable=no-member
            self.log.warn("%s SUBMITTER_PLUGIN_NOT_LOADED : ARC library installation problem?" % message)
        if result.isSet(arc.SubmissionStatus.AUTHENTICATION_ERROR):  # pylint: disable=no-member
            self.log.warn(
                "%s authentication error - screwed up / expired proxy? Renew / upload pilot proxy on machine?" % message
            )
        if result.isSet(arc.SubmissionStatus.ERROR_FROM_ENDPOINT):  # pylint: disable=no-member
            self.log.warn("%s some error from the CE - possibly CE problems?" % message)
        self.log.warn("%s ... maybe above messages will give a hint." % message)

    #############################################################################
    def killJob(self, jobIDList):
        """Kill the specified jobs"""

        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

        jobList = list(jobIDList)
        if isinstance(jobIDList, str):
            jobList = [jobIDList]

        self.log.debug("Killing jobs %s" % jobIDList)
        jobs = []
        for jobID in jobList:
            jobs.append(self._getARCJob(jobID))

        # JobSupervisor is able to aggregate jobs to perform bulk operations and thus minimizes the communication overhead
        # We still need to create chunks to avoid timeout in the case there are too many jobs to supervise
        for chunk in breakListIntoChunks(jobs, 100):
            job_supervisor = arc.JobSupervisor(self.usercfg, chunk)
            if not job_supervisor.Cancel():
                errorString = " - ".join(jobList).strip()
                return S_ERROR("Failed to kill at least one of these jobs: %s. CE(?) not reachable?" % errorString)

        return S_OK()

    #############################################################################
    def getCEStatus(self):
        """Method to return information on running and pending jobs.
        We hope to satisfy both instances that use robot proxies and those which use proper configurations.
        """

        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

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
            cmd1 = "ldapsearch -x -o ldif-wrap=no -LLL -H ldap://%s:2135  -b 'o=glue' " % self.ceHost
            cmd2 = '"(&(objectClass=GLUE2MappingPolicy)(GLUE2PolicyRule=vo:%s))"' % vo.lower()
            cmd3 = " | grep GLUE2MappingPolicyShareForeignKey | grep %s" % (self.arcQueue)
            cmd4 = " | sed 's/GLUE2MappingPolicyShareForeignKey: /GLUE2ShareID=/' "
            cmd5 = " | xargs -L1 ldapsearch -x -o ldif-wrap=no -LLL -H ldap://%s:2135 -b 'o=glue' " % self.ceHost
            cmd6 = " | egrep '(ShareWaiting|ShareRunning)'"
            res = shellCall(0, cmd1 + cmd2 + cmd3 + cmd4 + cmd5 + cmd6)
            if not res["OK"]:
                self.log.debug("Could not query CE %s - is it down?" % self.ceHost)
                return res
            try:
                ldapValues = res["Value"][1].split("\n")
                running = [lValue for lValue in ldapValues if "GLUE2ComputingShareRunningJobs" in lValue]
                waiting = [lValue for lValue in ldapValues if "GLUE2ComputingShareWaitingJobs" in lValue]
                result["RunningJobs"] = int(running[0].split(":")[1])
                result["WaitingJobs"] = int(waiting[0].split(":")[1])
            except IndexError:
                res = S_ERROR("Unknown ldap failure for site %s" % self.ceHost)
                return res

        return result

    #############################################################################
    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""

        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

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
            self.log.debug("Retrieving status for job %s" % jobID)
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
                    self.log.debug("Killing held job %s" % jobID)
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
    def getJobOutput(self, jobID, workingDirectory=None):
        """Get the specified job standard output and error files.
        Standard output and error are returned as strings.
        If further outputs are retrieved, they are stored in workingDirectory.
        """
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

        if jobID.find(":::") != -1:
            pilotRef, stamp = jobID.split(":::")
        else:
            pilotRef = jobID
            stamp = ""
        if not stamp:
            return S_ERROR("Pilot stamp not defined for %s" % pilotRef)

        job = self._getARCJob(pilotRef)

        arcID = os.path.basename(pilotRef)
        self.log.debug("Retrieving pilot logs for %s" % pilotRef)
        if not workingDirectory:
            if "WorkingDirectory" in self.ceParameters:
                workingDirectory = os.path.join(self.ceParameters["WorkingDirectory"], arcID)
            else:
                workingDirectory = arcID
        outFileName = os.path.join(workingDirectory, "%s.out" % stamp)
        errFileName = os.path.join(workingDirectory, "%s.err" % stamp)
        self.log.debug("Working directory for pilot output %s" % workingDirectory)

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
            self.log.debug("Pilot output = %s" % output)
            self.log.debug("Pilot error = %s" % error)
        else:
            job.Update()
            arcState = job.State.GetGeneralState()
            if arcState != "Undefined":
                return S_ERROR(
                    "Failed to retrieve output for %s as job is not finished (maybe not started yet)" % jobID
                )
            self.log.debug(
                "Could not retrieve pilot output for %s - either permission / proxy error or could not connect to CE"
                % pilotRef
            )
            return S_ERROR("Failed to retrieve output for %s" % jobID)

        return S_OK((output, error))
