""" ARC Computing Element
    Using the ARC API now

**Configuration Parameters**

Configuration for the ARCComputingElement submission can be done via the configuration system. See the page about
configuring :ref:`resourcesComputing` for where the options can be placed.

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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six
import os
import stat

import arc  # Has to work if this module is called #pylint: disable=import-error
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCESiteMapping
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.File import makeGuid
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.Resources.Computing.PilotBundle import writeScript
from DIRAC.WorkloadManagementSystem.Client import PilotStatus


# Uncomment the following 5 lines for getting verbose ARC api output (debugging)
# import sys
# logstdout = arc.LogStream(sys.stdout)
# logstdout.setFormat(arc.ShortFormat)
# arc.Logger_getRootLogger().addDestination(logstdout)
# arc.Logger_getRootLogger().setThreshold(arc.VERBOSE)

CE_NAME = "ARC"
MANDATORY_PARAMETERS = ["Queue"]  # Mandatory for ARC CEs in GLUE2?
STATES_MAP = {
    "Accepted": PilotStatus.WAITING,
    "Preparing": PilotStatus.WAITING,
    "Submitting": PilotStatus.WAITING,
    "Queuing": PilotStatus.WAITING,
    "Undefined": PilotStatus.UNKNOWN,
    "Running": PilotStatus.RUNNING,
    "Finishing": PilotStatus.RUNNING,
    "Deleted": PilotStatus.ABORTED,
    "Killed": PilotStatus.ABORTED,
    "Failed": PilotStatus.FAILED,
    "Hold": PilotStatus.FAILED,
    "Finished": PilotStatus.DONE,
    "Other": PilotStatus.DONE,
}


class ARCComputingElement(ComputingElement):

    #############################################################################
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super(ARCComputingElement, self).__init__(ceUniqueID)

        self.ceType = CE_NAME
        self.submittedJobs = 0
        self.mandatoryParameters = MANDATORY_PARAMETERS
        self.pilotProxy = ""
        self.queue = ""
        self.gridEnv = ""
        self.ceHost = self.ceName
        self.endpointType = "Gridftp"
        self.usercfg = arc.common.UserConfig()
        self.preamble = ""

        # set the timeout to the default 20 seconds in case the UserConfig constructor did not
        self.usercfg.Timeout(20)  # pylint: disable=pointless-statement
        self.ceHost = self.ceParameters.get("Host", self.ceName)
        self.gridEnv = self.ceParameters.get("GridEnv", self.gridEnv)

        # Used in getJobStatus
        self.mapStates = STATES_MAP
        # Do these after all other initialisations, in case something barks
        self.xrslExtraString = self.__getXRSLExtraString()
        self.xrslMPExtraString = self.__getXRSLExtraString(multiprocessor=True)

    #############################################################################

    def __getARCJob(self, jobID):
        """Create an ARC Job with all the needed / possible parameters defined.
        By the time we come here, the environment variable X509_USER_PROXY should already be set
        """
        j = arc.Job()
        j.JobID = str(jobID)
        j.IDFromEndpoint = os.path.basename(j.JobID)

        if self.endpointType == "Gridftp":
            statURL = "ldap://%s:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=%s)" % (self.ceHost, jobID)
            j.JobStatusURL = arc.URL(str(statURL))
            j.JobStatusInterfaceName = "org.nordugrid.ldapng"

            mangURL = "gsiftp://%s:2811/jobs/" % (self.ceHost)
            j.JobManagementURL = arc.URL(str(mangURL))
            j.JobManagementInterfaceName = "org.nordugrid.gridftpjob"

            j.ServiceInformationURL = j.JobManagementURL
            j.ServiceInformationInterfaceName = "org.nordugrid.ldapng"
        else:
            commonURL = "https://%s:8443/arex" % self.ceHost
            j.JobStatusURL = arc.URL(str(commonURL))
            j.JobStatusInterfaceName = "org.ogf.glue.emies.activitymanagement"

            j.JobManagementURL = arc.URL(str(commonURL))
            j.JobManagementInterfaceName = "org.ogf.glue.emies.activitymanagement"

            j.ServiceInformationURL = arc.URL(str(commonURL))
            j.ServiceInformationInterfaceName = "org.ogf.glue.emies.resourceinfo"

        j.PrepareHandler(self.usercfg)
        return j

    def __getXRSLExtraString(self, multiprocessor=False):
        # For the XRSL additional string from configuration - only done at initialisation time
        # If this string changes, the corresponding (ARC) site directors have to be restarted
        #
        # Variable = XRSLExtraString (or XRSLMPExtraString for multi processor mode)
        # Default value = ''
        #   If you give a value, I think it should be of the form
        #          (aaa = "xxx")
        #   Otherwise the ARC job description parser will have a fit
        # Locations searched in order :
        # Top priority    : Resources/Sites/<Grid>/<Site>/CEs/<CE>/XRSLExtraString
        # Second priority : Resources/Sites/<Grid>/<Site>/XRSLExtraString
        # Default         : Resources/Computing/CEDefaults/XRSLExtraString
        #
        xrslExtraString = ""  # Start with the default value
        result = getCESiteMapping(self.ceHost)
        if not result["OK"] or not result["Value"]:
            self.log.error("Unknown CE ...")
            return
        self.site = result["Value"][self.ceHost]
        # Now we know the site. Get the grid
        grid = self.site.split(".")[0]
        # The different possibilities that we have agreed upon
        if multiprocessor:
            xtraVariable = "XRSLMPExtraString"
        else:
            xtraVariable = "XRSLExtraString"
        firstOption = "Resources/Sites/%s/%s/CEs/%s/%s" % (grid, self.site, self.ceHost, xtraVariable)
        secondOption = "Resources/Sites/%s/%s/%s" % (grid, self.site, xtraVariable)
        defaultOption = "Resources/Computing/CEDefaults/%s" % xtraVariable
        # Now go about getting the string in the agreed order
        self.log.debug("Trying to get %s : first option %s" % (xtraVariable, firstOption))
        result = gConfig.getValue(firstOption, defaultValue="")
        if result != "":
            xrslExtraString = result
            self.log.debug("Found %s : %s" % (xtraVariable, xrslExtraString))
        else:
            self.log.debug("Trying to get %s : second option %s" % (xtraVariable, secondOption))
            result = gConfig.getValue(secondOption, defaultValue="")
            if result != "":
                xrslExtraString = result
                self.log.debug("Found %s : %s" % (xtraVariable, xrslExtraString))
            else:
                self.log.debug("Trying to get %s : default option %s" % (xtraVariable, defaultOption))
                result = gConfig.getValue(defaultOption, defaultValue="")
                if result != "":
                    xrslExtraString = result
                    self.log.debug("Found %s : %s" % (xtraVariable, xrslExtraString))
        if xrslExtraString:
            self.log.always("%s : %s" % (xtraVariable, xrslExtraString))
            self.log.always(" --- to be added to pilots going to CE : %s" % self.ceHost)
        return xrslExtraString

    #############################################################################
    def _addCEConfigDefaults(self):
        """Method to make sure all necessary Configuration Parameters are defined"""
        # First assure that any global parameters are loaded
        ComputingElement._addCEConfigDefaults(self)

    #############################################################################
    def __writeXRSL(self, executableFile, inputs=None, outputs=None, executables=None):
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
                xrslInputs += '(%s "%s")' % (os.path.basename(inputFile), inputFile)

        # Output files to retrieve once the execution is complete
        xrslOutputs = '("%s.out" "") ("%s.err" "")' % (diracStamp, diracStamp)
        if outputs:
            if not isinstance(outputs, list):
                outputs = [outputs]
            for outputFile in outputs:
                xrslOutputs += '(%s "")' % (outputFile)

        xrsl = """
&(executable="%(executable)s")
(inputFiles=(%(executable)s "%(executableFile)s") %(xrslInputAdditions)s)
(stdout="%(diracStamp)s.out")
(stderr="%(diracStamp)s.err")
(outputFiles=%(xrslOutputFiles)s)
(queue=%(queue)s)
%(xrslMPAdditions)s
%(xrslExecutables)s
%(xrslExtraString)s
    """ % {
            "executableFile": executableFile,
            "executable": os.path.basename(executableFile),
            "xrslInputAdditions": xrslInputs,
            "diracStamp": diracStamp,
            "queue": self.arcQueue,
            "xrslOutputFiles": xrslOutputs,
            "xrslMPAdditions": xrslMPAdditions,
            "xrslExecutables": xrslExecutables,
            "xrslExtraString": self.xrslExtraString,
        }

        return xrsl, diracStamp

    def _bundlePreamble(self, executableFile):
        """Bundle the preamble with the executable file"""
        wrapperContent = "%s\n%s" % (self.preamble, executableFile)
        return writeScript(wrapperContent, os.getcwd())

    #############################################################################
    def _reset(self):
        self.queue = self.ceParameters.get("CEQueueName", self.ceParameters["Queue"])
        if "GridEnv" in self.ceParameters:
            self.gridEnv = self.ceParameters["GridEnv"]

        self.preamble = self.ceParameters.get("Preamble", self.preamble)

        # ARC endpoint types (Gridftp, Emies)
        endpointType = self.ceParameters.get("EndpointType", self.endpointType)
        if endpointType not in ["Gridftp", "Emies"]:
            self.log.warn("Unknown ARC endpoint, change to default", self.endpointType)
        else:
            self.endpointType = endpointType
        return S_OK()

    #############################################################################
    def submitJob(self, executableFile, proxy, numberOfJobs=1, inputs=None, outputs=None):
        """Method to submit job"""

        # Assume that the ARC queues are always of the format nordugrid-<batchSystem>-<queue>
        # And none of our supported batch systems have a "-" in their name
        self.arcQueue = self.queue.split("-", 2)[2]
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

        self.log.verbose("Executable file path: %s" % executableFile)
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
            xrslString, diracStamp = self.__writeXRSL(executableFile, inputs, outputs, executables)
            self.log.debug("XRSL string submitted : %s" % xrslString)
            self.log.debug("DIRAC stamp for job : %s" % diracStamp)
            # The arc bindings don't accept unicode objects in Python 2 so xrslString must be explicitly cast
            result = arc.JobDescription_Parse(str(xrslString), jobdescs)
            if not result:
                self.log.error("Invalid job description", "%r, message=%s" % (xrslString, result.str()))
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
                self.log.debug("Successfully submitted job %s to CE %s" % (pilotJobReference, self.ceHost))
            else:
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
                    self.log.warn(
                        "%s Job not submitted - incorrect job description? (missing field in XRSL string?)" % message
                    )
                if result.isSet(arc.SubmissionStatus.SUBMITTER_PLUGIN_NOT_LOADED):  # pylint: disable=no-member
                    self.log.warn("%s SUBMITTER_PLUGIN_NOT_LOADED : ARC library installation problem?" % message)
                if result.isSet(arc.SubmissionStatus.AUTHENTICATION_ERROR):  # pylint: disable=no-member
                    self.log.warn(
                        "%s authentication error - screwed up / expired proxy? Renew / upload pilot proxy on machine?"
                        % message
                    )
                if result.isSet(arc.SubmissionStatus.ERROR_FROM_ENDPOINT):  # pylint: disable=no-member
                    self.log.warn("%s some error from the CE - possibly CE problems?" % message)
                self.log.warn("%s ... maybe above messages will give a hint." % message)
                break  # Boo hoo *sniff*

        if self.preamble:
            os.unlink(executableFile)

        if batchIDList:
            result = S_OK(batchIDList)
            result["PilotStampDict"] = stampDict
        else:
            result = S_ERROR("No pilot references obtained from the ARC job submission")
        return result

    #############################################################################
    def killJob(self, jobIDList):
        """Kill the specified jobs"""

        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

        jobList = list(jobIDList)
        if isinstance(jobIDList, six.string_types):
            jobList = [jobIDList]

        self.log.debug("Killing jobs %s" % jobIDList)
        jobs = []
        for jobID in jobList:
            jobs.append(self.__getARCJob(jobID))

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
            self.log.debug("Running jobs for CE %s : %s" % (self.ceHost, ceStats.RunningJobs))
            self.log.debug("Waiting jobs for CE %s : %s" % (self.ceHost, ceStats.WaitingJobs))
            result["RunningJobs"] = ceStats.RunningJobs
            result["WaitingJobs"] = ceStats.WaitingJobs
        else:
            # The system which works properly at present for ARC CEs that are configured correctly.
            # But for this we need the VO to be known - ask me (Raja) for the whole story if interested.
            # cmd = 'ldapsearch -x -LLL -H ldap://%s:2135 -b mds-vo-name=resource,o=grid "(GlueVOViewLocalID=%s)"' % (
            #     self.ceHost, vo.lower())
            if not self.queue:
                self.log.error("ARCComputingElement: No queue ...")
                res = S_ERROR("Unknown queue (%s) failure for site %s" % (self.queue, self.ceHost))
                return res
            cmd1 = "ldapsearch -x -o ldif-wrap=no -LLL -h %s:2135  -b 'o=glue' " % self.ceHost
            cmd2 = '"(&(objectClass=GLUE2MappingPolicy)(GLUE2PolicyRule=vo:%s))"' % vo.lower()
            cmd3 = " | grep GLUE2MappingPolicyShareForeignKey | grep %s" % (self.queue.split("-")[-1])
            cmd4 = " | sed 's/GLUE2MappingPolicyShareForeignKey: /GLUE2ShareID=/' "
            cmd5 = " | xargs -L1 ldapsearch -x -o ldif-wrap=no -LLL -h %s:2135 -b 'o=glue' " % self.ceHost
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
        if isinstance(jobIDList, six.string_types):
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
            jobs.append(self.__getARCJob(jobID))

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
            self.log.debug("ARC status for job %s is %s" % (jobID, arcState))
            if arcState:  # Meaning arcState is filled. Is this good python?
                resultDict[jobID] = self.mapStates[arcState]
                # Renew proxy only of jobs which are running or queuing
                if arcState in ("Running", "Queuing"):
                    nearExpiry = arc.Time() + arc.Period(10000)  # 2 hours, 46 minutes and 40 seconds
                    if job.ProxyExpirationTime < nearExpiry:
                        # Jobs to renew are aggregated to perform bulk operations
                        jobsToRenew.append(job)
                        self.log.debug(
                            "Renewing proxy for job %s whose proxy expires at %s" % (jobID, job.ProxyExpirationTime)
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
            self.log.debug("DIRAC status for job %s is %s" % (jobID, resultDict[jobID]))

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
    def getJobOutput(self, jobID, localDir=None):
        """Get the specified job standard output and error files. If the localDir is provided,
        the output is returned as file in this directory. Otherwise, the output is returned
        as strings.
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

        job = self.__getARCJob(pilotRef)

        arcID = os.path.basename(pilotRef)
        self.log.debug("Retrieving pilot logs for %s" % pilotRef)
        if "WorkingDirectory" in self.ceParameters:
            workingDirectory = os.path.join(self.ceParameters["WorkingDirectory"], arcID)
        else:
            workingDirectory = arcID
        outFileName = os.path.join(workingDirectory, "%s.out" % stamp)
        errFileName = os.path.join(workingDirectory, "%s.err" % stamp)
        self.log.debug("Working directory for pilot output %s" % workingDirectory)

        isItOkay = job.Retrieve(self.usercfg, arc.URL(str(workingDirectory)), False)
        if isItOkay:
            output = None
            error = None
            try:
                with open(outFileName, "r") as outFile:
                    output = outFile.read()
                os.unlink(outFileName)
                with open(errFileName, "r") as errFile:
                    error = errFile.read()
                os.unlink(errFileName)
            except IOError as e:
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
