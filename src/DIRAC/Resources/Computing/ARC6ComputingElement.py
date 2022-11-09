""" ARC6 Computing Element
    Using the ARC API now

    Temporary ARC Computing Element able to submit to gridftp and arex services
    via the REST and EMI-ES interfaces.
    Use it only if gridftp services are not supported anymore.
    Arc6CE should be dropped once the AREXCE will be fully operational.
"""
import os
import stat

import arc  # Has to work if this module is called #pylint: disable=import-error
from DIRAC import S_OK, S_ERROR
from DIRAC.Resources.Computing.ARCComputingElement import ARCComputingElement


class ARC6ComputingElement(ARCComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)
        # To ease the association between pilots and jobs, we need to remove the "REST" information
        # from the URL generated in submitJob()
        # This should be reconstructed in getARCJob() to retrieve the outputs.
        self.restUrlPart = "rest/1.0/jobs/"

        # Default ComputingInfo Endpoint, used to get details about the queues
        self.computingInfoEndpoint = "org.nordugrid.ldapglue2"

    def _reset(self):
        super()._reset()
        # ComputingInfoEndpoint to get information about queues
        # https://www.nordugrid.org/arc/arc6/users/client_tools.html?#arcinfo
        # Expected values are: [
        # org.nordugrid.ldapng, org.nordugrid.ldapglue2, org.nordugrid.arcrest, org.ogf.glue.emies.resourceinfo
        # ]
        self.computingInfoEndpoint = self.ceParameters.get("ComputingInfoEndpoint", self.computingInfoEndpoint)
        return S_OK()

    def _getARCJob(self, jobID):
        """Create an ARC Job with all the needed / possible parameters defined.
        By the time we come here, the environment variable X509_USER_PROXY should already be set
        """
        j = arc.Job()
        j.IDFromEndpoint = os.path.basename(j.JobID)

        # Get the endpoint type (GridFTP or AREX)
        endpointType = j.JobID.split(":")[0]
        if endpointType == "gsiftp":
            j.JobID = str(jobID)

            statURL = f"ldap://{self.ceHost}:2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid={jobID})"
            j.JobStatusURL = arc.URL(str(statURL))
            j.JobStatusInterfaceName = "org.nordugrid.ldapng"

            mangURL = os.path.dirname(j.JobID)
            j.JobManagementURL = arc.URL(str(mangURL))
            j.JobManagementInterfaceName = "org.nordugrid.gridftpjob"

            j.ServiceInformationURL = j.JobManagementURL
            j.ServiceInformationInterfaceName = "org.nordugrid.ldapng"
        else:
            # We integrate the REST info in the JobID (see further explanation in __init__())
            j.JobID = os.path.join(os.path.dirname(jobID), self.restUrlPart, os.path.basename(jobID))

            commonURL = "/".join(j.JobID.split("/")[0:4])
            j.JobStatusURL = arc.URL(str(commonURL))
            j.JobStatusInterfaceName = "org.nordugrid.arcrest"

            j.JobManagementURL = arc.URL(str(commonURL))
            j.JobManagementInterfaceName = "org.nordugrid.arcrest"

            j.ServiceInformationURL = arc.URL(str(commonURL))
            j.ServiceInformationInterfaceName = "org.nordugrid.arcrest"

        j.PrepareHandler(self.usercfg)
        return j

    def submitJob(self, executableFile, proxy, numberOfJobs=1):
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

        batchIDList = []
        stampDict = {}

        # Creating an endpoint
        endpoint = arc.Endpoint(self.ceHost, arc.Endpoint.COMPUTINGINFO, self.computingInfoEndpoint)

        # Get the ExecutionTargets of the ComputingElement (Can be REST, EMI-ES or GRIDFTP)
        retriever = arc.ComputingServiceRetriever(self.usercfg, [endpoint])
        retriever.wait()
        targetsWithQueues = list(retriever.GetExecutionTargets())

        # Targets also include queues
        # To avoid losing time trying to submit to queues we cannot interact with, we only keep the interesting ones
        targets = []
        for target in targetsWithQueues:
            if target.ComputingShare.Name == self.arcQueue:
                self.log.debug(
                    "Adding target:",
                    f"{target.ComputingEndpoint.URLString} ({target.ComputingEndpoint.InterfaceName})",
                )
                targets.append(target)

        # At this point, we should have GRIDFTP and AREX (EMI-ES and REST) targets related to arcQueue
        # We intend to submit to AREX first, if it does not work, GRIDFTP is used
        submissionWorked = False
        for target in targets:
            # If the submission is already done, we stop
            if submissionWorked:
                break

            for __i in range(numberOfJobs):

                # The basic job description
                jobdescs = arc.JobDescriptionList()

                # Get the job into the ARC way
                xrslString, diracStamp = self._writeXRSL(executableFile)
                self.log.debug("XRSL string submitted : %s" % xrslString)
                self.log.debug("DIRAC stamp for job : %s" % diracStamp)

                # The arc bindings don't accept unicode objects in Python 2 so xrslString must be explicitly cast
                result = arc.JobDescription.Parse(str(xrslString), jobdescs)
                if not result:
                    self.log.error("Invalid job description", f"{xrslString!r}, message={result.str()}")
                    break

                # Submit the job
                job = arc.Job()
                result = target.Submit(self.usercfg, jobdescs[0], job)

                # Save info or else ..else.
                if result == arc.SubmissionStatus.NONE:
                    # Job successfully submitted
                    pilotJobReference = job.JobID

                    # Remove the REST part from the URL obtained (see explanation in __init__())
                    pilotJobReference = pilotJobReference.replace(self.restUrlPart, "")

                    batchIDList.append(pilotJobReference)
                    stampDict[pilotJobReference] = diracStamp
                    submissionWorked = True
                    self.log.debug(f"Successfully submitted job {pilotJobReference} to CE {self.ceHost}")
                else:
                    self._analyzeSubmissionError(result)
                    break  # Boo hoo *sniff*

        if batchIDList:
            result = S_OK(batchIDList)
            result["PilotStampDict"] = stampDict
        else:
            result = S_ERROR("No pilot references obtained from the ARC job submission")
        return result

    def getCEStatus(self):
        """Method to return information on running and pending jobs.
        We hope to satisfy both instances that use robot proxies and those which use proper configurations.
        """

        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("ARCComputingElement: failed to set up proxy", result["Message"])
            return result
        self.usercfg.ProxyPath(os.environ["X509_USER_PROXY"])

        # Creating an endpoint
        endpoint = arc.Endpoint(self.ceHost, arc.Endpoint.COMPUTINGINFO, self.computingInfoEndpoint)

        # Get the ExecutionTargets of the ComputingElement (Can be REST, EMI-ES or GRIDFTP)
        retriever = arc.ComputingServiceRetriever(self.usercfg, [endpoint])
        retriever.wait()  # Takes a bit of time to get and parse the ldap information
        targetsWithQueues = retriever.GetExecutionTargets()

        # Targets also include queues
        # Some of them might be used by different VOs
        targets = []
        for target in targetsWithQueues:
            if target.ComputingShare.Name == self.arcQueue:
                self.log.debug(
                    "Adding target:",
                    f"{target.ComputingEndpoint.URLString} ({target.ComputingEndpoint.InterfaceName})",
                )
                targets.append(target)

        # We extract stat from the AREX service (targets[0])
        ceStats = targets[0].ComputingShare
        self.log.debug(f"Running jobs for CE {self.ceHost} : {ceStats.RunningJobs}")
        self.log.debug(f"Waiting jobs for CE {self.ceHost} : {ceStats.WaitingJobs}")

        result = S_OK()
        result["SubmittedJobs"] = 0
        result["RunningJobs"] = ceStats.RunningJobs
        result["WaitingJobs"] = ceStats.WaitingJobs

        return result
