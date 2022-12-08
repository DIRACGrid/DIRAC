""" AREX Computing Element (ARC REST interface)
    Using the REST interface now and fail if REST interface is not available.
    A lot of the features are common with the API interface. In particular, the XRSL
    language is used in both cases. So, we retain the xrslExtraString and xrslMPExtraString strings.
"""

import os
import json
import requests

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.Computing.ARCComputingElement import ARCComputingElement


class AREXComputingElement(ARCComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)

        # Default REST port
        self.port = "443"
        # REST version to adopt
        self.restVersion = "1.0"
        # Time left before proxy renewal: 3 hours is a good default
        self.proxyTimeLeftBeforeRenewal = 10800
        # Timeout
        self.arcRESTTimeout = 5.0
        # Request session
        self.session = None
        self.headers = {}
        # URL used to communicate with the REST interface
        self.base_url = ""

    #############################################################################

    def _reset(self):
        """Configure the Request Session to interact with the AREX REST interface.
        Specification : https://www.nordugrid.org/arc/arc6/tech/rest/rest.html
        """
        super()._reset()
        self.log.debug("Testing if the REST interface is available", f"for {self.ceName}")

        # Get options from the ceParameters dictionary
        self.port = self.ceParameters.get("Port", self.port)
        self.restVersion = self.ceParameters.get("RESTVersion", self.restVersion)

        self.proxyTimeLeftBeforeRenewal = self.ceParameters.get(
            "proxyTimeLeftBeforeRenewal", self.proxyTimeLeftBeforeRenewal
        )
        self.arcRESTTimeout = self.ceParameters.get("ARCRESTTimeout", self.arcRESTTimeout)

        # Build the URL based on the CEName, the port and the REST version
        service_url = os.path.join("https://", f"{self.ceName}:{self.port}")
        self.base_url = os.path.join(service_url, "arex", "rest", self.restVersion)

        # Set up the request framework
        self.session = requests.Session()
        self.session.verify = Locations.getCAsLocation()
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        # Attach the token to the headers if present
        if os.environ.get("BEARER_TOKEN"):
            self.headers["Authorization"] = "Bearer " + os.environ["BEARER_TOKEN"]

        return S_OK()

    #############################################################################

    def _arcToDiracID(self, arcJobID):
        """Convert an ARC jobID into a DIRAC jobID.
        Example: 1234 becomes https://<ce>:<port>/arex/1234

        :param str: ARC jobID
        :return: DIRAC jobID
        """
        # Add CE and protocol information to arc Job ID
        if "://" in arcJobID:
            self.log.warn("Identifier already in ARC format", arcJobID)
            return arcJobID

        diracJobID = "https://" + self.ceHost + ":" + self.port + "/arex/" + arcJobID
        return diracJobID

    def _DiracToArcID(self, diracJobID):
        """Convert a DIRAC jobID into an ARC jobID.
        Example: https://<ce>:<port>/arex/1234 becomes 1234

        :param str: DIRAC jobID
        :return: ARC jobID
        """
        # Remove CE and protocol information from arc Job ID
        if "://" in diracJobID:
            arcJobID = diracJobID.split("arex/")[-1]
            return arcJobID
        self.log.warn("Identifier already in REST format?", diracJobID)
        return diracJobID

    def _urlJoin(self, command):
        """Add the command to the base URL.

        :param str command: command to execute
        """
        return os.path.join(self.base_url, command)

    #############################################################################

    def __generateDelegationID(self):
        """Get a delegationID.
        1st step of the process: http://www.nordugrid.org/arc/arc6/tech/rest/rest.html#new-delegation
        Ask the server to create a pair of keys and return a CSR (Certificate Signing Request)

        :return: a tuple containing the delegation ID generated and the CSR.
        """
        # Starts a new delegation process
        params = {"action": "new"}
        query = self._urlJoin("delegations")

        # Submit a POST request
        response = self.session.post(
            query,
            headers=self.headers,
            params=params,
            timeout=self.arcRESTTimeout,
        )
        if not response.ok:
            return S_ERROR(f"Failed to get a delegation ID: {response.status_code} {response.reason}")

        # Extract delegationID from response
        delegationURL = response.headers.get("location", "")
        if not delegationURL:
            return S_ERROR(f"Cannot extract delegation ID from the response: {response.headers}")

        delegationID = delegationURL.split("new/")[-1]
        certificateSigningRequestData = response.text
        return S_OK((delegationID, certificateSigningRequestData))

    def __uploadCertificate(self, delegationID, csrContent):
        """Upload the certificate to the delegation space.
        2nd step of the process: http://www.nordugrid.org/arc/arc6/tech/rest/rest.html#new-delegation
        Sign the CSR and upload it.

        :param str delegationID: identifier of the delegation
        :param str csrContent: content of the CSR to sign
        """
        headers = {"Content-Type": "x-pem-file"}
        query = self._urlJoin(os.path.join("delegations", delegationID))

        # Get a proxy and sign the CSR
        proxy = X509Chain()
        result = proxy.loadProxyFromFile(self.session.cert)
        if not result["OK"]:
            return S_ERROR(f"Can't load {self.session.cert}: {result['Message']}")
        result = proxy.generateChainFromRequestString(csrContent)
        if not result["OK"]:
            return S_ERROR("Problem with the Certificate Signing Request")

        # Submit the certificate
        response = self.session.put(
            query,
            data=result["Value"],
            headers=headers,
            timeout=self.arcRESTTimeout,
        )
        if not response.ok:
            return S_ERROR(
                "Issue while interacting with the delegation",
                f"{response.status_code} - {response.reason}",
            )

        return S_OK()

    def _prepareDelegation(self):
        """Here we handle the delegations (Nordugrid language) = Proxy (Dirac language)

        Create and upload a new delegation to the CE and return the delegation ID.
        This happens when the call is from the job submission function (self.submitJob).
        We want to attach a delegation to the XRSL strings we submit for each job, so that
        we can update this later if needed.
        More info at
        https://www.nordugrid.org/arc/arc6/users/xrsl.html#delegationid
        https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#delegation-functionality
        """
        result = self.__generateDelegationID()
        if not result["OK"]:
            return result
        delegationID, csrContent = result["Value"]

        result = self.__uploadCertificate(delegationID, csrContent)
        if not result["OK"]:
            return result
        return S_OK(delegationID)

    def _getDelegationID(self, arcJobID):
        """Query and return the delegation ID of the given job.

        This happens when the call is from self.renewJobs. This function needs to know the
        delegation associated to the job
        More info at
        https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#jobs-management
        https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#delegations-management

        :param str jobID: ARC job ID
        :return: delegation ID
        """
        query = self._urlJoin("jobs")

        # Submit the POST request to get the delegation
        jobsJson = {"job": [{"id": arcJobID}]}
        response = self.session.post(
            query,
            data=json.dumps(jobsJson),
            headers=self.headers,
            timeout=self.arcRESTTimeout,
        )

        if not response.ok:
            return S_ERROR("Issue while interacting with the delegation", f"{response.status_code} - {response.reason}")

        responseDelegation = response.json()
        if "delegation_id" not in responseDelegation["job"]:
            return S_ERROR(f"Cannot find the Delegation ID for Job {arcJobID}")

        delegationID = responseDelegation["job"]["delegation_id"][0]
        return S_OK(delegationID)

    #############################################################################

    def _getArcJobID(self, executableFile, inputs, outputs, executables, delegation):
        """Get an ARC JobID endpoint to upload executables and inputs.

        :param str executableFile: executable to submit
        :param list inputs: list of input files
        :param list outputs: list of expected output files
        :param list executables: list of secondary executables (will be uploaded with the executable mode)
        :param str delegation: delegation ID

        :return: tuple containing a job ID and a stamp
        """
        # Prepare the command
        params = {"action": "new"}
        query = self._urlJoin("jobs")

        # Get the job into the ARC way
        xrslString, diracStamp = self._writeXRSL(executableFile, inputs, outputs, executables)
        xrslString += delegation
        self.log.debug("XRSL string submitted", f"is {xrslString}")
        self.log.debug("DIRAC stamp for job", f"is {diracStamp}")

        # Submit the POST request
        response = self.session.post(
            query,
            data=xrslString,
            headers=self.headers,
            params=params,
            timeout=self.arcRESTTimeout,
        )
        if not response.ok:
            self.log.warn(
                "Failed to submit job",
                f"to CE {self.ceHost} with error - {response.status_code} - and messages : {response.reason}",
            )
            return S_ERROR("Failed to submit job")

        responseJob = response.json()["job"]
        if responseJob["status-code"] > "400":
            self.log.warn(
                "Failed to submit job",
                f"to CE {self.ceHost} with error - {responseJob['status-code']} - and messages: {responseJob['reason']}",
            )
            return S_ERROR("Failed to submit jobs")

        arcJobID = responseJob["id"]
        return S_OK((arcJobID, diracStamp))

    def _uploadJobDependencies(self, arcJobID, executableFile, inputs, executables):
        """Upload job dependencies so that the job can start.
        This includes the executables and the inputs.

        :param str arcJobID: ARC job ID
        :param str executableFile: executable file
        :param list inputs: inputs required by the executable file
        :param list executables: executables require by the executable file
        """
        filesToSubmit = [executableFile]
        filesToSubmit += executables
        if inputs:
            if not isinstance(inputs, list):
                inputs = [inputs]
            filesToSubmit += inputs

        for fileToSubmit in filesToSubmit:
            queryExecutable = self._urlJoin(os.path.join("jobs", arcJobID, "session", os.path.basename(fileToSubmit)))

            # Extract the content of the file
            with open(fileToSubmit) as f:
                content = f.read()

            # Submit the PUT request
            response = self.session.put(
                queryExecutable, data=content, headers=self.headers, timeout=self.arcRESTTimeout
            )
            if not response.ok:
                self.log.error("Input not uploaded:", fileToSubmit)
                return S_ERROR(f"Input not uploaded: {fileToSubmit}")

            self.log.verbose("Input correctly uploaded", fileToSubmit)
        return S_OK()

    def submitJob(self, executableFile, proxy, numberOfJobs=1, inputs=None, outputs=None):
        """Method to submit job
        Assume that the ARC queues are always of the format nordugrid-<batchSystem>-<queue>
        And none of our supported batch systems have a "-" in their name
        """
        if not self.session:
            return S_ERROR("REST interface not initialised. Cannot submit jobs.")
        self.log.verbose("Executable file path:", executableFile)

        # Get a proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("Failed to set up proxy", result["Message"])
            return result
        self.session.cert = Locations.getProxyLocation()

        # Get a "delegation" and use the same delegation for all the jobs
        delegation = ""
        result = self._prepareDelegation()
        if not result["OK"]:
            self.log.warn("Could not get a delegation", f"For CE {self.ceHost}")
            self.log.warn("Continue without a delegation")
        else:
            delegation = f"\n(delegationid={result['Value']})"

        # If there is a preamble, then we bundle it in an executable file
        executables = []
        if self.preamble:
            executables = [executableFile]
            executableFile = self._bundlePreamble(executableFile)

        # Submit multiple jobs sequentially.
        # Bulk submission would not be significantly faster than multiple single submission.
        # https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#job-submission-create-a-new-job
        # Also : https://bugzilla.nordugrid.org/show_bug.cgi?id=4069
        batchIDList = []
        stampDict = {}
        for _ in range(numberOfJobs):
            result = self._getArcJobID(executableFile, inputs, outputs, executables, delegation)
            if not result["OK"]:
                self.log.error(result["Message"])
                break
            arcJobID, diracStamp = result["Value"]

            # At this point, only the XRSL job has been submitted to AREX services
            # Here we also upload the executable, other executable files and inputs.
            result = self._uploadJobDependencies(arcJobID, executableFile, inputs, executables)
            if not result["OK"]:
                return result

            jobID = self._arcToDiracID(arcJobID)
            batchIDList.append(jobID)
            stampDict[jobID] = diracStamp
            self.log.debug(
                "Successfully submitted job",
                f"{jobID} to CE {self.ceHost}",
            )

        if batchIDList:
            result = S_OK(batchIDList)
            result["PilotStampDict"] = stampDict
        else:
            result = S_ERROR("No ID obtained from the ARC job submission")
        return result

    #############################################################################

    def killJob(self, jobIDList):
        """Kill the specified jobs

        :param list: list of DIRAC Job IDs
        """
        if not self.session:
            return S_ERROR("REST interface not initialised. Cannot kill jobs.")
        self.log.debug("Killing jobs", ",".join(jobIDList))

        # Get a proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("Failed to set up proxy", result["Message"])
            return result
        self.session.cert = Locations.getProxyLocation()

        # List of jobs in json format for the REST query
        jList = [self._DiracToArcID(job) for job in jobIDList]
        jobsJson = {"job": [{"id": job} for job in jList]}

        # Prepare the command
        params = {"action": "kill"}
        query = self._urlJoin("jobs")

        # Killing jobs should be fast
        response = self.session.post(
            query,
            data=json.dumps(jobsJson),
            headers=self.headers,
            params=params,
            timeout=self.arcRESTTimeout,
        )
        if not response.ok:
            return S_ERROR(f"Failed to kill all these jobs: {response.status_code} {response.reason}")

        self.log.debug("Successfully deleted jobs")
        return S_OK()

    #############################################################################

    def getCEStatus(self):
        """Method to return information on running and pending jobs.
        Query the CE directly to get the number of waiting and running jobs for the given
        VO and queue.
        The specification is apparently in glue2 and if you do a raw print out of the information
        it goes on and on as it dumps the full configuration of the CE for all VOs, queues,
        states and parameters. Hopefully this function weeds out everything except the relevant
        queue.
        """
        if not self.session:
            return S_ERROR("REST interface not initialised. Cannot get CE status.")

        # Get a proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("Failed to set up proxy", result["Message"])
            return result
        self.session.cert = Locations.getProxyLocation()

        # Try to find out which VO we are running for.
        # Essential now for REST interface.
        res = getVOfromProxyGroup()
        vo = res["Value"] if res["OK"] else ""

        # Prepare the command
        params = {"schema": "glue2"}
        query = self._urlJoin("info")

        # Submit the GET request
        response = self.session.get(query, headers=self.headers, params=params, timeout=self.arcRESTTimeout)
        if not response.ok:
            res = S_ERROR(f"Unknown failure for CE {self.ceHost}. Is the CE down?")
            return res
        ceData = response.json()

        # Look only in the relevant section out of the headache
        queueInfo = ceData["Domains"]["AdminDomain"]["Services"]["ComputingService"]["ComputingShare"]
        if not isinstance(queueInfo, list):
            queueInfo = [queueInfo]

        # I have only seen the VO published in lower case ...
        result = S_OK()
        result["SubmittedJobs"] = 0

        magic = self.arcQueue + "_" + vo.lower()
        for _, qi in enumerate(queueInfo):
            if qi["ID"].endswith(magic):
                result["RunningJobs"] = int(qi["RunningJobs"])
                result["WaitingJobs"] = int(qi["WaitingJobs"])
                break  # Pick the first (should be only ...) matching queue + VO

        return result

    #############################################################################

    def _renewJobs(self, arcJobList):
        """Written for the REST interface - jobList is already in the ARC format

        :param list arcJobList: list of ARC Job ID
        """
        # Renew the jobs
        for arcJob in arcJobList:
            # First get the delegation (proxy)
            result = self._getDelegationID(arcJob)
            if not result["OK"]:
                self.log.warn("Could not get a delegation from", f"Job {arcJob}")
                continue
            delegationID = result["Value"]

            # Prepare the command
            params = {"action": "get"}
            query = self._urlJoin(os.path.join("delegations", delegationID))

            # Submit the POST request to get the proxy
            response = self.session.post(query, headers=self.headers, params=params, timeout=self.arcRESTTimeout)
            proxy = X509Chain()
            res = proxy.loadChainFromString(response.text)

            # Now test and renew the proxy
            if not res["OK"]:
                continue

            timeLeft = proxy.getRemainingSecs()
            if timeLeft < self.proxyTimeLeftBeforeRenewal:
                self.log.debug(
                    "Renewing proxy for job",
                    f"{arcJob} whose proxy expires at {timeLeft}",
                )
                # Proxy needs to be renewed - try to renew it
                params = {"action": "renew"}
                query = self._urlJoin(os.path.join("delegations", delegationID))

                response = self.session.post(
                    query,
                    headers=self.headers,
                    params=params,
                    timeout=self.arcRESTTimeout,
                )
                if response.ok:
                    self.log.debug("Proxy successfully renewed", f"for job {arcJob}")
                else:
                    self.log.debug(
                        "Proxy not renewed",
                        f"for job {arcJob} with delegation {delegationID}",
                    )
            else:  # No need to renew. Proxy is long enough
                continue
        return S_OK()

    #############################################################################

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs.

        :param list jobIDList: list of DIRAC Job ID, followed by the DIRAC stamp.
        """
        if not self.session:
            return S_ERROR("REST interface not initialised. Cannot get job status.")

        # Get a proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("AREXComputingElement: failed to set up proxy", result["Message"])
            return result
        self.session.cert = Locations.getProxyLocation()

        if not isinstance(jobIDList, list):
            jobIDList = [jobIDList]

        # Jobs are stored with a DIRAC stamp (":::XXXXX") appended
        jobList = []
        for j in jobIDList:
            job = j.split(":::")[0]
            jobList.append(job)

        self.log.debug("Getting status of jobs:", jobList)
        arcJobsJson = {"job": [{"id": self._DiracToArcID(job)} for job in jobList]}

        # Prepare the command
        params = {"action": "status"}
        query = self._urlJoin("jobs")

        # Submit the POST request to get status of the jobs
        response = self.session.post(
            query,
            data=json.dumps(arcJobsJson),
            headers=self.headers,
            params=params,
            timeout=self.arcRESTTimeout,
        )
        if not response.ok:
            self.log.info(
                "Failed getting the status of the jobs",
                f"{response.status_code} - {response.reason}",
            )
            return S_ERROR("Failed getting the status of the jobs")

        resultDict = {}
        jobsToRenew = []
        jobsToCancel = []

        # A single job is returned in a dict, while multiple jobs are returned in a list
        # If a single job is handled, then we must add it to a list to process it
        arcJobsInfo = response.json()["job"]
        if isinstance(arcJobsInfo, dict):
            arcJobsInfo = [arcJobsInfo]

        for arcJob in arcJobsInfo:
            jobID = self._arcToDiracID(arcJob["id"])
            # ARC REST interface returns hyperbole
            arcState = arcJob["state"].capitalize()
            self.log.debug("REST ARC status", f"for job {jobID} is {arcState}")
            resultDict[jobID] = self.mapStates[arcState]

            # Renew proxy only of jobs which are running or queuing
            if arcState in ("Running", "Queuing"):
                jobsToRenew.append(arcJob["id"])
            # Cancel held jobs so they don't sit in the queue forever
            if arcState == "Hold":
                jobsToCancel.append(arcJob["id"])
                self.log.debug(f"Killing held job {jobID}")

        # Renew jobs to be renewed
        # Does not work at present - wait for a new release of ARC CEs for this.
        if jobsToRenew:
            result = self._renewJobs(jobsToRenew)
            if not result["OK"]:
                return result

        # Kill jobs to be killed
        if jobsToCancel:
            result = self.killJob(jobsToCancel)
            if not result["OK"]:
                return result

        return S_OK(resultDict)

    #############################################################################

    def getJobLog(self, jobID):
        """Get job logging info

        :param str jobID: DIRAC JobID followed by the DIRAC stamp.
        :return: string representing the logging info of a given jobID
        """
        if not self.session:
            return S_ERROR("REST interface not initialised. Cannot get job output.")

        # Get a proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("AREXComputingElement: failed to set up proxy", result["Message"])
            return result
        self.session.cert = Locations.getProxyLocation()

        # Extract stamp from the Job ID
        if ":::" in jobID:
            jobID = jobID.split(":::")[0]

        # Prepare the command: Get output files
        arcJob = self._DiracToArcID(jobID)
        query = self._urlJoin(os.path.join("jobs", arcJob, "diagnose", "errors"))

        # Submit the GET request to retrieve outputs
        self.log.debug(f"Retrieving logging info for {jobID}")
        response = self.session.get(query, headers=self.headers, timeout=self.arcRESTTimeout)
        if not response.ok:
            self.log.error("Error downloading logging info", f"for {jobID}: {response.status_code} {response.reason}")
            return S_ERROR(f"Failed to retrieve logging info for {jobID}")
        loggingInfo = response.text

        return S_OK(loggingInfo)

    #############################################################################

    def _getListOfAvailableOutputs(self, jobID, arcJobID):
        """Request a list of outputs available for a given jobID.

        :param str jobID: DIRAC job ID without the DIRAC stamp
        :param str arcJobID: ARC job ID
        :return list: names of the available outputs
        """
        query = self._urlJoin(os.path.join("jobs", arcJobID, "session"))

        # Submit the GET request to retrieve the names of the outputs
        self.log.debug(f"Retrieving the names of the outputs for {jobID}")
        response = self.session.get(query, headers=self.headers, timeout=self.arcRESTTimeout)

        if not response.ok:
            self.log.error("Error getting available outputs", f"for {jobID}: {response.status_code} {response.reason}")
            return S_ERROR(f"Failed to retrieve at least some outputs for {jobID}")

        if not response.text:
            return S_ERROR(f"There is no output for job {jobID}")

        return S_OK(response.json()["file"])

    def getJobOutput(self, jobID, workingDirectory=None):
        """Get the outputs of the given DIRAC job ID.

        Outputs and stored in workingDirectory if present, else in a new directory named <ARC JobID>.

        :param str jobID: DIRAC JobID followed by the DIRAC stamp.
        :param str workingDirectory: name of the directory containing the retrieved outputs.
        :return: content of stdout and stderr
        """
        if not self.session:
            return S_ERROR("REST interface not initialised. Cannot get job output.")

        # Get a proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("AREXComputingElement: failed to set up proxy", result["Message"])
            return result
        self.session.cert = Locations.getProxyLocation()

        # Extract stamp from the Job ID
        if ":::" in jobID:
            jobRef, stamp = jobID.split(":::")
        else:
            return S_ERROR(f"DIRAC stamp not defined for {jobRef}")
        job = self._DiracToArcID(jobRef)

        # Get the list of available outputs
        result = self._getListOfAvailableOutputs(jobRef, job)
        if not result["OK"]:
            return result
        remoteOutputs = result["Value"]
        self.log.debug("Outputs to get are", remoteOutputs)

        # We assume that workingDirectory exists
        if not workingDirectory:
            if "WorkingDirectory" in self.ceParameters:
                workingDirectory = os.path.join(self.ceParameters["WorkingDirectory"], job)
            else:
                workingDirectory = job
            os.mkdir(workingDirectory)

        stdout = None
        stderr = None
        for remoteOutput in remoteOutputs:
            # Prepare the command
            query = self._urlJoin(os.path.join("jobs", job, "session", remoteOutput))

            # Submit the GET request to retrieve outputs
            response = self.session.get(query, headers=self.headers, timeout=self.arcRESTTimeout)
            if not response.ok:
                self.log.error(
                    "Error downloading", f"{remoteOutput} for {job}: {response.status_code} {response.reason}"
                )
                return S_ERROR(f"Failed to retrieve at least some output for {jobID}")
            outputContent = response.text

            if remoteOutput == f"{stamp}.out":
                stdout = outputContent
            elif remoteOutput == f"{stamp}.err":
                stderr = outputContent
            else:
                localOutput = os.path.join(workingDirectory, remoteOutput)
                with open(localOutput, "w") as f:
                    f.write(outputContent)

        return S_OK((stdout, stderr))
