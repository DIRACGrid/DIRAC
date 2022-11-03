""" AREX Computing Element (ARC REST interface)
    Using the REST interface now and fail if REST interface is not available.
    A lot of the features are common with the API interface. In particular, the XRSL
    language is used in both cases. So, we retain the xrslExtraString and xrslMPExtraString strings.
"""


__RCSID__ = "$Id$"

import os
import json
import requests

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.Computing.ARCComputingElement import ARCComputingElement

# Note : interiting from ARCComputingElement. See https://github.com/DIRACGrid/DIRAC/pull/5330#discussion_r740907255
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

        The following needed variables are obtained from the CS. If not available, some hopefully
        sensible defaults are set.
        "RESTEndpoint"      - CE is Queried if not available in CS
               - The endpoint we talk to
        "XRSLExtraString" - Default = ""
               - Any CE specific string with additional parameters
        "XRSLMPExtraString" - Default = ""
               - Any CE specific string with additional parameters for MP jobs
        "ARCRESTTimeout"    - DEfault = 1.0 (seconds)
               - Timeout for the rest query
        "proxyTimeLeftBeforeRenewal" - Default = 10000 (seconds)
               - As the name says

        Note : This is not run from __init__ as the design of DIRAC means that ceParameters is
        filled with CEDefaults only at the time this class is initialised for the given CE
        """
        # super()._reset()
        self.log.debug("Testing if the REST interface is available", "for %s" % self.ceName)

        # Get options from the ceParameters dictionary
        self.port = self.ceParameters.get("Port", self.port)
        self.restVersion = self.ceParameters.get("RESTVersion", self.restVersion)
        self.queue = self.ceParameters.get("Queue", self.queue)

        self.proxyTimeLeftBeforeRenewal = self.ceParameters.get(
            "proxyTimeLeftBeforeRenewal", self.proxyTimeLeftBeforeRenewal
        )
        self.arcRESTTimeout = self.ceParameters.get("ARCRESTTimeout", self.arcRESTTimeout)

        # Build the URL based on the CEName, the port and the REST version
        service_url = "https://" + self.ceName + ":" + self.port
        self.base_url = service_url + "/arex/rest/" + self.restVersion + "/"

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
        # Add CE and protocol information to pilot ID
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
        # Remove CE and protocol information from pilot ID
        if "://" in diracJobID:
            arcJobID = diracJobID.split("arex/")[-1]
            return arcJobID
        self.log.warn("Identifier already in REST format?", diracJobID)
        return diracJobID

    def _UrlJoin(self, words):
        # Return a full URL. The base_url is already defined.
        if not isinstance(words, list):
            return "Unknown input : %s" % words
        b_url = self.base_url.strip()
        q_url = b_url if b_url.endswith("/") else b_url + "/"
        for word in words:
            w = str(word).strip()
            w = w if w.endswith("/") else w + "/"
            q_url = q_url + w
        return q_url

    #############################################################################

    def _getDelegation(self, jobID):
        """Here we handle the delegations (Nordugrid language) = Proxy (Dirac language)

        If the jobID is empty:
            Create and upload a new delegation to the CE and return the delegation ID.
            This happens when the call is from the job submission function (self.submitJob).
            We want to attach a delegation to the XRSL strings we submit for each job, so that
            we can update this later if needed.
            More info at
            https://www.nordugrid.org/arc/arc6/users/xrsl.html#delegationid
            https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#delegation-functionality

        If the jobID is not empty:
            Query and return the delegation ID of the given job.
            This happens when the call is from self.renewJobs. This function needs to know the
            delegation associated to the job
            More info at
            https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#jobs-management
            https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#delegations-management

        :param str: job ID
        :return: delegation ID
        """
        # Create a delegation
        if not jobID:
            # Prepare the command
            command = "delegations"
            params = {"action": "new"}
            query = self._UrlJoin([command])
            if query.startswith("Unknown"):
                return S_ERROR("Problem creating REST query %s" % query)

            # Get a proxy
            proxy = X509Chain()
            result = proxy.loadProxyFromFile(self.session.cert)
            if not result["OK"]:
                return S_ERROR("Can't load {}: {} ".format(self.session.cert, result["Message"]))

            # Submit a POST request
            response = self.session.post(
                query,
                data=proxy.dumpAllToString(),
                headers=self.headers,
                params=params,
                timeout=self.arcRESTTimeout,
            )
            delegationID = ""
            if response.ok:
                delegationURL = response.headers.get("location", "")
                if delegationURL:
                    delegationID = delegationURL.split("new/")[-1]
                    # Prepare the command
                    command = "delegations/" + delegationID
                    query = self._UrlJoin([command])
                    if query.startswith("Unknown"):
                        return S_ERROR("Problem creating REST query %s" % query)

                    # Submit the proxy
                    response = self.session.put(
                        query,
                        data=response.text,
                        headers=self.headers,
                        timeout=self.arcRESTTimeout,
                    )
                    if not response.ok:
                        self.log.warn(
                            "Issue while interacting with the delegation",
                            f"{response.status_code} - {response.reason}",
                        )
                        delegationID = ""

            return S_OK(delegationID)

        # Retrieve delegation for existing job
        else:
            # Prepare the command
            command = "jobs"
            params = {"action": "delegations"}
            query = self._UrlJoin([command])
            if query.startswith("Unknown"):
                return S_ERROR("Problem creating REST query %s" % query)

            # Submit the POST request to get the delegation
            jobsJson = {"job": [{"id": jobID}]}
            response = self.session.post(
                query,
                data=json.dumps(jobsJson),
                headers=self.headers,
                timeout=self.arcRESTTimeout,
            )
            delegationID = ""
            if response.ok:  # Check if the job has a delegation
                p = response.json()
                if "delegation_id" in p["job"]:
                    delegationID = p["job"]["delegation_id"][0]
            return S_OK(delegationID)

    #############################################################################

    def submitJob(self, executableFile, proxy, numberOfJobs=1):
        """Method to submit job
        Assume that the ARC queues are always of the format nordugrid-<batchSystem>-<queue>
        And none of our supported batch systems have a "-" in their name
        """
        if not self.session:
            return S_ERROR("REST interface not initialised. Cannot submit jobs.")
        self.log.verbose("Executable file path: %s" % executableFile)

        # Get the name of the queue: nordugrid-<batchsystem>-<queue>
        self.arcQueue = self.queue.split("-", 2)[2]

        # Get a proxy
        result = self._prepareProxy()
        if not result["OK"]:
            self.log.error("Failed to set up proxy", result["Message"])
            return result
        self.session.cert = Locations.getProxyLocation()

        # Prepare the command
        command = "jobs"
        params = {"action": "new"}
        query = self._UrlJoin([command])
        if query.startswith("Unknown"):
            return S_ERROR("Problem creating REST query %s" % query)

        # Get a "delegation" and use the same delegation for all the jobs
        delegation = ""
        result = self._getDelegation("")
        if not result["OK"]:
            self.log.warn("Could not get a delegation", "For CE %s" % self.ceHost)
            self.log.warn("Continue without a delegation")
        else:
            delegation = "(delegationid=%s)" % result["Value"]

        # Submit multiple jobs sequentially.
        # Bulk submission would not be significantly faster than multiple single submission.
        # https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#job-submission-create-a-new-job
        # Also : https://bugzilla.nordugrid.org/show_bug.cgi?id=4069
        batchIDList = []
        stampDict = {}
        for _ in range(numberOfJobs):
            # Get the job into the ARC way
            xrslString, diracStamp = self._writeXRSL(executableFile)
            xrslString += delegation
            self.log.debug("XRSL string submitted", "is %s" % xrslString)
            self.log.debug("DIRAC stamp for job", "is %s" % diracStamp)

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
                    "to CE %s with error - %s - and messages : %s"
                    % (self.ceHost, response.status_code, response.reason),
                )
                break

            responseJob = response.json()["job"]
            if responseJob["status-code"] > "400":
                self.log.warn(
                    "Failed to submit job",
                    "to CE %s with error - %s - and messages: %s"
                    % (self.ceHost, responseJob["status-code"], responseJob["reason"]),
                )
                break

            jobID = responseJob["id"]
            pilotJobReference = self._arcToDiracID(jobID)
            batchIDList.append(pilotJobReference)
            stampDict[pilotJobReference] = diracStamp
            self.log.debug(
                "Successfully submitted job",
                f"{pilotJobReference} to CE {self.ceHost}",
            )

            # At this point, only the XRSL job has been submitted to AREX services
            # Here we also upload the executable.
            command = "jobs/" + jobID + "/session/" + os.path.basename(executableFile)
            query = self._UrlJoin([command])
            if query.startswith("Unknown"):
                return S_ERROR("Problem creating REST query %s" % query)

            # Extract the content of the file
            with open(executableFile) as f:
                content = f.read()

            # Submit the PUT request
            response = self.session.put(query, data=content, headers=self.headers, timeout=self.arcRESTTimeout)
            if response.ok:
                self.log.info("Executable correctly uploaded")

        if batchIDList:
            result = S_OK(batchIDList)
            result["PilotStampDict"] = stampDict
        else:
            result = S_ERROR("No pilot references obtained from the ARC job submission")
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
        command = "jobs"
        params = {"action": "kill"}
        query = self._UrlJoin([command])
        if query.startswith("Unknown"):
            return S_ERROR("Problem creating REST query %s" % query)

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

        self.log.debug("Successfully deleted jobs %s " % (response.json()))
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
        command = "info"
        params = {"schema": "glue2"}
        query = self._UrlJoin([command])
        if query.startswith("Unknown"):
            return S_ERROR("Problem creating REST query %s" % query)

        # Submit the GET request
        response = self.session.get(query, headers=self.headers, params=params, timeout=self.arcRESTTimeout)
        if not response.ok:
            res = S_ERROR("Unknown failure for CE %s. Is the CE down?" % self.ceHost)
            return res
        ceData = response.json()

        # Look only in the relevant section out of the headache
        queueInfo = ceData["Domains"]["AdminDomain"]["Services"]["ComputingService"]["ComputingShare"]
        if not isinstance(queueInfo, list):
            queueInfo = [queueInfo]

        # I have only seen the VO published in lower case ...
        result = S_OK()
        result["SubmittedJobs"] = 0

        magic = self.queue + "_" + vo.lower()
        for i in range(len(queueInfo)):
            if queueInfo[i]["ID"].endswith(magic):
                result["RunningJobs"] = queueInfo[i]["RunningJobs"]
                result["WaitingJobs"] = queueInfo[i]["WaitingJobs"]
                break  # Pick the first (should be only ...) matching queue + VO

        return result

    #############################################################################

    def _renewJobs(self, jobList):
        """Written for the REST interface - jobList is already in the ARC format
        This function is called only by this class, NOT by the SiteDirector
        """
        # Renew the jobs
        for job in jobList:
            # First get the delegation (proxy)
            result = self._getDelegation(job)
            if not result["OK"]:
                self.log.warn("Could not get a delegation from", "Job %s" % job)
                continue
            delegationID = result["Value"]

            # Prepare the command
            command = "delegations/" + delegationID
            params = {"action": "get"}
            query = self._UrlJoin([command])
            if query.startswith("Unknown"):
                return S_ERROR("Problem creating REST query %s" % query)

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
                    f"{job} whose proxy expires at {timeLeft}",
                )
                # Proxy needs to be renewed - try to renew it
                command = "delegations/" + delegationID
                params = {"action": "renew"}
                query = self._UrlJoin([command])
                if query.startswith("Unknown"):
                    return S_ERROR("Problem creating REST query %s" % query)
                response = self.session.post(
                    query,
                    headers=self.headers,
                    params=params,
                    timeout=self.arcRESTTimeout,
                )
                if response.ok:
                    self.log.debug("Proxy successfully renewed", "for job %s" % job)
                else:
                    self.log.debug(
                        "Proxy not renewed",
                        f"for job {job} with delegation {delegationID}",
                    )
            else:  # No need to renew. Proxy is long enough
                continue
        return S_OK()

    #############################################################################

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs"""
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

        # Pilots are stored with a DIRAC stamp (":::XXXXX") appended
        jobList = []
        for j in jobIDList:
            job = j.split(":::")[0]
            jobList.append(job)

        self.log.debug("Getting status of jobs : %s" % jobList)
        jobsJson = {"job": [{"id": self._DiracToArcID(job)} for job in jobList]}

        # Prepare the command
        command = "jobs"
        params = {"action": "status"}
        query = self._UrlJoin([command])
        if query.startswith("Unknown"):
            return S_ERROR("Problem creating REST query %s" % query)

        # Submit the POST request to get status of the pilots
        response = self.session.post(
            query,
            data=json.dumps(jobsJson),
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
        for job in response.json()["job"]:
            jobID = self._arcToDiracID(job["id"])
            # ARC REST interface returns hyperbole
            arcState = job["state"].capitalize()
            self.log.debug("REST ARC status", f"for job {jobID} is {arcState}")
            resultDict[jobID] = self.mapStates[arcState]

            # Renew proxy only of jobs which are running or queuing
            if arcState in ("Running", "Queuing"):
                jobsToRenew.append(job["id"])
            # Cancel held jobs so they don't sit in the queue forever
            if arcState == "Hold":
                jobsToCancel.append(job["id"])
                self.log.debug("Killing held job %s" % jobID)

        # Renew jobs to be renewed
        # Does not work at present - wait for a new release of ARC CEs for this.
        result = self._renewJobs(jobsToRenew)
        if not result["OK"]:
            return result

        # Kill jobs to be killed
        result = self.killJob(jobsToCancel)
        if not result["OK"]:
            return result

        return S_OK(resultDict)

    #############################################################################

    def getJobOutput(self, jobID, _localDir=None):
        """Get the specified job standard output and error files.
        The output is returned as strings.
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
            pilotRef, stamp = jobID.split(":::")
        else:
            pilotRef = jobID
            stamp = ""
        if not stamp:
            return S_ERROR("Pilot stamp not defined for %s" % pilotRef)

        # Prepare the command
        command = "jobs/"
        job = self._DiracToArcID(pilotRef)
        query = self._UrlJoin([command, job, "session", stamp, ".out"])
        if query.startswith("Unknown"):
            return S_ERROR("Problem creating REST query %s" % query)

        # Submit the GET request to retrieve outputs
        response = self.session.get(query, headers=self.headers, timeout=self.arcRESTTimeout)
        if not response.ok:
            self.log.error("Error downloading stdout", f"for {job}: {response.text}")
            return S_ERROR("Failed to retrieve at least some output for %s" % jobID)
        output = response.text

        query = self._UrlJoin([command, job, "session", stamp, ".err"])
        if query.startswith("Unknown"):
            return S_ERROR("Problem creating REST query %s" % query)
        response = self.session.get(query, headers=self.headers, timeout=self.arcRESTTimeout)
        if not response.ok:
            self.log.error("Error downloading stderr", f"for {job}: {response.text}")
            return S_ERROR("Failed to retrieve at least some output for %s" % jobID)
        error = response.text

        return S_OK((output, error))
