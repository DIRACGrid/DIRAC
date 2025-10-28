"""AREX Computing Element (ARC REST interface)

Allows interacting with ARC AREX services via a REST interface.

**Configuration Parameters**

Configuration for the AREXComputingElement submission can be done via the configuration system.
Below, you can find a list of parameters specific to the AREX CE.

Preamble:
   Line that should be executed just before the executable file.

Timeout:
   Duration in seconds before declaring a timeout exception.

Port:
   Port added to the CE host name to interact with AREX services.

ProxyTimeLeftBeforeRenewal:
   Time in seconds before the AREXCE renews proxy of submitted payloads.

RESTVersion:
   Version of the REST interface to use.

XRSLExtraString:
   Default additional string for ARC submit files. Should be written in the following format::

     (key = "value")

   Times (wall/CPU time) should be given in seconds (see https://www.nordugrid.org/arc/arc6/users/xrsl.html?#cputime).

XRSLMPExtraString:
   Default additional string for ARC submit files for multi-processor jobs. Should be written in the following format::

     (key = "value")

   The XRSLExtraString note about times also applies to this configuration option.

AlwaysIncludeProxy:
    A boolean, set to true to include the proxy in job submission even
    in cases where tokens are the primary authentication method.
    (Recommended for ARC6 tokens, deprecated for ARC7)

**Code Documentation**
"""

import json
import os
import shutil
import stat
import tempfile
import uuid

import requests

from DIRAC import S_ERROR, S_OK
from DIRAC.Core.Security import Locations
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.Core.Security.X509Chain import X509Chain  # pylint: disable=import-error
from DIRAC.Resources.Computing.ComputingElement import ComputingElement
from DIRAC.WorkloadManagementSystem.Client import PilotStatus

MANDATORY_PARAMETERS = ["Queue"]

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
    "None": PilotStatus.ABORTED,
}


class AREXComputingElement(ComputingElement):
    def __init__(self, ceUniqueID):
        """Standard constructor."""
        super().__init__(ceUniqueID)
        self.mandatoryParameters = MANDATORY_PARAMETERS

        # Name of the queue
        self.queue = ""
        # Preamble to add to the executable
        self.preamble = ""
        # Used in getJobStatus
        self.mapStates = STATES_MAP

        # Extra XRSL info
        self.xrslExtraString = ""
        self.xrslMPExtraString = ""

        # Default REST port
        self.port = "443"
        # REST version to adopt
        self.restVersion = "1.0"
        # Time left before proxy renewal: 3 hours is a good default
        self.proxyTimeLeftBeforeRenewal = 10800
        # Timeout
        self.timeout = 5.0
        # Request session
        self.session = None
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        # URL used to communicate with the REST interface
        self.base_url = ""
        # A flag to always include a proxy, even if a token is the primary auth method
        self.alwaysIncludeProxy = False

    #############################################################################

    def _reset(self):
        """Configure the Request Session to interact with the AREX REST interface.
        Specification : https://www.nordugrid.org/arc/arc6/tech/rest/rest.html
        """
        # Queue parameters
        # Assume that the ARC queues are always of the format nordugrid-<batchSystem>-<queue>
        # And none of our supported batch systems have a "-" in their name
        queue = self.ceParameters.get("CEQueueName", self.ceParameters["Queue"])
        self.queue = queue.split("-", 2)[2]

        self.preamble = self.ceParameters.get("Preamble", self.preamble)

        self.xrslExtraString = self.ceParameters.get("XRSLExtraString", self.xrslExtraString)
        self.xrslMPExtraString = self.ceParameters.get("XRSLMPExtraString", self.xrslMPExtraString)

        # REST parameters
        self.port = self.ceParameters.get("Port", self.port)
        self.restVersion = self.ceParameters.get("RESTVersion", self.restVersion)
        self.timeout = float(self.ceParameters.get("Timeout", self.timeout))

        # Credentials parameters
        self.audienceName = f"https://{self.ceName}:{self.port}"

        self.proxyTimeLeftBeforeRenewal = self.ceParameters.get(
            "ProxyTimeLeftBeforeRenewal", self.proxyTimeLeftBeforeRenewal
        )

        # Build the URL based on the CEName, the port and the REST version
        service_url = os.path.join("https://", f"{self.ceName}:{self.port}")
        self.base_url = os.path.join(service_url, "arex", "rest", self.restVersion)

        self.alwaysIncludeProxy = False
        if self.ceParameters.get("AlwaysIncludeProxy", "false").lower() in ("true", "yes"):
            self.alwaysIncludeProxy = True

        # Set up the request framework
        self.session = requests.Session()
        self.session.verify = Locations.getCAsLocation()

        return S_OK()

    #############################################################################

    def setToken(self, token):
        """Set the token and update the headers

        :param token: OAuth2Token object or dictionary containing token structure
        """
        super().setToken(token)
        self.headers["Authorization"] = "Bearer " + self.token["access_token"]

    def _arcIDToJobReference(self, arcJobID):
        """Convert an ARC jobID into a job reference.
        Example: 1234 becomes https://<ce>:<port>/arex/1234

        :param str: ARC jobID
        :return: job reference, defined as an ARC jobID with additional details
        """
        # Add CE and protocol information to arc Job ID
        if "://" in arcJobID:
            self.log.warn("Identifier already in ARC format", arcJobID)
            return arcJobID

        return f"https://{self.ceName}:{self.port}/arex/{arcJobID}"

    def _jobReferenceToArcID(self, jobReference):
        """Convert a job reference into an ARC jobID.
        Example: https://<ce>:<port>/arex/1234 becomes 1234

        :param str: job reference, defined as an ARC jobID with additional details
        :return: ARC jobID
        """
        # Remove CE and protocol information from arc Job ID
        if "://" in jobReference:
            arcJobID = jobReference.split("arex/")[-1]
            return arcJobID
        self.log.warn("Identifier already in REST format?", jobReference)
        return jobReference

    #############################################################################

    def _urlJoin(self, command):
        """Add the command to the base URL.

        :param str command: command to execute
        """
        return os.path.join(self.base_url, command)

    def _request(self, method, query, params=None, data=None, headers=None, timeout=None, stream=False):
        """Perform a request and properly handle the results/exceptions.

        :param str method: "post", "get", "put"
        :param str query: query to submit
        :param dict/str params: parameters of the query
        :param dict headers: headers of the query
        :param int timeout: timeout value
        :return: response
        """
        if not headers:
            headers = self.headers
        if not timeout:
            timeout = self.timeout

        if method.upper() not in ["GET", "POST", "PUT"]:
            return S_ERROR("The request method is unknown")

        try:
            response = self.session.request(
                method, query, headers=headers, params=params, data=data, timeout=timeout, stream=stream
            )
            if not response.ok:
                return S_ERROR(f"Response: {response.status_code} - {response.reason}")
            return S_OK(response)
        except requests.Timeout as e:
            return S_ERROR(f"Request timed out, consider increasing the Timeout value: {e}")
        except requests.ConnectionError as e:
            return S_ERROR(f"Connection failed, consider checking the state of the CE: {e}")
        except requests.RequestException as e:
            return S_ERROR(f"Request exception: {e}")

    def _checkSession(self):
        """Check that the session exists and carries a valid proxy."""
        if not self.session:
            return S_ERROR("REST interface not initialised.")

        # Reinitialize the authentication parameters
        self.session.cert = None
        self.headers.pop("Authorization", None)

        # We need a token or a proxy to interact with the REST interface
        if not (self.token or self.proxy):
            self.log.error("Proxy or token not set")
            return S_ERROR("Proxy or token not set")

        # If a token is set, we use it
        if self.token:
            # Attach the token to the headers if present
            self.headers["Authorization"] = f"Bearer {self.token['access_token']}"
            self.log.verbose("A token is attached to the header of the request(s)")
        if not self.token or self.alwaysIncludeProxy:
            # Prepare the proxy in X509_USER_PROXY
            if not (result := self._prepareProxy())["OK"]:
                self.log.error("Failed to set up proxy", result["Message"])
                return result

            # Attach the proxy to the session
            self.session.cert = os.environ.get("X509_USER_PROXY")
            self.log.verbose("A proxy is attached to the session")

        return S_OK()

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
        result = self._request("post", query, params=params)
        if not result["OK"]:
            self.log.error("Failed to get a delegation ID.", result["Message"])
            return S_ERROR("Failed to get a delegation ID")
        response = result["Value"]

        # Extract delegationID from response
        delegationURL = response.headers.get("location", "")
        if not delegationURL:
            return S_ERROR(f"Cannot extract delegation ID from the response: {response.headers}")

        delegationID = delegationURL.split("/")[-1]
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

        # Sign the CSR
        result = self.proxy.generateChainFromRequestString(csrContent)
        if not result["OK"]:
            self.log.error("Problem with the Certificate Signing Request:", result["Message"])
            return S_ERROR("Problem with the Certificate Signing Request")

        # Submit the certificate
        result = self._request("put", query, data=result["Value"], headers=headers)
        if not result["OK"]:
            self.log.error("Issue while interacting with the delegation.", result["Message"])
            return S_ERROR("Issue while interacting with the delegation")

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

    def _getDelegationIDs(self):
        """Query and return the delegation IDs.

        This happens when the call is from self.renewDelegation.
        More info at
        https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#jobs-management
        https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#delegations-management

        :return: list of delegation IDs
        """
        query = self._urlJoin("delegations")

        # Submit the POST request to get the delegation
        result = self._request("get", query)
        if not result["OK"]:
            self.log.warn("Issue while interacting with the delegations.", result["Message"])
            return result
        response = result["Value"]

        # If there is no delegation, response.json is expected to return an exception
        try:
            responseDelegation = response.json()
        except requests.JSONDecodeError:
            return S_OK([])

        # This is not expected
        if "delegation" not in responseDelegation:
            return S_OK([])

        # If there is a single delegationID, then we get an str instead of a list
        # Not specified in the documentation
        delegations = responseDelegation["delegation"]
        if isinstance(delegations, dict):
            delegations = [delegations]

        # responseDelegation should be {'delegation': [{'id': <delegationID>}, ...]}
        delegationIDs = [delegationContent["id"] for delegationContent in delegations]
        return S_OK(delegationIDs)

    def _getProxyFromDelegationID(self, delegationID):
        """Get proxy stored within the delegation

        :param str delegationID: delegation ID
        """
        query = self._urlJoin(os.path.join("delegations", delegationID))
        params = {"action": "get"}

        # Submit the POST request to get the delegation
        result = self._request("post", query, params=params)
        if not result["OK"]:
            self.log.error("Could not get a proxy for", f"delegation {delegationID}: {result['Message']}")
            return S_ERROR(f"Could not get a proxy for delegation {delegationID}: {result['Message']}")
        response = result["Value"]

        proxy = X509Chain()
        result = proxy.loadChainFromString(response.text)
        if not result["OK"]:
            self.log.error(
                "Issue while trying to load proxy content from delegation", f"{delegationID}: {result['Message']}"
            )
            return result

        return S_OK(proxy)

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
            queue=self.queue,
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

        with tempfile.NamedTemporaryFile(
            "w", delete=False, prefix="preamble-", suffix=f"-{executableFile}"
        ) as bundleFile:
            bundleFile.write(wrapperContent)
            return bundleFile.name

    def _getArcJobID(self, executableFile, inputs, outputs, delegation):
        """Get an ARC JobID endpoint to upload executables and inputs.

        :param str executableFile: executable to submit
        :param list inputs: list of input files
        :param list outputs: list of expected output files
        :param str delegation: delegation ID

        :return: tuple containing a job ID and a stamp
        """
        # Prepare the command
        params = {"action": "new"}
        query = self._urlJoin("jobs")

        # Get the job into the ARC way
        xrslString, diracStamp = self._writeXRSL(executableFile, inputs, outputs)
        xrslString += delegation
        self.log.debug("XRSL string submitted", f"is {xrslString}")
        self.log.debug("DIRAC stamp for job", f"is {diracStamp}")

        # Submit the POST request
        result = self._request("post", query, params=params, data=xrslString)
        if not result["OK"]:
            self.log.error("Failed to submit job.", result["Message"])
            return S_ERROR("Failed to submit job")
        response = result["Value"]

        responseJob = response.json()["job"]
        if isinstance(responseJob, list):
            responseJob = responseJob[0]

        if responseJob["status-code"] > "400":
            self.log.warn(
                "Failed to submit job",
                f"to CE {self.ceName} with error - {responseJob['status-code']} - and messages: {responseJob['reason']}",
            )
            return S_ERROR("Failed to submit jobs")

        arcJobID = responseJob["id"]
        return S_OK((arcJobID, diracStamp))

    def _uploadJobDependencies(self, arcJobID, executableFile, inputs):
        """Upload job dependencies so that the job can start.
        This includes the executables and the inputs.

        :param str arcJobID: ARC job ID
        :param str executableFile: executable file
        :param list inputs: inputs required by the executable file
        """
        filesToSubmit = [executableFile]
        filesToSubmit += inputs

        for fileToSubmit in filesToSubmit:
            queryExecutable = self._urlJoin(os.path.join("jobs", arcJobID, "session", os.path.basename(fileToSubmit)))

            # Extract the content of the file
            with open(fileToSubmit) as f:
                content = f.read()

            # Submit the PUT request
            result = self._request("put", queryExecutable, data=content)
            if not result["OK"]:
                self.log.error("Input not uploaded", f"{fileToSubmit}: {result['Message']}")
                return S_ERROR(f"Input not uploaded: {fileToSubmit}")

            self.log.verbose("Input correctly uploaded", fileToSubmit)
        return S_OK()

    def submitJob(self, executableFile, proxy, numberOfJobs=1, inputs=None, outputs=None):
        """Method to submit job
        Assume that the ARC queues are always of the format nordugrid-<batchSystem>-<queue>
        And none of our supported batch systems have a "-" in their name
        """
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot submit jobs", result["Message"])
            return result

        self.log.verbose(f"Executable file path: {executableFile}")

        if not inputs:
            inputs = []
        if not outputs:
            outputs = []

        # Delegation cannot be used with a token
        delegation = ""
        if not self.token or self.alwaysIncludeProxy:
            # Get existing delegations
            result = self._getDelegationIDs()
            if not result["OK"]:
                self.log.error("Could not get delegation IDs.", result["Message"])
                return S_ERROR("Could not get delegation IDs")
            delegationIDs = result["Value"]

            # Get the delegationID which corresponds to the DIRAC group of the proxy if it exists
            currentDelegationID = None
            proxyGroup = self.proxy.getDIRACGroup()
            for delegationID in delegationIDs:
                # Get the proxy attached to the delegationID
                result = self._getProxyFromDelegationID(delegationID)

                # Bug in AREX, sometimes delegationID does not exist anymore,
                # but still appears in the list of delegationIDs.
                # Issue submitted here: https://bugzilla.nordugrid.org/show_bug.cgi?id=4133
                # In this case, we just try with the next one
                if not result["OK"] and "404" in result["Message"]:
                    continue

                # Else, it means there was an issue with the CE,
                # we stop the execution
                if not result["OK"]:
                    return result

                proxy = result["Value"]

                if proxy.getDIRACGroup() != proxyGroup:
                    continue

                # If we are here, we have found the right delegationID to use
                currentDelegationID = delegationID
                # Renew the delegation just in case
                if not self.token or self.alwaysIncludeProxy:
                    result = self._renewDelegation(currentDelegationID)
                    if not result["OK"]:
                        self.log.warn("Could not renew the delegation", f"{currentDelegationID}: {result['Message']}")
                break

            if not currentDelegationID:
                # No existing delegation, we need to prepare one
                result = self._prepareDelegation()
                if not result["OK"]:
                    self.log.warn("Could not get a new delegation", f"for CE {self.ceName}")
                    return S_ERROR("Could not get a new delegation")
                currentDelegationID = result["Value"]

            delegation = f"\n(delegationid={currentDelegationID})"

        # If there is a preamble, then we bundle it in an executable file
        if self.preamble:
            inputs.append(executableFile)
            executableFile = self._bundlePreamble(executableFile)

        # Submit multiple jobs sequentially.
        # Bulk submission would not be significantly faster than multiple single submission.
        # https://www.nordugrid.org/arc/arc6/tech/rest/rest.html#job-submission-create-a-new-job
        # Also : https://bugzilla.nordugrid.org/show_bug.cgi?id=4069
        batchIDList = []
        stampDict = {}
        for _ in range(numberOfJobs):
            result = self._getArcJobID(executableFile, inputs, outputs, delegation)
            if not result["OK"]:
                break
            arcJobID, diracStamp = result["Value"]

            # At this point, only the XRSL job has been submitted to AREX services
            # Here we also upload the executable, other executable files and inputs.
            result = self._uploadJobDependencies(arcJobID, executableFile, inputs)
            if not result["OK"]:
                break

            jobReference = self._arcIDToJobReference(arcJobID)
            batchIDList.append(jobReference)
            stampDict[jobReference] = diracStamp
            self.log.debug(
                "Successfully submitted job",
                f"{jobReference} to CE {self.ceName}",
            )

        # Remove the bundled preamble
        if self.preamble:
            os.remove(executableFile)

        if batchIDList:
            result = S_OK(batchIDList)
            result["PilotStampDict"] = stampDict
        else:
            result = S_ERROR("No ID obtained from the ARC job submission")
        return result

    #############################################################################

    def killJob(self, jobIDList):
        """Kill the specified jobs

        :param list jobIDList: list of Job references
        """
        if not isinstance(jobIDList, list):
            jobIDList = [jobIDList]
        self.log.debug("Killing jobs", ",".join(jobIDList))

        # Convert job references to ARC jobs
        # Job references might be stored with a DIRAC stamp (":::XXXXX") that should be removed
        arcJobList = [self._jobReferenceToArcID(job.split(":::")[0]) for job in jobIDList]
        return self._killJob(arcJobList)

    def _killJob(self, arcJobList):
        """Kill the specified jobs

        :param list arcJobList: list of ARC Job IDs
        """
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot kill jobs", result["Message"])
            return result

        # List of jobs in json format for the REST query
        jobsJson = {"job": [{"id": job} for job in arcJobList]}

        # Prepare the command
        params = {"action": "kill"}
        query = self._urlJoin("jobs")

        # Killing jobs should be fast
        result = self._request("post", query, params=params, data=json.dumps(jobsJson))
        if not result["OK"]:
            self.log.error("Failed to kill all these jobs.", result["Message"])
            return S_ERROR("Failed to kill all these jobs")

        self.log.debug("Successfully deleted jobs")
        return S_OK()

    #############################################################################

    def cleanJob(self, jobIDList):
        """Clean files related to the specified jobs

        :param list jobIDList: list of job references
        """
        if not isinstance(jobIDList, list):
            jobIDList = [jobIDList]
        self.log.debug("Cleaning jobs", ",".join(jobIDList))

        # Convert job references to ARC jobs
        # Job references might be stored with a DIRAC stamp (":::XXXXX") that should be removed
        arcJobList = [self._jobReferenceToArcID(job.split(":::")[0]) for job in jobIDList]
        return self._cleanJob(arcJobList)

    def _cleanJob(self, arcJobList):
        """Clean files related to the specified jobs

        :param list jobIDList: list of ARC Job IDs
        """
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot clean jobs", result["Message"])
            return result

        # List of jobs in json format for the REST query
        jobsJson = {"job": [{"id": job} for job in arcJobList]}

        # Prepare the command
        params = {"action": "clean"}
        query = self._urlJoin("jobs")

        # Cleaning jobs
        result = self._request("post", query, params=params, data=json.dumps(jobsJson))
        if not result["OK"]:
            self.log.error("Failed to clean all these jobs.", result["Message"])
            return S_ERROR("Failed to clean all these jobs")

        self.log.debug("Successfully cleaned jobs")
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
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot get CE Status", result["Message"])
            return result

        # Find out which VO we are running for.
        # Essential now for REST interface.
        result = getVOfromProxyGroup()
        if not result["OK"]:
            return result
        if not result["Value"]:
            return S_ERROR("Could not get VO value from the proxy group")
        vo = result["Value"]

        # Prepare the command
        params = {"schema": "glue2"}
        query = self._urlJoin("info")

        # Submit the GET request
        result = self._request("get", query, params=params)
        if not result["OK"]:
            self.log.error("Failed getting the status of the CE.", result["Message"])
            return S_ERROR("Failed getting the status of the CE")
        response = result["Value"]
        try:
            ceData = response.json()
        except requests.JSONDecodeError:
            self.log.exception("Failed decoding the status of the CE")
            return S_ERROR(f"Failed decoding the status of the CE")

        # Look only in the relevant section out of the headache
        # This "safe_get" function allows to go down the dictionary
        # even if some elements are lists instead of dictionaries
        # and returns None if any element is not found
        # FIXME: this is a temporary measure to be removed after https://github.com/DIRACGrid/DIRAC/issues/8354
        def safe_get(d, *keys):
            for k in keys:
                if isinstance(d, list):
                    d = d[0]  # assume first element
                d = d.get(k) if isinstance(d, dict) else None
                if d is None:
                    break
            return d

        queueInfo = safe_get(ceData, "Domains", "AdminDomain", "Services", "ComputingService", "ComputingShare")
        if queueInfo is None:
            self.log.error("Failed to extract queue info")

        if not isinstance(queueInfo, list):
            queueInfo = [queueInfo]

        # I have only seen the VO published in lower case ...
        result = S_OK()
        result["SubmittedJobs"] = 0

        magic = self.queue + "_" + vo.lower()
        for qi in queueInfo:
            if qi["ID"].endswith(magic):
                result["RunningJobs"] = int(qi["RunningJobs"])
                result["WaitingJobs"] = int(qi["WaitingJobs"]) + int(qi["StagingJobs"]) + int(qi["PreLRMSWaitingJobs"])
                break  # Pick the first (should be only ...) matching queue + VO
        else:
            return S_ERROR(f"Could not find the queue {self.queue} associated to VO {vo}")

        return result

    #############################################################################

    def _renewDelegation(self, delegationID):
        """Renew the delegation if needed

        :params delegationID: delegation ID to renew
        """
        result = self._getProxyFromDelegationID(delegationID)
        if not result["OK"]:
            return result
        proxy = result["Value"]

        # Do we have the right proxy to perform a delegation renewal?
        if self.proxy.getDIRACGroup() != proxy.getDIRACGroup():
            return S_OK()

        # Check if the proxy is long enough
        result = proxy.getRemainingSecs()
        if not result["OK"]:
            self.log.error(
                "Could not get remaining time from the proxy for",
                f"delegation {delegationID}: {result['Message']}",
            )
            return S_ERROR(f"Could not get remaining time from the proxy for delegation {delegationID}")
        timeLeft = result["Value"]

        if timeLeft >= self.proxyTimeLeftBeforeRenewal:
            # No need to renew. Proxy is long enough
            return S_OK()

        self.log.notice(
            "Renewing delegation",
            f"{delegationID} whose proxy expires at {timeLeft}",
        )
        # Proxy needs to be renewed - try to renew it

        # First, get a new CSR from the delegation
        params = {"action": "renew"}
        query = self._urlJoin(os.path.join("delegations", delegationID))
        result = self._request("post", query, params=params)
        if not result["OK"]:
            self.log.error(
                "Proxy not renewed, failed to get CSR",
                f"for delegation {delegationID}",
            )
            return S_ERROR(f"Proxy not renewed, failed to get CSR for delegation {delegationID}")
        response = result["Value"]

        # Then, sign and upload the certificate
        result = self.__uploadCertificate(delegationID, response.text)
        if not result["OK"]:
            self.log.error(
                "Proxy not renewed, failed to send renewed proxy",
                f"delegation {delegationID}: {result['Message']}",
            )
            return S_ERROR(f"Proxy not renewed, failed to send renewed proxy for delegation {delegationID}")

        self.log.verbose("Proxy successfully renewed", f"for delegation {delegationID}")

        return S_OK()

    def getJobStatus(self, jobIDList):
        """Get the status information for the given list of jobs.

        :param list jobIDList: list of job references, followed by the DIRAC stamp.
        """
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot get status of the jobs", result["Message"])
            return result

        if not isinstance(jobIDList, list):
            jobIDList = [jobIDList]

        self.log.debug("Getting status of jobs:", jobIDList)
        # Convert job references to ARC jobs and encapsulate them in a dictionary for the REST query
        # Job references might be stored with a DIRAC stamp (":::XXXXX") that should be removed
        arcJobsJson = {"job": [{"id": self._jobReferenceToArcID(job.split(":::")[0])} for job in jobIDList]}

        # Prepare the command
        params = {"action": "status"}
        query = self._urlJoin("jobs")

        # Submit the POST request to get status of the jobs
        result = self._request("post", query, params=params, data=json.dumps(arcJobsJson))
        if not result["OK"]:
            self.log.error("Failed getting the status of the jobs.", result["Message"])
            return S_ERROR("Failed getting the status of the jobs")
        response = result["Value"]

        resultDict = {}
        jobsToCancel = []

        # A single job is returned in a dict, while multiple jobs are returned in a list
        # If a single job is handled, then we must add it to a list to process it
        arcJobsInfo = response.json()["job"]
        if isinstance(arcJobsInfo, dict):
            arcJobsInfo = [arcJobsInfo]

        for arcJob in arcJobsInfo:
            jobReference = self._arcIDToJobReference(arcJob["id"])
            # ARC REST interface returns hyperbole
            arcState = arcJob["state"].capitalize()
            self.log.debug("REST ARC status", f"for job {jobReference} is {arcState}")
            resultDict[jobReference] = self.mapStates[arcState]

            # Cancel held jobs so they don't sit in the queue forever
            if arcState == "Hold":
                jobsToCancel.append(arcJob["id"])
                self.log.debug(f"Killing held job {jobReference}")

        # Renew delegations to renew the proxies of the jobs
        if not self.token or self.alwaysIncludeProxy:
            result = self._getDelegationIDs()
            if not result["OK"]:
                return result
            delegationIDs = result["Value"]
            for delegationID in delegationIDs:
                result = self._renewDelegation(delegationID)
                if not result["OK"]:
                    # Only log here as we still want to return statuses
                    self.log.warn("Failed to renew delegation", f"{delegationID}: {result['Message']}")

        # Kill held jobs
        if jobsToCancel:
            result = self._killJob(jobsToCancel)
            if not result["OK"]:
                # Only log here as we still want to return statuses
                self.log.warn("Failed to kill held jobs:", result["Message"])

        return S_OK(resultDict)

    #############################################################################

    def getJobLog(self, jobID):
        """Get job logging info

        :param str jobID: Job reference followed by the DIRAC stamp.
        :return: string representing the logging info of a given jobID
        """
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot get job logging info", result["Message"])
            return result

        # Prepare the command: Get output files
        arcJob = self._jobReferenceToArcID(jobID.split(":::")[0])
        query = self._urlJoin(os.path.join("jobs", arcJob, "diagnose", "errors"))

        # Submit the GET request to retrieve outputs
        self.log.debug(f"Retrieving logging info for {jobID}")
        result = self._request("get", query)
        if not result["OK"]:
            self.log.error("Failed to retrieve logging info for", f"{jobID}: {result['Message']}")
            return S_ERROR(f"Failed to retrieve logging info for {jobID}")
        response = result["Value"]
        loggingInfo = response.text

        return S_OK(loggingInfo)

    #############################################################################

    def _getListOfAvailableOutputs(self, jobID, arcJobID):
        """Request a list of outputs available for a given jobID.

        :param str jobID: job reference without the DIRAC stamp
        :param str arcJobID: ARC job ID
        :return list: names of the available outputs
        """
        query = self._urlJoin(os.path.join("jobs", arcJobID, "session"))

        # Submit the GET request to retrieve the names of the outputs
        self.log.debug(f"Retrieving the names of the outputs for {jobID}")
        result = self._request("get", query)
        if not result["OK"]:
            self.log.error("Failed to retrieve at least some outputs", f"for {jobID}: {result['Message']}")
            return S_ERROR(f"Failed to retrieve at least some outputs for {jobID}")
        response = result["Value"]

        if not response.text:
            return S_ERROR(f"There is no output for job {jobID}")

        return S_OK(response.json()["file"])

    def getJobOutput(self, jobID, workingDirectory=None):
        """Get the outputs of the given job reference.

        Outputs and stored in workingDirectory if present, else in a new directory named <ARC JobID>.

        :param str jobID: job reference followed by the DIRAC stamp.
        :param str workingDirectory: name of the directory containing the retrieved outputs.
        :return: content of stdout and stderr
        """
        result = self._checkSession()
        if not result["OK"]:
            self.log.error("Cannot get job outputs", result["Message"])
            return result

        # Extract stamp from the Job ID
        if ":::" in jobID:
            jobRef, stamp = jobID.split(":::")
        else:
            return S_ERROR(f"DIRAC stamp not defined for {jobID}")
        arcJob = self._jobReferenceToArcID(jobRef)

        # Get the list of available outputs
        result = self._getListOfAvailableOutputs(jobRef, arcJob)
        if not result["OK"]:
            return result
        remoteOutputs = result["Value"]
        self.log.debug("Outputs to get are", remoteOutputs)

        if not workingDirectory:
            if "WorkingDirectory" in self.ceParameters:
                # We assume that workingDirectory exists
                workingDirectory = os.path.join(self.ceParameters["WorkingDirectory"], arcJob)
            else:
                workingDirectory = arcJob
            os.mkdir(workingDirectory)

        stdout = None
        stderr = None
        for remoteOutput in remoteOutputs:
            # Prepare the command
            query = self._urlJoin(os.path.join("jobs", arcJob, "session", remoteOutput))

            # Submit the GET request to retrieve outputs
            result = self._request("get", query, stream=True)
            if not result["OK"]:
                self.log.error("Error downloading", f"{remoteOutput} for {arcJob}: {result['Message']}")
                return S_ERROR(f"Error downloading {remoteOutput} for {jobID}")
            response = result["Value"]

            localOutput = os.path.join(workingDirectory, remoteOutput)
            with open(localOutput, "wb") as f:
                shutil.copyfileobj(response.raw, f)

            if remoteOutput == f"{stamp}.out":
                with open(localOutput) as f:
                    stdout = f.read()
            if remoteOutput == f"{stamp}.err":
                with open(localOutput) as f:
                    stderr = f.read()

        return S_OK((stdout, stderr))
