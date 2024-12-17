""" Encapsulate here the logic for matching jobs

    Utilities and classes here are used by MatcherHandler
"""
import time

from DIRAC import convertToPy3VersionNumber, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security import Properties
from DIRAC.Core.Utilities.PrettyPrint import printDict
from DIRAC.FrameworkSystem.Client.Logger import setContextLogger
from DIRAC.ResourceStatusSystem.Client.SiteStatus import SiteStatus
from DIRAC.WorkloadManagementSystem.Client import JobStatus, PilotStatus
from DIRAC.WorkloadManagementSystem.Client.Limiter import Limiter
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.WorkloadManagementSystem.DB.JobLoggingDB import JobLoggingDB
from DIRAC.WorkloadManagementSystem.DB.PilotAgentsDB import PilotAgentsDB
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB, multiValueMatchFields, singleValueDefFields


class PilotVersionError(Exception):
    pass


class Matcher:
    """Logic for matching"""

    def __init__(self, pilotAgentsDB=None, jobDB=None, tqDB=None, jlDB=None, opsHelper=None, pilotRef=None):
        """c'tor"""
        if pilotAgentsDB:
            self.pilotAgentsDB = pilotAgentsDB
        else:
            self.pilotAgentsDB = PilotAgentsDB()
        if jobDB:
            self.jobDB = jobDB
        else:
            self.jobDB = JobDB()
        if tqDB:
            self.tqDB = tqDB
        else:
            self.tqDB = TaskQueueDB()
        if jlDB:
            self.jlDB = jlDB
        else:
            self.jlDB = JobLoggingDB()

        if opsHelper:
            self.opsHelper = opsHelper
        else:
            self.opsHelper = Operations()

        if pilotRef:
            self.log = gLogger.getLocalSubLogger(f"[{pilotRef}]Matcher")
        else:
            self.log = gLogger.getSubLogger("Matcher")

        self.limiter = Limiter(jobDB=self.jobDB, opsHelper=self.opsHelper, pilotRef=pilotRef)

        self.siteClient = SiteStatus()

    def selectJob(self, resourceDescription, credDict):
        """Main job selection function to find the highest priority job matching the resource capacity"""
        with setContextLogger(self.log):
            startTime = time.time()

            resourceDict = self._getResourceDict(resourceDescription, credDict)

            # Make a nice print of the resource matching parameters
            toPrintDict = dict(resourceDict)
            if "MaxRAM" in resourceDescription:
                toPrintDict["MaxRAM"] = resourceDescription["MaxRAM"]
            if "NumberOfProcessors" in resourceDescription:
                toPrintDict["NumberOfProcessors"] = resourceDescription["NumberOfProcessors"]
            toPrintDict["Tag"] = []
            if "Tag" in resourceDict:
                for tag in resourceDict["Tag"]:
                    if not tag.endswith("GB") and not tag.endswith("Processors"):
                        toPrintDict["Tag"].append(tag)
            if not toPrintDict["Tag"]:
                toPrintDict.pop("Tag")
            self.log.info("Resource description for matching", printDict(toPrintDict))

            negativeCond = self.limiter.getNegativeCondForSite(resourceDict["Site"], resourceDict.get("GridCE"))
            result = self.tqDB.matchAndGetJob(resourceDict, negativeCond=negativeCond)

            if not result["OK"]:
                raise RuntimeError(result["Message"])
            result = result["Value"]
            if not result["matchFound"]:
                self.log.info("No match found")
                return {}

            jobID = result["jobId"]
            resAtt = self.jobDB.getJobAttributes(jobID, ["Status"])
            if not resAtt["OK"]:
                raise RuntimeError("Could not retrieve job attributes")
            if not resAtt["Value"]:
                raise RuntimeError("No attributes returned for job")
            if not resAtt["Value"]["Status"] == "Waiting":
                self.log.error("Job matched by the TQ is not in Waiting state", str(jobID))
                result = self.tqDB.deleteJob(jobID)
                if not result["OK"]:
                    raise RuntimeError(result["Message"])
                raise RuntimeError(f"Job {str(jobID)} is not in Waiting state")

            self._reportStatus(resourceDict, jobID)

            result = self.jobDB.getJobJDL(jobID)
            if not result["OK"]:
                raise RuntimeError("Failed to get the job JDL")

            resultDict = {}
            resultDict["JDL"] = result["Value"]
            resultDict["JobID"] = jobID

            matchTime = time.time() - startTime
            self.log.verbose("Match time", f"[{str(matchTime)}]")

            # Get some extra stuff into the response returned
            resOpt = self.jobDB.getJobOptParameters(jobID)
            if resOpt["OK"]:
                for key, value in resOpt["Value"].items():
                    resultDict[key] = value
            resAtt = self.jobDB.getJobAttributes(jobID, ["Owner", "OwnerGroup"])
            if not resAtt["OK"]:
                raise RuntimeError("Could not retrieve job attributes")
            if not resAtt["Value"]:
                raise RuntimeError("No attributes returned for job")

            if self.opsHelper.getValue("JobScheduling/CheckMatchingDelay", True):
                self.limiter.updateDelayCounters(resourceDict["Site"], jobID)

            pilotInfoReportedFlag = resourceDict.get("PilotInfoReportedFlag", False)
            if not pilotInfoReportedFlag:
                self._updatePilotInfo(resourceDict)
            self._updatePilotJobMapping(resourceDict, jobID)

            resultDict["Owner"] = resAtt["Value"]["Owner"]
            resultDict["Group"] = resAtt["Value"]["OwnerGroup"]
            resultDict["PilotInfoReportedFlag"] = True

            return resultDict

    def _getResourceDict(self, resourceDescription, credDict):
        """from resourceDescription to resourceDict (just various mods)"""
        resourceDict = self._processResourceDescription(resourceDescription)
        resourceDict = self._checkCredentials(resourceDict, credDict)
        self._checkPilotVersion(resourceDict)
        if not self._checkMask(resourceDict):
            # Banned destinations can only take Test jobs
            resourceDict["JobType"] = "Test"

        self.log.verbose("Resource description")
        for key in resourceDict:
            self.log.debug(f"{key.rjust(20)} : {resourceDict[key]}")

        return resourceDict

    def _processResourceDescription(self, resourceDescription):
        """Check and form the resource description dictionary

        :param resourceDescription: a ceDict coming from a JobAgent,
                                    for example.
        :return: updated dictionary of resource description parameters
        """

        resourceDict = {}
        for name in singleValueDefFields:
            if name in resourceDescription:
                resourceDict[name] = resourceDescription[name]

        for name in multiValueMatchFields:
            if name in resourceDescription:
                resourceDict[name] = resourceDescription[name]

        # resourceDescription["Tag"] can be ''
        if "Tag" in resourceDescription:
            tags = resourceDescription["Tag"]
            resourceDict["Tag"] = (
                tags if isinstance(tags, list) else list({tag.strip("\"' ") for tag in tags.strip("[]").split(",")})
            )
            if "RequiredTag" in resourceDescription:
                requiredTagsList = (
                    list({tag.strip("\"' ") for tag in resourceDescription["RequiredTag"].strip("[]").split(",")})
                    if isinstance(resourceDescription["RequiredTag"], str)
                    else resourceDescription["RequiredTag"]
                )
                resourceDict["RequiredTag"] = requiredTagsList

        if "JobID" in resourceDescription:
            resourceDict["JobID"] = resourceDescription["JobID"]

        # Convert MaxRAM and NumberOfProcessors parameters into a list of tags
        maxRAM = resourceDescription.get("MaxRAM")
        if maxRAM:
            try:
                maxRAM = int(maxRAM / 1000)
            except ValueError:
                maxRAM = None
        nProcessors = resourceDescription.get("NumberOfProcessors")
        if nProcessors:
            try:
                nProcessors = int(nProcessors)
            except ValueError:
                nProcessors = None
        for param, key in [(maxRAM, "GB"), (nProcessors, "Processors")]:
            if param and param <= 1024:
                paramList = list(range(2, param + 1))
                paramTags = ["%d%s" % (par, key) for par in paramList]
                if paramTags:
                    resourceDict.setdefault("Tag", []).extend(paramTags)

        # Add 'MultiProcessor' to the list of tags
        if nProcessors and nProcessors > 1:
            resourceDict.setdefault("Tag", []).append("MultiProcessor")

        # Add 'WholeNode' to the list of tags
        if "WholeNode" in resourceDescription:
            resourceDict.setdefault("Tag", []).append("WholeNode")

        if "Tag" in resourceDict:
            resourceDict["Tag"] = list(set(resourceDict["Tag"]))
        if "RequiredTag" in resourceDict:
            resourceDict["RequiredTag"] = list(set(resourceDict["RequiredTag"]))

        for k in (
            "DIRACVersion",
            "ReleaseVersion",
            "ReleaseProject",
            "VirtualOrganization",
            "PilotReference",
            "PilotBenchmark",
            "PilotInfoReportedFlag",
        ):
            if k in resourceDescription:
                resourceDict[k] = resourceDescription[k]

        return resourceDict

    def _reportStatus(self, resourceDict, jobID):
        """Reports the status of the matched job in jobDB and jobLoggingDB

        Do not fail if errors happen here
        """
        attNames = ["Status", "MinorStatus", "ApplicationStatus", "Site"]
        attValues = ["Matched", "Assigned", "Unknown", resourceDict["Site"]]
        result = self.jobDB.setJobAttributes(jobID, attNames, attValues, update=True)
        if not result["OK"]:
            self.log.error("Problem reporting job status", f"setJobAttributes, jobID = {jobID}: {result['Message']}")
        else:
            self.log.verbose("Set job attributes for jobID", jobID)

        result = self.jlDB.addLoggingRecord(jobID, status=JobStatus.MATCHED, minorStatus="Assigned", source="Matcher")
        if not result["OK"]:
            self.log.error("Problem reporting job status", f"addLoggingRecord, jobID = {jobID}: {result['Message']}")
        else:
            self.log.verbose("Added logging record for jobID", jobID)

    def _checkMask(self, resourceDict):
        """Check the mask: are we allowed to run normal jobs?

        FIXME: should we move to site OR SE?
        """
        if "Site" not in resourceDict:
            self.log.error("Missing Site Name in Resource JDL")
            raise RuntimeError("Missing Site Name in Resource JDL")

        # Check if site is allowed
        result = self.siteClient.getUsableSites(resourceDict["Site"])
        if not result["OK"]:
            self.log.error("Internal error", f"siteClient.getUsableSites: {result['Message']}")
            raise RuntimeError("Internal error")

        if resourceDict["Site"] not in result["Value"]:
            return False

        return True

    def _updatePilotInfo(self, resourceDict):
        """Update pilot information - do not fail if we don't manage to do it"""
        pilotReference = resourceDict.get("PilotReference", "")
        if pilotReference and pilotReference != "Unknown":
            gridCE = resourceDict.get("GridCE", "Unknown")
            site = resourceDict.get("Site", "Unknown")
            benchmark = resourceDict.get("PilotBenchmark", 0.0)
            self.log.verbose(
                "Reporting pilot info",
                f"for {pilotReference}: gridCE={gridCE}, site={site}, benchmark={benchmark:f}",
            )

            result = self.pilotAgentsDB.setPilotStatus(
                pilotReference, status=PilotStatus.RUNNING, gridSite=site, destination=gridCE, benchmark=benchmark
            )
            if not result["OK"]:
                self.log.warn(
                    "Problem updating pilot information",
                    f"; setPilotStatus. pilotReference: {pilotReference}; {result['Message']}",
                )

    def _updatePilotJobMapping(self, resourceDict, jobID):
        """Update pilot to job mapping information"""
        pilotReference = resourceDict.get("PilotReference", "")
        if pilotReference and pilotReference != "Unknown":
            result = self.pilotAgentsDB.setCurrentJobID(pilotReference, jobID)
            if not result["OK"]:
                self.log.error(
                    "Problem updating pilot information",
                    f";setCurrentJobID. pilotReference: {pilotReference}; {result['Message']}",
                )
            result = self.pilotAgentsDB.setJobForPilot(jobID, pilotReference, updateStatus=False)
            if not result["OK"]:
                self.log.error(
                    "Problem updating pilot information",
                    f"; setJobForPilot. pilotReference: {pilotReference}; {result['Message']}",
                )

    def _checkCredentials(self, resourceDict, credDict):
        """Check if we can get a job given the passed credentials"""
        if Properties.GENERIC_PILOT in credDict["properties"]:
            # You can only match groups in the same VO
            if credDict["group"] == "hosts":
                # for the host case the VirtualOrganization parameter
                # is mandatory in resourceDict
                vo = resourceDict.get("VirtualOrganization", "")
            else:
                vo = Registry.getVOForGroup(credDict["group"])
            if "OwnerGroup" not in resourceDict:
                result = Registry.getGroupsForVO(vo)
                if result["OK"]:
                    resourceDict["OwnerGroup"] = result["Value"]
                else:
                    raise RuntimeError(result["Message"])
        else:
            # If it's a job sharing. The group has to be the same and just check that the DN (if any)
            # belongs to the same group
            if Properties.JOB_SHARING in credDict["properties"]:
                resourceDict["OwnerGroup"] = credDict["group"]
                self.log.notice("Setting the resource group to the credentials group")
                if "OwnerDN" in resourceDict and resourceDict["OwnerDN"] != credDict["DN"]:
                    ownerDN = resourceDict["OwnerDN"]
                    result = Registry.getGroupsForDN(resourceDict["OwnerDN"])
                    if not result["OK"]:
                        raise RuntimeError(result["Message"])
                    if credDict["group"] not in result["Value"]:
                        # DN is not in the same group! bad boy.
                        self.log.warn(
                            "You cannot request jobs from this DN, as it does not belong to your group!",
                            f"({ownerDN})",
                        )
                        resourceDict["OwnerDN"] = credDict["DN"]
            # Nothing special, group and DN have to be the same
            else:
                resourceDict["OwnerDN"] = credDict["DN"]
                resourceDict["OwnerGroup"] = credDict["group"]

        return resourceDict

    def _checkPilotVersion(self, resourceDict):
        """Check the pilot DIRAC version"""
        if self.opsHelper.getValue("Pilot/CheckVersion", True):
            if "ReleaseVersion" not in resourceDict:
                if "DIRACVersion" not in resourceDict:
                    raise PilotVersionError("Version check requested and not provided by Pilot")
                else:
                    pilotVersion = resourceDict["DIRACVersion"]
            else:
                pilotVersion = resourceDict["ReleaseVersion"]

            validVersions = [
                convertToPy3VersionNumber(newStyleVersion)
                for newStyleVersion in self.opsHelper.getValue("Pilot/Version", [])
            ]
            if validVersions and convertToPy3VersionNumber(pilotVersion) not in validVersions:
                raise PilotVersionError(
                    "Pilot version does not match the production version: %s not in ( %s )"
                    % (pilotVersion, ",".join(validVersions))
                )
            # Check project if requested
            validProject = self.opsHelper.getValue("Pilot/Project", "")
            if validProject:
                if "ReleaseProject" not in resourceDict:
                    raise PilotVersionError(f"Version check requested but expected project {validProject} not received")
                if resourceDict["ReleaseProject"] != validProject:
                    raise PilotVersionError(
                        "Version check requested but expected project %s != received %s"
                        % (validProject, resourceDict["ReleaseProject"])
                    )
