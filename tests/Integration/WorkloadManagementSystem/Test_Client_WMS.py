""" This is a test of using WMSClient and several other functions in WMS

    In order to run this test we need the following DBs installed:
    - JobDB
    - JobLoggingDB
    - TaskQueueDB
    - SandboxMetadataDB

    And the following services should also be on:
    - OptimizationMind
    - JobManager
    - SandboxStore
    - JobMonitoring
    - JobStateUpdate
    - WMSAdministrator
    - Matcher

    A user proxy is also needed to submit,
    and the Framework/ProxyManager need to be running with a such user proxy already uploaded.

    Due to the nature of the DIRAC WMS, only a full chain test makes sense,
    and this also means that this test is not easy to set up.
"""

# pylint: disable=protected-access,wrong-import-position,missing-docstring

import datetime
import os
import tempfile
import time

import pytest
import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient

from DIRAC.tests.Utilities.WMS import helloWorldJob, parametricJob

gLogger.setLevel("DEBUG")

dataManager = DataManager()
dirac = Dirac()
jobManagerClient = JobManagerClient()
jobMonitoringClient = JobMonitoringClient()
jobStateUpdateClient = JobStateUpdateClient()
matcherClient = MatcherClient()


@pytest.fixture(name="lfn")
def lfnFixture():

    # Get VO
    res = getProxyInfo()
    assert res["OK"]
    proxyInfo = res["Value"]
    vo = getVOForGroup(proxyInfo["group"])
    assert vo

    with tempfile.NamedTemporaryFile("w") as tempFile:
        tempFile.write(str(time.time()))
        tempFile.seek(0)  # write the characters to file

        # Create lfn file
        lfn = f"/{vo}/test/unit-test/WorkloadManager/{time.time()}"
        res = dataManager.putAndRegister(lfn, tempFile.name, "SE-1")
        assert res["OK"], res["Message"]
        assert "Successful" in res["Value"]
        assert lfn in res["Value"]["Successful"]
        assert res["Value"]["Successful"][lfn]

        yield lfn

        # Remove the file from storage element and file catalog
        res = dataManager.removeFile(lfn)
        assert res["OK"], res["Message"]
        assert "Successful" in res["Value"]
        assert lfn in res["Value"]["Successful"]
        assert res["Value"]["Successful"][lfn]

        # Clean the directories
        res = dataManager.cleanLogicalDirectory(os.path.dirname(lfn))
        assert res["OK"], res["Message"]
        res = dataManager.cleanLogicalDirectory(f"/{vo}/test")
        assert res["OK"], res["Message"]


# TODO: add a tape SE to test staging
@pytest.mark.parametrize(
    "jobType, inputData, expectedSite",
    [
        ("User", {}, "ANY"),
        ("User", {"SE-1": 1}, "DIRAC.Jenkins.ch"),
        ("User", {"SE-1": 2}, "DIRAC.Jenkins.ch"),
        # ("User", {"SE-2": 1}, "DIRAC.Jenkins.ch"),
        ("MCSimulation", {}, "ANY"),
        ("MCSimulation", {"SE-1": 1}, "ANY"),
        ("MCSimulation", {"SE-1": 2}, "ANY"),
        ("MCSimulation", {"SE-2": 1}, "ANY"),
        ("MCSimulation", {"SE-1": 1, "SE-2": 1}, "ANY"),
    ],
)
# @pytest.mark.parametrize("inputData", [True, False])
def test_submitJob(jobType, inputData, expectedSite):
    """
    This test will check that a submitted job ends up in the WAITING state
    and that the job is inserted in the taskQueueDB and can be matched
    """

    # Get VO
    res = getProxyInfo()
    assert res["OK"]
    proxyInfo = res["Value"]
    vo = getVOForGroup(proxyInfo["group"])
    assert vo

    # Create lfns
    tempFiles = []
    lfns = []
    for diracSE, numberOfLNFsToCreate in inputData.items():
        for _ in range(numberOfLNFsToCreate):
            tempFile = tempfile.NamedTemporaryFile("w")
            tempFile.write(str(time.time()))
            tempFile.seek(0)  # write the characters to file
            tempFiles.append(tempFile)

            # Create lfn file
            lfn = f"/{vo}/test/unit-test/WorkloadManager/{time.time()}"
            res = dataManager.putAndRegister(lfn, tempFile.name, diracSE)
            assert res["OK"], res["Message"]
            assert "Successful" in res["Value"]
            assert lfn in res["Value"]["Successful"]
            assert res["Value"]["Successful"][lfn]

            lfns.append(lfn)

    # Create the job
    job = helloWorldJob()
    job.setType(jobType)
    if lfns:
        job.setInputData(lfns)

    print(f"Original: {job._toJDL()}")

    # Submitting the job
    result = dirac.submitJob(job)
    assert result["OK"], result["Message"]
    jobID = result["Value"]
    print(f"Submitted job {jobID}")

    try:
        # Wait for the optimizers to run
        startingTime = time.time()
        while time.time() - startingTime < 5 * 60:
            result = jobMonitoringClient.getJobsStates(jobID)
            assert result["OK"], result["Message"]
            if result["Value"][jobID]["Status"] in (JobStatus.WAITING, JobStatus.FAILED):
                break
            time.sleep(1)
        print(f"Lookup time: {time.time() - startingTime}s")

        # Check if the optimizers ran correctly
        print(result["Value"][jobID])
        assert result["Value"][jobID]["Status"] == JobStatus.WAITING
        # FIXME: flaky. This has to do with the CachedJobState.commitChanges()
        # assert result["Value"][jobID]["MinorStatus"] == JobMinorStatus.PILOT_AGENT_SUBMISSION
        # assert result["Value"][jobID]["ApplicationStatus"] == "Unknown"

        res = jobMonitoringClient.getJobJDL(jobID, False)
        assert res["OK"], res["Message"]
        print(f"Job description: {res['Value']}")
        jobDescription = ClassAd(res["Value"])

        # Check that the JDL contains some fields
        assert jobDescription.lookupAttribute("Owner") is True
        assert jobDescription.lookupAttribute("OwnerGroup") is True
        assert jobDescription.lookupAttribute("OwnerDN") is True
        assert jobDescription.lookupAttribute("CPUTime") is True
        assert jobDescription.lookupAttribute("Priority") is True
        assert jobDescription.lookupAttribute("JobID") is True

        res = jobMonitoringClient.getJobSite(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"] == expectedSite

        resourceDescription = {
            "OwnerGroup": jobDescription.getAttributeString("OwnerGroup"),
            "OwnerDN": jobDescription.getAttributeString("OwnerDN"),
            "VirtualOrganization": jobDescription.getAttributeString("VirtualOrganization"),
            "CPUTime": jobDescription.getAttributeInt("CPUTime"),
            "DIRACVersion": "pippo",
            "GridCE": "some.grid.ce.org",
            "ReleaseVersion": "blabla",
            "PilotInfoReportedFlag": "True",
            "PilotBenchmark": "anotherPilot",
            "Site": "DIRAC.Jenkins.ch",
        }

        # Request job until ours is picked up or request returns S_ERROR
        while True:
            time.sleep(1)
            res = matcherClient.requestJob(resourceDescription)
            print(f"Matcher result: {res}")
            if not res["OK"] or res["Value"]["JobID"] == jobID:
                break

        # Assert that our job has been selected by the matcher
        assert res["OK"], res["Message"]
        assert res["Value"]["JobID"] == jobID

        # Check that the job has been putten in the MATCHED status
        result = jobMonitoringClient.getJobsStates(jobID)
        assert result["OK"], result["Message"]
        assert result["Value"][jobID]["Status"] == JobStatus.MATCHED
        assert result["Value"][jobID]["MinorStatus"] == "Assigned"
        assert result["Value"][jobID]["ApplicationStatus"] == "Unknown"
    finally:
        # Remove the file from storage element and file catalog
        for lfn in lfns:
            res = dataManager.removeFile(lfn)
            res = dataManager.cleanLogicalDirectory(os.path.dirname(lfn))
            res = dataManager.cleanLogicalDirectory(f"/{vo}/test")

        # Close all the temporary files
        for tempFile in tempFiles:
            tempFile.close()

        # Remove the job from the DB
        jobManagerClient.removeJob(jobID)


def test_submitJob_parametricJob():
    """This test will submit a parametric job which should generate 3 actual jobs"""

    # create the job
    job = parametricJob()

    res = dirac.submitJob(job)
    assert res["OK"], res["Message"]
    jobIDList = res["Value"]

    try:
        assert len(jobIDList) == 3

        res = jobMonitoringClient.getJobsParameters(jobIDList, ["JobName"])
        assert res["OK"], res["Message"]
        jobNames = [res["Value"][jobID]["JobName"] for jobID in res["Value"]]
        assert set(jobNames) == {f"parametric_helloWorld_{nJob}" for nJob in range(3)}

        # Wait for the optimizers to run
        startingTime = time.time()
        while time.time() - startingTime < 5 * 60:
            result = jobMonitoringClient.getJobsStates(jobIDList)
            assert result["OK"], result["Message"]
            jobsAreNoLongerInChecking = True
            for jobID in jobIDList:
                if result["Value"][jobID]["Status"] in JobStatus.CHECKING:
                    jobsAreNoLongerInChecking = False
            if jobsAreNoLongerInChecking:
                break

            time.sleep(1)
        print(f"Lookup time: {time.time() - startingTime}s")

        for jobID in jobIDList:
            assert result["Value"][jobID]["Status"] == JobStatus.WAITING
            # FIXME: flaky. This has to do with the CachedJobState.commitChanges()
            # assert result["Value"][jobID]["MinorStatus"] == JobMinorStatus.PILOT_AGENT_SUBMISSION
            # assert result["Value"][jobID]["ApplicationStatus"] == "Unknown"
    finally:
        jobManagerClient.removeJob(jobIDList)


def test_WMSClient_rescheduleJob():

    # create the job
    job = helloWorldJob()
    job.setType("User")

    result = dirac.submitJob(job)
    assert result["OK"], result["Message"]
    jobID = result["Value"]

    try:
        # Wait for the optimizers to run
        startingTime = time.time()
        while time.time() - startingTime < 5 * 60:
            result = jobMonitoringClient.getJobsStates(jobID)
            assert result["OK"], result["Message"]
            if result["Value"][jobID]["Status"] in (JobStatus.WAITING, JobStatus.FAILED):
                break
            time.sleep(1)
        print(f"Lookup time: {time.time() - startingTime}s")

        # Check if the optimizers ran correctly
        assert result["Value"][jobID]["Status"] == JobStatus.WAITING
        # FIXME: flaky. This has to do with the CachedJobState.commitChanges()
        # assert result["Value"][jobID]["MinorStatus"] == JobMinorStatus.PILOT_AGENT_SUBMISSION
        # assert result["Value"][jobID]["ApplicationStatus"] == "Unknown"

        res = jobMonitoringClient.getJobJDL(jobID, False)
        assert res["OK"], res["Message"]
        print(f"Job description: {res['Value']}")
        jobDescription = ClassAd(res["Value"])

        # Check that the JDL contains some fields
        assert jobDescription.lookupAttribute("Owner") is True
        assert jobDescription.lookupAttribute("OwnerGroup") is True
        assert jobDescription.lookupAttribute("OwnerDN") is True
        assert jobDescription.lookupAttribute("CPUTime") is True
        assert jobDescription.lookupAttribute("Priority") is True
        assert jobDescription.lookupAttribute("JobID") is True

        # Check that the owner
        res = jobMonitoringClient.getJobOwner(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"] == jobDescription.getAttributeString("Owner")

        resourceDescription = {
            "OwnerGroup": jobDescription.getAttributeString("OwnerGroup"),
            "OwnerDN": jobDescription.getAttributeString("OwnerDN"),
            "VirtualOrganization": jobDescription.getAttributeString("VirtualOrganization"),
            "CPUTime": jobDescription.getAttributeInt("CPUTime"),
            "DIRACVersion": "pippo",
            "GridCE": "some.grid.ce.org",
            "ReleaseVersion": "blabla",
            "PilotInfoReportedFlag": "True",
            "PilotBenchmark": "anotherPilot",
            "Site": "DIRAC.Jenkins.ch",
        }

        # Request job until ours is picked up or resquest job return S_ERROR
        while True:
            res = matcherClient.requestJob(resourceDescription)
            print(f"Matcher result: {res}")
            if not res["OK"] or res["Value"]["JobID"] == jobID:
                break

        # Assert that our job has been selected by the matcher
        assert res["OK"], res["Message"]
        assert res["Value"]["JobID"] == jobID

        # Check that the job has been putten in the MATCHED status
        result = jobMonitoringClient.getJobsStates(jobID)
        assert result["OK"], result["Message"]
        assert result["Value"][jobID]["Status"] == JobStatus.MATCHED
        assert result["Value"][jobID]["MinorStatus"] == "Assigned"
        assert result["Value"][jobID]["ApplicationStatus"] == "Unknown"

        res = jobManagerClient.rescheduleJob(jobID)
        assert res["OK"], res["Messsage"]

        startingTime = time.time()
        while time.time() - startingTime < 5 * 60:
            result = jobMonitoringClient.getJobsStates(jobID)
            assert result["OK"], result["Message"]
            if result["Value"][jobID]["Status"] in (JobStatus.CHECKING, JobStatus.FAILED):
                break
            time.sleep(1)
        print(f"Lookup time: {time.time() - startingTime}s")

        # Check if the optimizers ran correctly after rescheduling
        assert result["Value"][jobID]["Status"] == JobStatus.CHECKING

        res = jobMonitoringClient.getJobJDL(jobID, False)
        assert res["OK"], res["Message"]
        print(f"Job description: {res['Value']}")
        jobDescription = ClassAd(res["Value"])

        assert jobDescription.lookupAttribute("Owner") is True
        assert jobDescription.lookupAttribute("OwnerGroup") is True
        assert jobDescription.lookupAttribute("OwnerDN") is True
        assert jobDescription.lookupAttribute("JobID") is True

    finally:
        jobManagerClient.removeJob(jobID)


def test_JobStateUpdateAndJobMonitoring():
    """Verifying all JobStateUpdate and JobMonitoring functions"""

    # Create a job
    job = helloWorldJob()

    # Submitting the job. Checking few stuff
    res = dirac.submitJob(job)
    assert res["OK"], res["Message"]
    jobID = int(res["Value"])

    try:
        res = jobMonitoringClient.getJobJDL(jobID, True)
        assert res["OK"], res["Message"]
        res = jobMonitoringClient.getJobJDL(jobID, False)
        assert res["OK"], res["Message"]
        res = jobMonitoringClient.getJobsParameters([jobID], [])
        assert res["OK"], res["Message"]
        res = jobMonitoringClient.getJobOwner(jobID)
        assert res["OK"], res["Message"]

        # Adding stuff

        # forcing the update
        res = jobStateUpdateClient.setJobStatus(jobID, JobStatus.RUNNING, "running", "source", None, True)
        assert res["OK"], res["Message"]
        res = jobStateUpdateClient.setJobParameters(jobID, [("par1", "par1Value"), ("par2", "par2Value")])
        assert res["OK"], res["Message"]
        res = jobStateUpdateClient.setJobApplicationStatus(jobID, "app status", "source")
        assert res["OK"], res["Message"]
        # res = jobStateUpdate.setJobFlag()
        # self.assertTrue(res['OK'], res.get('Message'))
        # res = jobStateUpdate.unsetJobFlag()
        # self.assertTrue(res['OK'], res.get('Message'))
        res = jobStateUpdateClient.setJobSite(jobID, "Site")
        assert res["OK"], res["Message"]

        # now checking few things
        res = jobMonitoringClient.getJobsStatus(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"][jobID]["Status"] == JobStatus.RUNNING

        res = jobMonitoringClient.getJobParameters(jobID, ["par1", "par2"])
        assert res["OK"], res["Message"]
        assert res["Value"] == {jobID: {"par1": "par1Value", "par2": "par2Value"}}

        res = jobMonitoringClient.getJobParameter(jobID, "par1")
        assert res["OK"], res["Message"]
        assert res["Value"] == {"par1": "par1Value"}

        res = jobMonitoringClient.getJobParameters(jobID)
        assert res["OK"], res["Message"]
        assert jobID in res["Value"]
        assert "par1" in res["Value"][jobID]
        assert "par2" in res["Value"][jobID]
        assert res["Value"][jobID]["par1"] == "par1Value"
        assert res["Value"][jobID]["par2"] == "par2Value"

        res = jobMonitoringClient.getJobParameters(jobID, "par1")
        assert res["OK"], res["Message"]
        assert res["Value"] == {jobID: {"par1": "par1Value"}}

        res = jobMonitoringClient.getJobAttribute(jobID, "Site")
        assert res["OK"], res["Message"]
        assert res["Value"] == "Site"

        res = jobMonitoringClient.getJobAttributes(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"]["ApplicationStatus"] == "app status"
        assert res["Value"]["JobName"] == "helloWorld"

        res = jobMonitoringClient.getJobSummary(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"]["ApplicationStatus"] == "app status"
        assert res["Value"]["Status"] == JobStatus.RUNNING

        res = jobMonitoringClient.getJobHeartBeatData(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"] == []

        res = jobMonitoringClient.getInputData(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"] == []

        res = jobMonitoringClient.getJobSummary(jobID)
        assert res["OK"], res["Message"]

        res = jobMonitoringClient.getAtticJobParameters(jobID)
        assert res["OK"], res["Message"]

        res = jobStateUpdateClient.setJobStatus(jobID, JobStatus.DONE, "MinorStatus", "Unknown")
        assert res["OK"], res["Message"]

        res = jobMonitoringClient.getJobSummary(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"]["Status"] == JobStatus.DONE
        assert res["Value"]["MinorStatus"] == "MinorStatus"
        assert res["Value"]["ApplicationStatus"] == "app status"

        res = jobStateUpdateClient.sendHeartBeat(jobID, {"bih": "bih"}, {"boh": "boh"})
        assert res["OK"], res["Message"]
    finally:
        jobManagerClient.removeJob(jobID)


def test_JobStateUpdateAndJobMonitoringMultiple(lfn):
    """Now, let's submit some jobs. Different sites, types, inputs"""

    jobIDs = []
    lfnss = [[], [lfn]]
    types = ["User", "MCSimulation"]

    for lfns in lfnss:
        for jobType in types:
            job = helloWorldJob()
            job.setDestination("DIRAC.Jenkins.ch")
            job.setType(jobType)
            if lfns:
                job.setInputData(lfns)
            res = dirac.submitJob(job)
            assert res["OK"], res["Message"]
            jobID = res["Value"]
            jobIDs.append(jobID)

    try:
        res = jobMonitoringClient.getSites()
        assert res["OK"], res["Message"]
        assert set(res["Value"]) <= {"ANY", "DIRAC.Jenkins.ch", "Site"}

        res = jobMonitoringClient.getJobTypes()
        assert res["OK"], res["Message"]
        assert sorted(res["Value"]) == sorted(types)

        res = jobMonitoringClient.getApplicationStates()
        assert res["OK"], res["Message"]
        assert res["Value"] == ["Unknown"]

        res = jobMonitoringClient.getOwners()
        assert res["OK"], res["Message"]
        res = jobMonitoringClient.getOwnerGroup()
        assert res["OK"], res["Message"]
        res = jobMonitoringClient.getJobGroups()
        assert res["OK"], res["Message"]
        resJG_empty = res["Value"]
        res = jobMonitoringClient.getJobGroups(None, datetime.datetime.utcnow())
        assert res["OK"], res["Message"]
        resJG_olderThanNow = res["Value"]
        assert resJG_empty == resJG_olderThanNow
        res = jobMonitoringClient.getJobGroups(None, datetime.datetime.utcnow() - datetime.timedelta(days=365))
        assert res["OK"], res["Message"]
        resJG_olderThanOneYear = res["Value"]
        assert set(resJG_olderThanOneYear).issubset(set(resJG_olderThanNow))

        res = jobMonitoringClient.getStates()
        assert res["OK"], res["Message"]
        assert set(res["Value"]) <= {
            JobStatus.RECEIVED,
            JobStatus.CHECKING,
            JobStatus.WAITING,
            JobStatus.MATCHED,
            JobStatus.KILLED,
        }
        res = jobMonitoringClient.getMinorStates()
        assert res["OK"], res["Message"]

        res = jobMonitoringClient.getJobs()
        assert res["OK"], res["Message"]
        assert {str(x) for x in jobIDs} <= set(res["Value"])
        # res = jobMonitor.getCounters(attrList)
        # self.assertTrue(res['OK'], res.get('Message'))
        res = jobMonitoringClient.getJobsSummary(jobIDs)
        assert res["OK"], res["Message"]
        res = jobMonitoringClient.getJobPageSummaryWeb({}, [], 0, 100)
        assert res["OK"], res["Message"]

        res = jobStateUpdateClient.setJobStatusBulk(
            jobID,
            {
                str(datetime.datetime.utcnow()): {
                    "Status": JobStatus.CHECKING,
                    "MinorStatus": "MinorStatus",
                    "Source": "Unknown",
                }
            },
            False,
        )
        assert res["OK"], res["Message"]

        res = jobMonitoringClient.getJobSummary(int(jobID))
        assert res["OK"], res["Message"]
        assert res["Value"]["Status"] in (JobStatus.CHECKING, JobStatus.WAITING)
        assert res["Value"]["MinorStatus"] == "MinorStatus"

        res = jobStateUpdateClient.setJobStatusBulk(
            jobID,
            {
                str(datetime.datetime.utcnow() + datetime.timedelta(hours=1)): {
                    "Status": JobStatus.WAITING,
                    "MinorStatus": "MinorStatus",
                    "Source": "Unknown",
                },
                str(datetime.datetime.utcnow() + datetime.timedelta(hours=2)): {
                    "Status": JobStatus.MATCHED,
                    "MinorStatus": "MinorStatus-matched",
                    "Source": "Unknown",
                },
            },
            False,
        )
        assert res["OK"], res["Message"]
        res = jobMonitoringClient.getJobSummary(int(jobID))
        assert res["OK"], res["Message"]
        assert res["Value"]["Status"] == JobStatus.MATCHED
        assert res["Value"]["MinorStatus"] == "MinorStatus-matched"

        res = jobStateUpdateClient.setJobsParameter({jobID: ["Whatever", "booh"]})
        assert res["OK"], res["Message"]

        res = jobMonitoringClient.getJobSummary(int(jobID))
        assert res["OK"], res["Message"]
        assert res["Value"]["Status"] == JobStatus.MATCHED
        assert res["Value"]["MinorStatus"] == "MinorStatus-matched"

        res = jobStateUpdateClient.setJobAttribute(jobID, "Status", JobStatus.RUNNING)
        assert res["OK"], res["Message"]

        res = jobMonitoringClient.getJobSummary(int(jobID))
        assert res["OK"], res["Message"]
        assert res["Value"]["Status"] == JobStatus.RUNNING
    finally:
        jobManagerClient.removeJob(jobIDs)


def test_JobManagerClient_removeJob():

    # Arrange
    job = helloWorldJob()
    job.setType("User")

    result = dirac.submitJob(job)
    assert result["OK"], result["Message"]
    jobID = result["Value"]

    # Wait for the optimizers to run
    startingTime = time.time()
    while time.time() - startingTime < 5 * 60:
        result = jobMonitoringClient.getJobsStates(jobID)
        assert result["OK"], result["Message"]
        if result["Value"][jobID]["Status"] in (JobStatus.WAITING, JobStatus.FAILED):
            break
        time.sleep(1)

    # Act
    res = jobManagerClient.removeJob(jobID)

    # Assert
    assert res["OK"], res["Message"]

    res = jobMonitoringClient.getJobsStatus([jobID])
    assert res["OK"], res["Message"]
    assert res["Value"] == {}
