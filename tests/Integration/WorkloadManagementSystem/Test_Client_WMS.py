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

# pylint: disable=protected-access,wrong-import-position,invalid-name,missing-docstring

import datetime
from diraccfg import CFG
import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.tests.Utilities.WMS import helloWorldJob, parametricJob, createFile

from DIRAC import gLogger, gConfig
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus
from DIRAC.WorkloadManagementSystem.Client.WMSClient import WMSClient
from DIRAC.WorkloadManagementSystem.Client.JobMonitoringClient import JobMonitoringClient
from DIRAC.WorkloadManagementSystem.Client.JobStateUpdateClient import JobStateUpdateClient
from DIRAC.WorkloadManagementSystem.Client.MatcherClient import MatcherClient
from DIRAC.WorkloadManagementSystem.Agent.JobCleaningAgent import JobCleaningAgent
from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


voConfig = """
DIRAC
{
    VirtualOrganization = vo
}
"""


@pytest.fixture(name="wmsClient")
def fixtureWmsClient():

    cfg = CFG()
    cfg.loadFromBuffer(voConfig)
    gConfig.loadCFG(cfg)

    gLogger.setLevel("DEBUG")

    yield WMSClient()

    # use the JobCleaningAgent method to remove the jobs in status Deleted and Killed
    jca = JobCleaningAgent("WorkloadManagement/JobCleaningAgent", "WorkloadManagement/JobCleaningAgent")
    jca.initialize()
    jca.removeDeletedJobs()


def test_FullChain(wmsClient: WMSClient):
    """This test will

    - call all the WMSClient methods
        that will end up calling all the JobManager service methods
    - use the JobMonitoring to verify few properties
    - call the JobCleaningAgent to eliminate job entries from the DBs
    """
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = JobStateUpdateClient()

    # create the job
    job = helloWorldJob()
    jobDescription = createFile(job)

    # submit the job
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    assert res["OK"], res["Message"]
    assert isinstance(res["Value"], int)
    assert res["Value"] == res["JobID"]
    jobID = res["JobID"]
    jobID = res["Value"]

    # updating the status
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.RUNNING, "Executing Minchiapp", "source")
    assert res["OK"], res["Message"]

    # reset the job
    res = wmsClient.resetJob(jobID)
    assert res["OK"], res["Message"]

    # reschedule the job
    res = wmsClient.rescheduleJob(jobID)
    assert res["OK"], res["Message"]

    res = jobMonitor.getJobsStatus(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"][jobID]["Status"] == JobStatus.RECEIVED

    res = jobMonitor.getJobsMinorStatus([jobID])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID: {"MinorStatus": JobMinorStatus.RESCHEDULED}}
    res = jobMonitor.getJobsApplicationStatus([jobID])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID: {"ApplicationStatus": "Unknown"}}

    res = jobMonitor.getJobsStates([jobID])
    assert res["OK"], res["Message"]
    assert res["Value"] == {
        jobID: {
            "Status": JobStatus.RECEIVED,
            "MinorStatus": JobMinorStatus.RESCHEDULED,
            "ApplicationStatus": "Unknown",
        }
    }

    # updating the status again
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.CHECKING, "checking", "source")
    assert res["OK"], res["Message"]
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.WAITING, "waiting", "source")
    assert res["OK"], res["Message"]
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.MATCHED, "matched", "source")
    assert res["OK"], res["Message"]

    # kill the job
    res = wmsClient.killJob(jobID)
    assert res["OK"], res["Message"]
    res = jobMonitor.getJobsStatus(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"][jobID]["Status"] == JobStatus.KILLED

    # delete the job - this will just set its status to "deleted"
    res = wmsClient.deleteJob(jobID)
    assert res["OK"], res["Message"]
    res = jobMonitor.getJobsStatus(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"][jobID]["Status"] == JobStatus.DELETED


def test_ParametricChain(wmsClient: WMSClient):
    """This test will submit a parametric job which should generate 3 actual jobs"""
    jobStateUpdate = JobStateUpdateClient()
    jobMonitor = JobMonitoringClient()

    # create the job
    job = parametricJob()
    jobDescription = createFile(job)

    # submit the job
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    assert res["OK"], res["Message"]
    jobIDList = res["Value"]
    assert len(jobIDList) == 3

    res = jobMonitor.getJobsParameters(jobIDList, ["JobName"])
    assert res["OK"], res["Message"]
    jobNames = [res["Value"][jobID]["JobName"] for jobID in res["Value"]]
    assert set(jobNames) == {f"parametric_helloWorld_{nJob}" for nJob in range(3)}

    for jobID in jobIDList:
        res = jobStateUpdate.setJobStatus(jobID, JobStatus.CHECKING, "checking", "source")
        assert res["OK"], res["Message"]

    res = wmsClient.deleteJob(jobIDList)
    assert res["OK"], res["Message"]
    print(res)

    for jobID in jobIDList:
        res = jobMonitor.getJobsStatus(jobID)
        assert res["OK"], res["Message"]
        assert res["Value"][jobID]["Status"] == JobStatus.DELETED


def test_JobStateUpdateAndJobMonitoring(wmsClient: WMSClient):
    """Verifying all JobStateUpdate and JobMonitoring functions"""
    jobMonitor = JobMonitoringClient()
    jobStateUpdate = JobStateUpdateClient()

    # create a job and check stuff
    job = helloWorldJob()
    jobDescription = createFile(job)

    # submitting the job. Checking few stuff
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    assert res["OK"], res["Message"]
    jobID = int(res["Value"])
    # jobID = res['JobID']
    res = jobMonitor.getJobJDL(jobID, True)
    assert res["OK"], res["Message"]
    res = jobMonitor.getJobJDL(jobID, False)
    assert res["OK"], res["Message"]
    res = jobMonitor.getJobsParameters([jobID], [])
    assert res["OK"], res["Message"]
    res = jobMonitor.getJobOwner(jobID)
    assert res["OK"], res["Message"]

    # Adding stuff

    # forcing the update
    res = jobStateUpdate.setJobStatus(jobID, JobStatus.RUNNING, "running", "source", None, True)
    assert res["OK"], res["Message"]
    res = jobStateUpdate.setJobParameters(jobID, [("par1", "par1Value"), ("par2", "par2Value")])
    assert res["OK"], res["Message"]
    res = jobStateUpdate.setJobApplicationStatus(jobID, "app status", "source")
    assert res["OK"], res["Message"]
    # res = jobStateUpdate.setJobFlag()
    # self.assertTrue(res['OK'], res.get('Message'))
    # res = jobStateUpdate.unsetJobFlag()
    # self.assertTrue(res['OK'], res.get('Message'))
    res = jobStateUpdate.setJobSite(jobID, "Site")
    assert res["OK"], res["Message"]

    # now checking few things
    res = jobMonitor.getJobsStatus(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"][jobID]["Status"] == JobStatus.RUNNING

    res = jobMonitor.getJobParameter(jobID, "par1")
    assert res["OK"], res["Message"]
    assert res["Value"] == {"par1": "par1Value"}

    res = jobMonitor.getJobParameters(jobID, ["par1", "par2"])
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID: {"par1": "par1Value", "par2": "par2Value"}}

    res = jobMonitor.getJobParameters(jobID, "par1")
    assert res["OK"], res["Message"]
    assert res["Value"] == {jobID: {"par1": "par1Value"}}

    res = jobMonitor.getJobAttribute(jobID, "Site")
    assert res["OK"], res["Message"]
    assert res["Value"] == "Site"

    res = jobMonitor.getJobAttributes(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"]["ApplicationStatus"] == "app status"
    assert res["Value"]["JobName"] == "helloWorld"

    res = jobMonitor.getJobSummary(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"]["ApplicationStatus"] == "app status"
    assert res["Value"]["Status"] == JobStatus.RUNNING

    res = jobMonitor.getJobHeartBeatData(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"] == []

    res = jobMonitor.getInputData(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"] == []

    res = jobMonitor.getJobSummary(jobID)
    assert res["OK"], res["Message"]

    res = jobMonitor.getAtticJobParameters(jobID)
    assert res["OK"], res["Message"]

    res = jobStateUpdate.setJobStatus(jobID, JobStatus.DONE, "MinorStatus", "Unknown")
    assert res["OK"], res["Message"]

    res = jobMonitor.getJobSummary(jobID)
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.DONE
    assert res["Value"]["MinorStatus"] == "MinorStatus"
    assert res["Value"]["ApplicationStatus"] == "app status"

    res = jobStateUpdate.sendHeartBeat(jobID, {"bih": "bih"}, {"boh": "boh"})
    assert res["OK"], res["Message"]

    # delete the job - this will just set its status to "deleted"
    wmsClient.deleteJob(jobID)


#     # Adding a platform
#     self.getDIRACPlatformMock.return_value = {'OK': False}
#
#     job = helloWorldJob()
#     job.setPlatform( "x86_64-slc6" )
#
#     jobDescription = createFile( job )
#
#     job.setCPUTime( 17800 )
#     job.setBannedSites( ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
#                          'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'] )
#     res = WMSClient().submitJob( job._toJDL( xmlFile = jobDescription ) )
#     self.assertTrue(res['OK'], res.get('Message'))
#     self.assertEqual( type( res['Value'] ), int )


def test_JobStateUpdateAndJobMonitoringMultuple(wmsClient: WMSClient):
    """Now, let's submit some jobs. Different sites, types, inputs"""

    jobMonitor = JobMonitoringClient()
    jobStateUpdate = JobStateUpdateClient()

    jobIDs = []
    lfnss = [["/vo/1.txt", "/vo/2.txt"], ["/vo/1.txt", "/vo/3.txt", "/vo/4.txt"], []]
    types = ["User", "Test"]

    for lfns in lfnss:
        for jobType in types:
            job = helloWorldJob()
            job.setDestination("DIRAC.Jenkins.ch")
            job.setInputData(lfns)
            job.setType(jobType)
            jobDescription = createFile(job)
            res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
            assert res["OK"], res["Message"]
            jobID = res["Value"]
        jobIDs.append(jobID)

    res = jobMonitor.getSites()
    assert res["OK"], res["Message"]
    assert set(res["Value"]) <= {"ANY", "DIRAC.Jenkins.ch", "Site"}

    res = jobMonitor.getJobTypes()
    assert res["OK"], res["Message"]
    assert sorted(res["Value"]) == sorted(types)

    res = jobMonitor.getApplicationStates()
    assert res["OK"], res["Message"]
    assert res["Value"] == ["app status", "Unknown"]

    res = jobMonitor.getOwners()
    assert res["OK"], res["Message"]
    res = jobMonitor.getOwnerGroup()
    assert res["OK"], res["Message"]
    res = jobMonitor.getJobGroups()
    assert res["OK"], res["Message"]
    resJG_empty = res["Value"]
    res = jobMonitor.getJobGroups(None, datetime.datetime.utcnow())
    assert res["OK"], res["Message"]
    resJG_olderThanNow = res["Value"]
    assert resJG_empty == resJG_olderThanNow
    res = jobMonitor.getJobGroups(None, datetime.datetime.utcnow() - datetime.timedelta(days=365))
    assert res["OK"], res["Message"]
    resJG_olderThanOneYear = res["Value"]
    assert set(resJG_olderThanOneYear).issubset(set(resJG_olderThanNow))
    res = jobMonitor.getStates()
    assert res["OK"], res["Message"]
    assert sorted(res["Value"]) in [[JobStatus.RECEIVED], sorted([JobStatus.RECEIVED, JobStatus.KILLED])]
    res = jobMonitor.getMinorStates()
    assert res["OK"], res["Message"]
    assert sorted(res["Value"]) in [
        ["Job accepted"],
        sorted(["Job accepted", JobMinorStatus.RESCHEDULED]),
        sorted(["Job accepted", "Marked for termination"]),
    ]

    res = jobMonitor.getJobs()
    assert res["OK"], res["Message"]
    assert {str(x) for x in jobIDs} <= set(res["Value"])
    # res = jobMonitor.getCounters(attrList)
    # self.assertTrue(res['OK'], res.get('Message'))
    res = jobMonitor.getJobsSummary(jobIDs)
    assert res["OK"], res["Message"]
    res = jobMonitor.getJobPageSummaryWeb({}, [], 0, 100)
    assert res["OK"], res["Message"]

    res = jobStateUpdate.setJobStatusBulk(
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

    res = jobMonitor.getJobSummary(int(jobID))
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.CHECKING
    assert res["Value"]["MinorStatus"] == "MinorStatus"

    res = jobStateUpdate.setJobStatusBulk(
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
    res = jobMonitor.getJobSummary(int(jobID))
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.MATCHED
    assert res["Value"]["MinorStatus"] == "MinorStatus-matched"

    res = jobStateUpdate.setJobsParameter({jobID: ["Whatever", "booh"]})
    assert res["OK"], res["Message"]

    res = jobMonitor.getJobSummary(int(jobID))
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.MATCHED
    assert res["Value"]["MinorStatus"] == "MinorStatus-matched"

    res = jobStateUpdate.setJobAttribute(jobID, "Status", JobStatus.RUNNING)
    assert res["OK"], res["Message"]

    res = jobMonitor.getJobSummary(int(jobID))
    assert res["OK"], res["Message"]
    assert res["Value"]["Status"] == JobStatus.RUNNING

    # delete the jobs - this will just set its status to "deleted"
    wmsClient.deleteJob(jobIDs)


#   def test_submitFail( self ):
#
#     # Adding a platform that should not exist
#     job = helloWorldJob()
#     job.setPlatform( "notExistingPlatform" )
#     jobDescription = createFile( job )
#
#     res = WMSClient().submitJob( job._toJDL( xmlFile = jobDescription ) )
#     self.assertTrue(res['OK'], res.get('Message'))
#
#     WMSClient().deleteJob( res['Value'] )


def test_matcher(wmsClient: WMSClient):
    # insert a proper DN to run the test
    resourceDescription = {
        "OwnerGroup": "prod",
        "OwnerDN": "/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser",
        "DIRACVersion": "pippo",
        "GridCE": "some.grid.ce.org",
        "ReleaseVersion": "blabla",
        "VirtualOrganization": "LHCb",
        "PilotInfoReportedFlag": "True",
        "PilotBenchmark": "anotherPilot",
        "Site": "DIRAC.Jenkins.ch",
        "CPUTime": 86400,
    }

    job = helloWorldJob()
    job.setDestination("DIRAC.Jenkins.ch")
    job.setInputData("/a/bbb")
    job.setType("User")
    jobDescription = createFile(job)
    res = wmsClient.submitJob(job._toJDL(xmlFile=jobDescription))
    assert res["OK"], res["Message"]

    jobID = res["Value"]

    # forcing the update
    res = JobStateUpdateClient().setJobStatus(jobID, JobStatus.WAITING, "matching", "source", None, True)
    assert res["OK"], res["Message"]

    tqDB = TaskQueueDB()
    tqDefDict = {
        "OwnerDN": "/C=ch/O=DIRAC/OU=DIRAC CI/CN=ciuser",
        "OwnerGroup": "prod",
        "Setup": "dirac-JenkinsSetup",
        "CPUTime": 86400,
    }
    res = tqDB.insertJob(jobID, tqDefDict, 10)
    assert res["OK"], res["Message"]

    res = MatcherClient().requestJob(resourceDescription)
    print(res)
    assert res["OK"], res["Message"]
    wmsClient.deleteJob(jobID)
