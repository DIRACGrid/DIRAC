# pylint: disable=invalid-name, missing-docstring
import pytest

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.WorkloadManagementSystem.DB.BundleDB import BundleDB  # noqa: E402


@pytest.fixture(name="jobInfos")
def fixtureJobInfo():
    return [
        {
            "Executable": "./executable1.sh",
            "Inputs": ["./input1.py", "./input1.json"],
            "Proxy": "FAKE-PROXY",
            "Processors": 2,
            "CEDict": {
                "NumberOfProcessors": 3,
                "ExecTemplate": "bash {inputs}",
                "Site": "DIRAC.Site1.fake",
                "GridCE": "FakeCE",
                "Queue": "FakeQueue",
            },
        },
        {
            "Executable": "./executable2.sh",
            "Inputs": ["./input2.py", "./input2.json"],
            "Proxy": "FAKE-PROXY",
            "Processors": 2,
            "CEDict": {
                "NumberOfProcessors": 3,
                "ExecTemplate": "bash {inputs}",
                "Site": "DIRAC.Site1.fake",
                "GridCE": "FakeCE",
                "Queue": "FakeQueue",
            },
        },
        {
            "Executable": "./executable3.sh",
            "Inputs": ["./input3.py", "./input3.json"],
            "Proxy": "FAKE-PROXY",
            "Processors": 2,
            "CEDict": {
                "NumberOfProcessors": 2,
                "ExecTemplate": "bash {inputs}",
                "Site": "DIRAC.Site2.fake",
                "GridCE": "FakeCE",
                "Queue": "FakeQueue",
            },
        },
        {
            "Executable": "./executable4.sh",
            "Inputs": ["./input4.py", "./input4.json"],
            "Proxy": "FAKE-PROXY",
            "Processors": 1,
            "CEDict": {
                "NumberOfProcessors": 3,
                "ExecTemplate": "bash {inputs}",
                "Site": "DIRAC.Site1.fake",
                "GridCE": "FakeCE",
                "Queue": "FakeQueue",
            },
        },
    ]


@pytest.fixture(name="bundleDB")
def fixtureBundleDB():
    db = BundleDB()
    yield db
    db._query("DELETE FROM JobToBundle")
    db._query("DELETE FROM BundlesInfo")


@pytest.mark.skip(reason="Old tests, need to be remade")
def test_AddToBundle(bundleDB: BundleDB, jobInfos):
    jobId = 0

    #
    # Should return error
    result = bundleDB.getBundleIdFromJobId(jobId)
    assert not result["OK"]

    #
    # Should create a new bundle
    job = jobInfos[0]
    result = bundleDB.insertJobToBundle(jobId, job["Executable"], job["Inputs"], job["Processors"], job["CEDict"])
    assert result["OK"]
    assert result["Value"]
    assert not result["Value"]["Ready"]

    # Save the bundle and job ids for later use
    bundleId1 = result["Value"]["BundleId"]
    jobId1 = jobId

    #
    # Should return the same bundle
    result = bundleDB.getBundleIdFromJobId(jobId)
    assert result["OK"]
    assert result["Value"]
    assert result["Value"] == bundleId1

    jobId += 1

    #
    # Should create a new bundle because it does not fit
    job = jobInfos[1]
    result = bundleDB.insertJobToBundle(jobId, job["Executable"], job["Inputs"], job["Processors"], job["CEDict"])
    assert result["OK"]
    assert result["Value"]
    assert not result["Value"]["Ready"]
    bundleId2 = result["Value"]["BundleId"]
    assert bundleId2 != bundleId1

    jobId += 1

    #
    # Should create a new bundle because a different CE
    job = jobInfos[2]
    result = bundleDB.insertJobToBundle(jobId, job["Executable"], job["Inputs"], job["Processors"], job["CEDict"])
    assert result["OK"]
    assert result["Value"]
    assert result["Value"]["Ready"]
    bundleId3 = result["Value"]["BundleId"]
    assert bundleId3 != bundleId2 and bundleId3 != bundleId1

    jobId += 1

    #
    # Should add it to the very first bundle because it fits
    job = jobInfos[3]
    result = bundleDB.insertJobToBundle(jobId, job["Executable"], job["Inputs"], job["Processors"], job["CEDict"])
    assert result["OK"]
    assert result["Value"]
    assert result["Value"]["Ready"]
    bundleId4 = result["Value"]["BundleId"]
    assert bundleId4 == bundleId1
    jobId4 = jobId

    #
    # Should contain the 2 added jobs
    result = bundleDB.getJobsOfBundle(bundleId4)
    assert result["OK"]
    assert result["Value"]
    jobIds = [job["JobID"] for job in result["Value"]]
    assert jobId1 in jobIds and jobId4 in jobIds
