""" This integration test only need the TaskQueueDB
    (which should of course be properly defined also in the configuration),
    and connects directly to it


    Run this test with::
        "python -m pytest tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py"


    Suggestion: for local testing, run this with::
        python -m pytest -c ../pytest.ini  -vv tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB.py
"""
from DIRAC import gLogger

import DIRAC

DIRAC.initialize()  # Initialize configuration

from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB


gLogger.setLevel("DEBUG")
tqDB = TaskQueueDB()


def test_basiChain():
    """a basic put - remove"""
    tqDefDict = {"Owner": "userName", "OwnerGroup": "myGroup", "CPUTime": 50000}
    result = tqDB.insertJob(123, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(123)
    assert result["OK"]
    tq = result["Value"]
    result = tqDB.deleteJob(123)
    assert result["OK"]
    result = tqDB.cleanOrphanedTaskQueues()
    assert result["OK"]
    result = tqDB.deleteTaskQueueIfEmpty(tq)
    assert result["OK"]


def test_chainWithParameter():
    """put - remove with parameters"""
    tqDefDict = {"Owner": "userName", "OwnerGroup": "myGroup", "CPUTime": 50000}

    # first job
    result = tqDB.insertJob(123, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(123)
    assert result["OK"]
    tq = result["Value"]

    result = tqDB.cleanOrphanedTaskQueues()
    assert result["OK"]
    result = tqDB.deleteTaskQueueIfEmpty(tq)  # this won't delete anything
    assert result["OK"]

    # second job
    result = tqDB.insertJob(125, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(125)
    tq = result["Value"]
    result = tqDB.deleteTaskQueueIfEmpty(tq)  # this won't delete anything, as both 123 and 125 are in
    assert result["OK"]  # but still it won't fail
    assert result["Value"] is False
    result = tqDB.retrieveTaskQueues()
    assert result["OK"]

    # now we will try to delete
    result = tqDB.deleteJob(123)
    assert result["OK"]
    result = tqDB.deleteJob(125)
    assert result["OK"]
    result = tqDB.deleteTaskQueueIfEmpty(tq)  # this should now delete tq
    assert result["OK"]
    result = tqDB.retrieveTaskQueues()
    assert result["OK"]


def test_chainWithSites():
    """put - remove with parameters including sites"""
    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Sites": ["LCG.CERN.ch"],
    }
    result = tqDB.insertJob(201, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(201)
    tq_job1 = result["Value"]

    result = tqDB.insertJob(2011, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(2011)
    tq_job11 = result["Value"]

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Sites": ["CLOUD.IN2P3.fr"],
    }
    result = tqDB.insertJob(203, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(203)
    tq_job2 = result["Value"]

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Sites": ["LCG.CERN.ch", "CLOUD.IN2P3.fr"],
    }
    result = tqDB.insertJob(203, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(203)
    tq_job3 = result["Value"]

    # matching
    # this should match everything
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000}, numQueuesToGet=5)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job11}

    # this should match those for CERN
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Site": "LCG.CERN.ch"}, numQueuesToGet=4)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job3, tq_job11}

    # this should match those for IN2P3
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Site": "CLOUD.IN2P3.fr"}, numQueuesToGet=4)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2, tq_job3}

    # this should match those for IN2P3
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Site": "CLOUD.IN2P3.fr"}, numQueuesToGet=4)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2, tq_job3}

    # now we will try to delete
    for jobID in [201, 2011, 202, 203]:
        result = tqDB.deleteJob(jobID)
        assert result["OK"]
    for tqID in [tq_job1, tq_job2, tq_job3]:
        result = tqDB.deleteTaskQueueIfEmpty(tqID)
        assert result["OK"]


def test_chainWithBannedSites():
    """put - remove with parameters including Banned sites"""
    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "BannedSites": ["LCG.CERN.ch", "CLOUD.IN2P3.fr"],
    }
    result = tqDB.insertJob(127, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(127)
    tq_job1 = result["Value"]

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "BannedSites": ["CLOUD.IN2P3.fr", "DIRAC.Test.org"],
    }
    result = tqDB.insertJob(128, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(128)
    tq_job2 = result["Value"]

    # matching
    # this should match everything
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000}, numQueuesToGet=4)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2}

    # this should match also everything
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "centos7"}, numQueuesToGet=4)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2}

    # this should match the first
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Site": "DIRAC.Test.org"}, numQueuesToGet=4)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1}

    # this should match the second
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Site": "LCG.CERN.ch"}, numQueuesToGet=4)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2}

    # this should not match anything because of the banned site CLOUD.IN2P3.fr
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Site": "CLOUD.IN2P3.fr"}, numQueuesToGet=4)
    assert result["OK"]
    assert result["Value"] == []

    result = tqDB.deleteTaskQueueIfEmpty(tq_job1)  # this won't delete anything, as 127 is in
    assert result["OK"]  # but still it won't fail
    assert result["Value"] is False
    result = tqDB.deleteJob(127)
    assert result["OK"]
    result = tqDB.deleteTaskQueueIfEmpty(tq_job1)  # this should now delete tq
    assert result["OK"]

    result = tqDB.deleteJob(128)
    assert result["OK"]

    for tqId in [tq_job1, tq_job2]:
        result = tqDB.deleteTaskQueueIfEmpty(tqId)
        assert result["OK"]

    result = tqDB.retrieveTaskQueues()
    assert result["OK"]
    assert result["Value"] == {}


def test_chainWithPlatforms():
    """put - remove with parameters including a platform"""

    # We'll try the following case
    #
    # possible platforms: slc5, slc6, centos7, debian, ubuntu
    # where:
    #  - centos7 > slc6 > slc5
    #  - ubuntu > debian
    # and of course what runs on rhel family does not run on debian family

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Platforms": ["centos7"],
    }
    result = tqDB.insertJob(1, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(1)
    tq_job1 = result["Value"]
    assert tq_job1 > 0

    result = tqDB.insertJob(2, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(2)
    tq_job2 = result["Value"]
    assert tq_job1 == tq_job2

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Platforms": ["ubuntu"],
    }
    result = tqDB.insertJob(3, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(3)
    tq_job3 = result["Value"]
    assert tq_job3 == tq_job1 + 1

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Platforms": ["centos7", "slc6"],
    }
    result = tqDB.insertJob(4, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(4)
    tq_job4 = result["Value"]
    assert tq_job4 == tq_job3 + 1

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Platforms": ["debian", "ubuntu"],
    }
    result = tqDB.insertJob(5, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(5)
    tq_job5 = result["Value"]
    assert tq_job5 == tq_job4 + 1

    # We should be in this situation (TQIds are obviously invented):
    #
    # select TQId, JobId FROM `tq_Jobs`
    # +--------+---------+
    # |   TQId |   JobId |
    # +--------+---------|
    # |    101 |       1 |
    # |    101 |       2 |
    # |    102 |       3 |
    # |    103 |       4 |
    # |    104 |       5 |
    # +--------+---------+
    #
    # select * FROM `tq_TQToPlatforms`
    # +--------+---------+
    # |   TQId | Value   |
    # |--------+---------|
    # |    101 | centos7 |
    # |    102 | ubuntu  |
    # |    103 | centos7 |
    # |    103 | slc6    |
    # |    104 | debian  |
    # |    104 | ubuntu  |
    # +--------+---------+

    # strict matching

    # centos7
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "centos7"}, numQueuesToGet=4)
    assert result["OK"]
    # this should match one in [tq_job1, tq_job2, tq_job4]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job4}

    # ubuntu
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "ubuntu"}, numQueuesToGet=4)
    assert result["OK"]
    # this should match one in [tq_job3, tq_job5]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job3, tq_job5}

    # slc6
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "slc6"}, numQueuesToGet=4)
    assert result["OK"]
    # this should match only tq_job4, as this is the only one that can run on slc6
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4}

    # slc5
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "slc5"}, numQueuesToGet=4)
    assert result["OK"]
    # this should not match anything
    assert result["Value"] == []

    # compatibility matching

    # ANY platform
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "ANY"}, numQueuesToGet=5)
    assert result["OK"]
    # this should match whatever
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5}

    # Now we insert a TQ without platform

    tqDefDict = {"Owner": "userName", "OwnerGroup": "myGroup", "CPUTime": 5000}
    result = tqDB.insertJob(6, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(6)
    tq_job6 = result["Value"]
    assert tq_job6 == tq_job5 + 1

    # matching for this one

    # ANY platform
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "ANY"}, numQueuesToGet=6)
    assert result["OK"]
    # this should match whatever
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6}

    # ANY platform within a list
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": ["ANY"]}, numQueuesToGet=6)
    assert result["OK"]
    # this should match whatever
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6}

    # no platform at all
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000}, numQueuesToGet=6)
    assert result["OK"]
    # this should match whatever
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6}

    # slc5 -- this time it should match 1 (the one without specified platform)
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "slc5"}, numQueuesToGet=6)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job6}

    # slc6
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "slc6"}, numQueuesToGet=6)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4, tq_job6}

    # slc5, slc6
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": ["slc5", "slc6"]}, numQueuesToGet=6)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4, tq_job6}

    # slc5, slc6, ubuntu
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": ["slc5", "slc6", "ubuntu"]}, numQueuesToGet=6)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job3, tq_job4, tq_job5, tq_job6}

    # Now we insert a TQ with platform "ANY" (same as no platform)

    tqDefDict = {"Owner": "userName", "OwnerGroup": "myGroup", "CPUTime": 5000, "Platform": "ANY"}
    result = tqDB.insertJob(7, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(7)
    tq_job7 = result["Value"]
    assert tq_job7 == tq_job6  # would be inserted in the same TQ

    # matching for this one

    # ANY platform
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "ANY"}, numQueuesToGet=7)
    assert result["OK"]
    # this should match whatever
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6, tq_job7}

    # NO platform
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000}, numQueuesToGet=7)
    assert result["OK"]
    # this should match whatever
    assert int(result["Value"][0][0]) in [tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6, tq_job7]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6, tq_job7}

    # slc5 -- this time it should match 2
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "slc5"}, numQueuesToGet=7)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job6, tq_job7}

    # slc6
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "slc6"}, numQueuesToGet=7)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4, tq_job6, tq_job7}

    # new platform appears
    # centos8 (> centos7)
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "centos8"}, numQueuesToGet=7)
    assert result["OK"]
    # here, I would like to see 3 TQs matched: those for slc6 + centos7 + ANY
    assert len(result["Value"]) == 1
    # but here it returns only 1 (those for ANY), by construction
    # so, this should be in theory improved

    for jobId in range(1, 8):
        result = tqDB.deleteJob(jobId)
        assert result["OK"]

    for tqId in [tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6, tq_job7]:
        result = tqDB.deleteTaskQueueIfEmpty(tqId)
        assert result["OK"]


def test_chainWithTags():
    """put - remove with parameters including one or more Tag(s) and/or RequiredTag(s)"""

    # We'll try the following case
    #
    # Tags: MultiProcessor, SingleProcessor, GPU
    #
    # We'll insert 5 jobs:
    #   1 : MultiProcessor
    #   2 : SingleProcessor
    #   3 : SingleProcessor, MultiProcessor
    #   4 : MultiProcessor, GPU
    #   5 : -- no tags
    #   6 : MultiProcessor, 17Processors

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Tags": ["MultiProcessor"],
    }
    result = tqDB.insertJob(1, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(1)
    tq_job1 = result["Value"]
    assert tq_job1 > 0

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Tags": ["SingleProcessor"],
    }
    result = tqDB.insertJob(2, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(2)
    tq_job2 = result["Value"]
    assert tq_job2 > tq_job1

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Tags": ["SingleProcessor", "MultiProcessor"],
    }
    result = tqDB.insertJob(3, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(3)
    tq_job3 = result["Value"]
    assert tq_job3 > tq_job2

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Tags": ["MultiProcessor", "GPU"],
    }
    result = tqDB.insertJob(4, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(4)
    tq_job4 = result["Value"]
    assert tq_job4 > tq_job3

    tqDefDict = {"Owner": "userName", "OwnerGroup": "myGroup", "CPUTime": 5000}
    result = tqDB.insertJob(5, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(5)
    tq_job5 = result["Value"]
    assert tq_job5 > tq_job4

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Tags": ["MultiProcessor", "17Processors"],
    }
    result = tqDB.insertJob(6, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(6)
    tq_job6 = result["Value"]
    assert tq_job6 > tq_job5

    # We should be in this situation (TQIds are obviously invented):
    #
    # mysql Dirac@localhost:TaskQueueDB> select `TQId`,`JobId` FROM `tq_Jobs`
    # +--------+---------+
    # |   TQId |   JobId |
    # |--------+---------|
    # |    101 |       1 |
    # |    102 |       2 |
    # |    103 |       3 |
    # |    104 |       4 |
    # |    105 |       5 |
    # |    106 |       6 |
    # +--------+---------+
    #
    # mysql Dirac@localhost:TaskQueueDB> select * FROM `tq_TQToTags`
    # +--------+-----------------+
    # |   TQId | Value           |
    # |--------+-----------------|
    # |    101 | MultiProcessor  |
    # |    102 | SingleProcessor |
    # |    103 | MultiProcessor  |
    # |    103 | SingleProcessor |
    # |    104 | GPU             |
    # |    104 | MultiProcessor  |
    # |    106 | MultiProcessor  |
    # |    106 | 17Processors    |
    # +--------+-----------------+

    # Matching

    # Matching Everything with Tag = "ANY"
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": "ANY"}, numQueuesToGet=6)
    assert result["OK"]
    # this should match whatever
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6}

    # Matching Everything with Tag = "aNy"
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": "aNy"}, numQueuesToGet=6)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6}

    # Matching Everything with Tag contains "aNy"
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": ["MultiProcessor", "aNy"]}, numQueuesToGet=6)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6}

    # Matching only tq_job5 when no tag is specified
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000}, numQueuesToGet=5)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job5}

    # Matching only tq_job5 when Tag = ""
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": ""}, numQueuesToGet=5)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job5}

    # Matching only tq_job5 when Tag = []
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": []}, numQueuesToGet=5)
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job5}

    # Matching MultiProcessor

    # Tag: 'MultiProcessor'
    # By doing this, we are basically saying that this CE is accepting ALSO MultiProcessor payloads
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": "MultiProcessor"}, numQueuesToGet=4)
    assert result["OK"]
    # this matches the tq_job1, as it is the only one that requires ONLY MultiProcessor,
    # AND the tq_job5, for which we have inserted no tags
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job5}

    # Tags: ['MultiProcessor', 'GPU']
    # By doing this, we are basically saying that this CE is accepting ALSO payloads that require MultiProcessor or GPU
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": ["MultiProcessor", "GPU"]}, numQueuesToGet=4)
    assert result["OK"]
    # this matches the tq_job1, as it requires ONLY MultiProcessor
    # the tq_job4, as it is the only one that requires BOTH MultiProcessor and GPU,
    # AND the tq_job5, for which we have inserted no tags
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job4, tq_job5, tq_job5}

    # RequiredTag: 'MultiProcessor' (but no Tag)
    # By doing this, we would be saying that this CE is accepting ONLY MultiProcessor payloads,
    # BUT since there are no Tags, we can't know what's POSSIBLE to run, so nothing should be matched
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "RequiredTag": "MultiProcessor"}, numQueuesToGet=4)
    assert result["OK"] is False

    # Tag: 'MultiProcessor' + RequiredTag: 'MultiProcessor'
    # By doing this, we are basically saying that this CE is accepting ONLY MultiProcessor payloads
    # which have ONLY the 'MultiProcessor' tag
    result = tqDB.matchAndGetTaskQueue(
        {"CPUTime": 50000, "Tag": "MultiProcessor", "RequiredTag": "MultiProcessor"},
        numQueuesToGet=4,
    )
    assert result["OK"]
    # this matches the tq_job1 as it is the only one that exposes the MultiProcessor tag ONLY
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1}

    # Tag: ['MultiProcessor', 'GPU'] + RequiredTag: 'MultiProcessor'
    # By doing this, we are basically saying that this CE is accepting MultiProcessor and GPU payloads
    # but requires to have the MultiProcessor tag
    result = tqDB.matchAndGetTaskQueue(
        {"CPUTime": 50000, "Tag": ["MultiProcessor", "GPU"], "RequiredTag": "MultiProcessor"},
        numQueuesToGet=4,
    )
    assert result["OK"]
    # this matches the tq_job1 as it is the only one that exposes the MultiProcessor tag ONLY
    # and tq_job4 because it has GPU and MultiProcessor tags
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job4}

    # CINECA type
    # We only want to have MultiProcessor payloads
    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 50000,
            "Tag": ["MultiProcessor", "17Processors", "20Processors", "4Processors"],
            "RequiredTag": "MultiProcessor",
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    # this matches the tq_job1 as it is the only one that exposes the MultiProcessor tag ONLY
    # and tq_job6 because it has 17Processors and MultiProcessor tags
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job6}

    # NumberOfProcessors and MaxRAM
    # This is translated to "#Processors" by the SiteDirector
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Tag": "4Processors"}, numQueuesToGet=4)
    assert result["OK"]
    # FIXME: this is not interpreted in any different way --- is it correct?
    # I believe it should be instead interpreted in a way similar to CPUTime
    # FIXME: the MaxRam parameter has a similar fate, and becomes "#GB",
    # and then there's no specific matching about it.

    for jobId in range(1, 8):
        result = tqDB.deleteJob(jobId)
        assert result["OK"]

    for tqId in [tq_job1, tq_job2, tq_job3, tq_job4, tq_job5, tq_job6]:
        result = tqDB.deleteTaskQueueIfEmpty(tqId)
        assert result["OK"]


def test_chainWithTagsAndPlatforms():
    """put - remove with parameters including one or more Tag(s) and platforms"""

    # platform only
    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Platforms": ["centos7"],
    }
    result = tqDB.insertJob(1, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(1)
    tq_job1 = result["Value"]
    assert tq_job1 > 0

    # Tag only
    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Tags": ["MultiProcessor"],
    }
    result = tqDB.insertJob(2, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(2)
    tq_job2 = result["Value"]
    assert tq_job2 > tq_job1

    # Platforms and Tag
    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Platforms": ["centos7"],
        "Tags": ["MultiProcessor"],
    }
    result = tqDB.insertJob(3, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(3)
    tq_job3 = result["Value"]
    assert tq_job3 > tq_job2

    # Tag and another platform
    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "myGroup",
        "CPUTime": 5000,
        "Platforms": ["slc6"],
        "Tags": ["MultiProcessor"],
    }
    result = tqDB.insertJob(4, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(4)
    tq_job4 = result["Value"]
    assert tq_job4 > tq_job3

    # We should be in this situation (TQIds are obviously invented):
    #
    # mysql Dirac@localhost:TaskQueueDB> select `TQId`,`JobId` FROM `tq_Jobs`
    # +--------+---------+
    # |   TQId |   JobId |
    # |--------+---------|
    # |    101 |       1 |
    # |    102 |       2 |
    # |    103 |       3 |
    # |    104 |       4 |
    # +--------+---------+
    #
    #
    # select * FROM `tq_TQToPlatforms`
    # +--------+---------+
    # |   TQId | Value   |
    # |--------+---------|
    # |    101 | centos7 |
    # |    103 | centos7 |
    # |    104 | debian  |
    # |    104 | slc6    |
    # +--------+---------+
    #
    # mysql Dirac@localhost:TaskQueueDB> select * FROM `tq_TQToTags`
    # +--------+-----------------+
    # |   TQId | Value           |
    # |--------+-----------------|
    # |    102 | MultiProcessor  |
    # |    103 | MultiProcessor  |
    # |    104 | MultiProcessor  |
    # +--------+-----------------+

    # Matching

    # Matching Everything

    # No Tag, Platform = "ANY"
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "ANY"}, numQueuesToGet=4)
    assert result["OK"]
    # this should match whatever that does not have tags required, so only tq_job1
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1}

    # Tag = "ANY", Platform = "ANY"
    result = tqDB.matchAndGetTaskQueue({"CPUTime": 50000, "Platform": "ANY", "Tag": "ANY"}, numQueuesToGet=4)
    assert result["OK"]
    # this should match whatever
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job4}

    # Tag = "ANY", Platform = "centos7"
    result = tqDB.matchAndGetTaskQueue(
        {"CPUTime": 50000, "Platform": "centos7", "Tag": "MultiProcessor"}, numQueuesToGet=4
    )
    assert result["OK"]
    # this should match whatever has platform == centos7, or no platform
    # and either no tags or the MultiProcessor tag
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3}

    for jobId in range(1, 8):
        result = tqDB.deleteJob(jobId)
        assert result["OK"]

    for tqId in [tq_job1, tq_job2, tq_job3, tq_job4]:
        result = tqDB.deleteTaskQueueIfEmpty(tqId)
        assert result["OK"]


def test_ComplexMatching():
    """test of a complex (realistic) matching. Something like:

    {'NumberOfProcessors': 1,
    'MaxRAM': 128000,
    'Site': ['Site_1', 'Site_2'],
    'Community': 'vo',
    'OwnerGroup': ['admin', 'prod', 'user'],
    'Platform': ['slc6', 'centos7'],
    'Tag': [],
    'CPUTime': 9999999}
    """

    # Let's first insert few jobs (no tags, for now, and always a platform)

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "admin",
        "CPUTime": 5000,
        "Sites": ["Site_1", "Site_2"],
        "Platforms": ["centos7"],
    }
    result = tqDB.insertJob(1, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(1)
    tq_job1 = result["Value"]

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "prod",
        "CPUTime": 5000,
        "Sites": ["Site_1"],
        "Platforms": ["slc6", "centos7"],
    }
    result = tqDB.insertJob(2, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(2)
    tq_job2 = result["Value"]

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "user",
        "CPUTime": 5000,
        "Sites": ["Site_2"],
        "Platforms": ["slc6", "centos7"],
    }
    result = tqDB.insertJob(3, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(3)
    tq_job3 = result["Value"]

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "user",
        "CPUTime": 5000,
        "Sites": ["Site_1", "Site_2"],
        "Platforms": ["ubuntu"],
    }
    result = tqDB.insertJob(4, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(4)
    tq_job4 = result["Value"]

    # now let's try some matching

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["slc6", "centos7"],
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": "ANY",
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["ubuntu"],
            "Tag": [],
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": "ANY",
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["slc6", "centos7", "ubuntu"],
            "Tag": [],
            "OwnerGroup": ["prod", "user"],
            "Site": "ANY",
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2, tq_job3, tq_job4}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["slc6", "centos7"],
            "Tag": [],
            "OwnerGroup": ["prod", "user"],
            "Site": "ANY",
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2, tq_job3}

    result = tqDB.matchAndGetTaskQueue(
        {"CPUTime": 9999999, "Platform": ["slc6", "centos7"], "OwnerGroup": ["prod", "user"]},
        numQueuesToGet=4,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2, tq_job3}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["slc6", "centos7"],
            "OwnerGroup": ["prod", "user"],
            "Site": ["Site_1", "Site_2"],
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2, tq_job3}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["slc6", "centos7"],
            "OwnerGroup": ["prod", "user"],
            "Site": ["Site_1"],
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job2}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 10,
            "Platform": ["slc6", "centos7"],
            "OwnerGroup": ["prod", "user"],
            "Site": ["Site_1", "Site_2"],
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    assert len(result["Value"]) == 0

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": "ANY",
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": ["ANY"],
        },
        numQueuesToGet=4,
    )
    assert result["OK"]
    assert len(result["Value"]) == 4

    # now inserting one without platform, and try again

    tqDefDict = {
        "Owner": "userName",
        "OwnerGroup": "user",
        "CPUTime": 5000,
        "Sites": ["Site_1", "Site_2"],
    }
    result = tqDB.insertJob(5, tqDefDict, 10)
    assert result["OK"]
    result = tqDB.getTaskQueueForJob(5)
    tq_job5 = result["Value"]

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["slc6", "centos7"],
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": "ANY",
        },
        numQueuesToGet=5,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job1, tq_job2, tq_job3, tq_job5}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["ubuntu"],
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": "Any",
        },
        numQueuesToGet=5,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4, tq_job5}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["ubuntu"],
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": "Any",
            "Tag": [],
        },
        numQueuesToGet=5,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4, tq_job5}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["ubuntu"],
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": ["Any", "Site_1"],
            "Tag": [],
        },
        numQueuesToGet=5,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4, tq_job5}

    result = tqDB.matchAndGetTaskQueue(
        {
            "CPUTime": 9999999,
            "Platform": ["ubuntu"],
            "OwnerGroup": ["admin", "prod", "user"],
            "Site": ["Any", "Site_1"],
            "Tag": ["SomeTAG"],
        },
        numQueuesToGet=5,
    )
    assert result["OK"]
    res = {int(x[0]) for x in result["Value"]}
    assert res == {tq_job4, tq_job5}

    for jobId in range(1, 8):
        result = tqDB.deleteJob(jobId)
        assert result["OK"]

    for tqId in [tq_job1, tq_job2, tq_job3, tq_job4, tq_job5]:
        result = tqDB.deleteTaskQueueIfEmpty(tqId)
        assert result["OK"]


def test_TQ():
    """test of various functions"""
    tqDefDict = {"Owner": "userName", "OwnerGroup": "myGroup", "CPUTime": 50000}
    tqDB.insertJob(123, tqDefDict, 10)

    result = tqDB.retrieveTaskQueues()
    assert result["OK"]
    assert list(result["Value"].values())[0] == {
        "Owner": "userName",
        "Jobs": 1,
        "OwnerGroup": "myGroup",
        "CPUTime": 86400,
        "Priority": 1.0,
    }
    result = tqDB.findOrphanJobs()
    assert result["OK"]
    result = tqDB.recalculateTQSharesForAll()
    assert result["OK"]

    # this will also remove the job
    result = tqDB.matchAndGetJob({"CPUTime": 300000})
    assert result["OK"]
    assert result["Value"]["matchFound"] is True
    assert result["Value"]["jobId"] in [123, 125]
    tq = result["Value"]["taskQueueId"]

    result = tqDB.deleteTaskQueueIfEmpty(tq)
    assert result["OK"]
