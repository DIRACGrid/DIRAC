"""Concurrency tests for the TaskQueueDB.

Like Test_TaskQueueDB.py, this only needs the TaskQueueDB and connects directly to it::

    python -m pytest tests/Integration/WorkloadManagementSystem/Test_TaskQueueDB_Concurrency.py

Many threads insert jobs with identical requirements (as JobScheduling optimizers do when a
production is submitted) while other threads match jobs and delete empty task queues.
"""

import itertools
import threading
import time

import DIRAC

DIRAC.initialize(require_auth=False, host_credentials=True)  # Initialize configuration

import pytest  # noqa: E402

from DIRAC.WorkloadManagementSystem.DB.TaskQueueDB import TaskQueueDB  # noqa: E402

tqDB = TaskQueueDB()

N_INSERTERS = 16
JOBS_PER_INSERTER = 25
FIRST_JOB_ID = 900_000_000

TQ_DEF = {
    "Owner": "userName",
    "OwnerGroup": "myGroup",
    "CPUTime": 50000,
    "Sites": ["LCG.Allowed.ch", "LCG.CERN.cern"],
    "JobTypes": ["MCSimulation"],
}


def _q(sql, args=None):
    result = tqDB._query(sql, args=args)
    assert result["OK"], result
    return result["Value"]


@pytest.fixture
def concurrent_run():
    """Run inserters, matchers, a TQ cleaner and an invariant watcher concurrently"""
    stop = threading.Event()
    jobIds = itertools.count(FIRST_JOB_ID)
    idLock = threading.Lock()
    report = {"insertErrors": [], "inserted": [], "matched": [], "wrongSite": [], "strippedTQs": set()}

    def inserter():
        for _ in range(JOBS_PER_INSERTER):
            with idLock:
                jobId = next(jobIds)
            result = tqDB.insertJob(jobId, TQ_DEF, 5)
            if result["OK"]:
                report["inserted"].append(jobId)
            else:
                report["insertErrors"].append(result["Message"])

    def matcher(site, bucket):
        while not stop.is_set():
            result = tqDB.matchAndGetJob({"CPUTime": 500000, "Site": site, "JobType": "MCSimulation"})
            if result["OK"] and result["Value"]["matchFound"]:
                report[bucket].append(result["Value"]["jobId"])
            else:
                time.sleep(0.005)

    def cleaner():
        while not stop.is_set():
            for (tqId,) in _q("SELECT TQId FROM tq_TaskQueues"):
                tqDB.deleteTaskQueueIfEmpty(tqId)
            tqDB.cleanOrphanedTaskQueues()

    def watcher():
        # A TQ holding jobs must never lose its requirement rows
        while not stop.is_set():
            for (tqId,) in _q(
                "SELECT DISTINCT j.TQId FROM tq_Jobs j "
                "WHERE NOT EXISTS (SELECT 1 FROM tq_TQToSites s WHERE s.TQId = j.TQId)"
            ):
                report["strippedTQs"].add(tqId)
            time.sleep(0.005)

    background = [
        threading.Thread(target=matcher, args=("LCG.CERN.cern", "matched")),
        threading.Thread(target=matcher, args=("LCG.CERN.cern", "matched")),
        # No job allows this site: anything matched here is a bug
        threading.Thread(target=matcher, args=("LCG.Forbidden.xx", "wrongSite")),
        threading.Thread(target=cleaner),
        threading.Thread(target=watcher),
    ]
    inserters = [threading.Thread(target=inserter) for _ in range(N_INSERTERS)]
    try:
        for t in background + inserters:
            t.start()
        for t in inserters:
            t.join()
        deadline = time.time() + 60
        while time.time() < deadline and _q("SELECT COUNT(*) FROM tq_Jobs WHERE JobId >= %s", (FIRST_JOB_ID,))[0][0]:
            time.sleep(0.1)
    finally:
        stop.set()
        for t in background:
            t.join()
        for (jobId,) in _q("SELECT JobId FROM tq_Jobs WHERE JobId >= %s", (FIRST_JOB_ID,)):
            tqDB.deleteJob(jobId)
        tqDB.cleanOrphanedTaskQueues()
    yield report


def test_concurrentInsertMatchAndCleanup(concurrent_run):
    report = concurrent_run
    assert report["insertErrors"] == []
    assert len(report["inserted"]) == N_INSERTERS * JOBS_PER_INSERTER
    # Every inserted job was matched exactly once
    assert sorted(report["matched"]) == sorted(report["inserted"])
    assert report["wrongSite"] == []
    assert report["strippedTQs"] == set()


def test_concurrentInsertCreatesFewTaskQueues():
    """Jobs with identical requirements inserted concurrently should share a task queue"""
    jobIds = list(range(FIRST_JOB_ID + 10_000, FIRST_JOB_ID + 10_000 + N_INSERTERS))
    barrier = threading.Barrier(N_INSERTERS)
    errors = []

    def insert(jobId):
        barrier.wait()
        result = tqDB.insertJob(jobId, dict(TQ_DEF, Sites=["LCG.Other.ch"]), 5)
        if not result["OK"]:
            errors.append(result["Message"])

    threads = [threading.Thread(target=insert, args=(jobId,)) for jobId in jobIds]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        tqIds = {tqDB.getTaskQueueForJob(jobId)["Value"] for jobId in jobIds}
        assert len(tqIds) == 1
        enabled = _q(
            "SELECT Enabled FROM tq_TaskQueues WHERE TQId IN (%s)" % ",".join(["%s"] * len(tqIds)), tuple(tqIds)
        )
        assert all(row[0] == 1 for row in enabled)
    finally:
        for jobId in jobIds:
            tqDB.deleteJob(jobId)
        tqDB.cleanOrphanedTaskQueues()


def test_failedTaskQueueCreationLeavesNothingBehind():
    """A task queue and its requirement rows must appear together, or not at all"""
    tqCount = "SELECT COUNT(*) FROM tq_TaskQueues"
    before = _q(tqCount)[0][0]
    # Too long for tq_TQToSites.Value: the TQ row is inserted before this fails
    badDef = dict(TQ_DEF, Sites=["LCG." + "x" * 80 + ".ch"])
    result = tqDB.insertJob(FIRST_JOB_ID + 20_000, badDef, 5)
    assert not result["OK"]
    assert _q(tqCount)[0][0] == before
