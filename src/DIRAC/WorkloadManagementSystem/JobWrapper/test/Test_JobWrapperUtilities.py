"""Unit tests for JobWrapperUtilities.executePayload's failure-reporting paths.

These complement the integration tests in Test_JobWrapperTemplate.py.
"""
from unittest.mock import MagicMock

from DIRAC import S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapperUtilities import executePayload


def _make_mock_job(execute_result, wmsMajorStatus="Running"):
    """Build a minimal JobWrapper-like mock that returns the given execute_result."""
    job = MagicMock()
    job.jobID = 1
    job.execute.return_value = execute_result
    job.wmsMajorStatus = wmsMajorStatus
    job.jobReport = MagicMock()
    return job


def test_executePayload_preserves_minor_status_when_postProcess_set_failed():
    """When postProcess set wmsMajorStatus=FAILED, executePayload must not overwrite
    the minor status with EXCEPTION_DURING_EXEC.

    This is the path triggered by a watchdog kill: postProcess returns
    S_ERROR("Payload killed by watchdog: ...") with the minor status already set
    to (e.g.) JOB_EXCEEDED_CPU. The wrapper must respect that.
    """
    job = _make_mock_job(
        execute_result=S_ERROR(f"Payload killed by watchdog: {JobMinorStatus.JOB_EXCEEDED_CPU}"),
        wmsMajorStatus=JobStatus.FAILED,
    )

    assert executePayload(job) is False

    # setJobStatus with EXCEPTION_DURING_EXEC must not have been called.
    for call in job.jobReport.setJobStatus.call_args_list:
        assert call.kwargs.get("minorStatus") != JobMinorStatus.EXCEPTION_DURING_EXEC

    # sendJobAccounting must be called with no minor-status override (preserves
    # whatever wmsMinorStatus postProcess set on the job).
    job.sendJobAccounting.assert_called_once_with()


def test_executePayload_falls_back_to_exception_when_postProcess_was_bypassed():
    """When postProcess never ran (e.g. process() returned S_ERROR before reaching
    postProcess), wmsMajorStatus is still RUNNING and no minor-status was reported.
    Fall back to EXCEPTION_DURING_EXEC so the user at least sees the job failed.
    """
    job = _make_mock_job(
        execute_result=S_ERROR("Payload process could not start after 5 seconds"),
        wmsMajorStatus="Running",
    )

    assert executePayload(job) is False

    # The fallback override must have been called.
    failed_calls = [
        call
        for call in job.jobReport.setJobStatus.call_args_list
        if call.kwargs.get("minorStatus") == JobMinorStatus.EXCEPTION_DURING_EXEC
    ]
    assert len(failed_calls) == 1
    assert failed_calls[0].kwargs["status"] == JobStatus.FAILED

    # sendJobAccounting must be called WITH explicit FAILED kwargs in the bypass case:
    # setJobStatus on jobReport doesn't update wmsMajorStatus / wmsMinorStatus on the
    # JobWrapper, so without these kwargs the accounting record would still show the
    # stale "Running" / "Application" state.
    job.sendJobAccounting.assert_called_once_with(
        status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC
    )


def test_executePayload_reschedule_path_unchanged():
    """EWMSRESC sub-branch returns early and is not affected by the clobber fix."""
    job = _make_mock_job(
        execute_result=S_ERROR(DErrno.EWMSRESC, "Job will be rescheduled"),
        wmsMajorStatus=JobStatus.FAILED,
    )

    assert executePayload(job) is False

    # The reschedule branch calls sendJobAccounting with explicit args.
    job.sendJobAccounting.assert_called_once()
    call = job.sendJobAccounting.call_args
    assert call.kwargs.get("minorStatus") == JobMinorStatus.JOB_WRAPPER_EXECUTION
