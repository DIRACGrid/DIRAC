"""JobWrapperUtilities

This module contains the functions that are used by the JobWrapperTemplate to execute the JobWrapper.
"""
import errno
import os
import signal
import time

from DIRAC import gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus, JobStatus
from DIRAC.WorkloadManagementSystem.Client.JobManagerClient import JobManagerClient
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper


class JobWrapperError(Exception):
    """Custom exception for handling JobWrapper "genuine" errors"""

    def __init__(self, value):
        self.value = value
        super().__init__()

    def __str__(self):
        return str(self.value)


def killJobWrapper(job: JobWrapper) -> int:
    """Function that stops and ultimately kills the JobWrapper"""
    # Giving the JobWrapper some time to complete possible tasks, then trying to kill the process
    time.sleep(60)
    os.kill(job.currentPID, signal.SIGTERM)
    # wait for half a minute and if worker is still alive use REAL silencer
    time.sleep(30)
    # now you're dead
    os.kill(job.currentPID, signal.SIGKILL)
    return 1


def rescheduleFailedJob(jobID: str, minorStatus: str, jobReport: JobReport):
    """Function for rescheduling a jobID, setting a minorStatus"""

    rescheduleResult = JobStatus.RESCHEDULED

    try:
        gLogger.warn("Failure during", minorStatus)

        # Setting a job parameter does not help since the job will be rescheduled,
        # instead set the status with the cause and then another status showing the
        # reschedule operation.

        jobReport.setApplicationStatus(f"Failed {minorStatus} ", sendFlag=False)
        jobReport.setJobStatus(status=JobStatus.RESCHEDULED, minorStatus=minorStatus, sendFlag=False)

        # We must send Job States and Parameters before it gets reschedule
        jobReport.sendStoredStatusInfo()
        jobReport.sendStoredJobParameters()

        gLogger.info("Job will be rescheduled after exception during execution of the JobWrapper")

        result = JobManagerClient().rescheduleJob(int(jobID))
        if not result["OK"]:
            gLogger.warn(result["Message"])
            if "Maximum number of reschedulings is reached" in result["Message"]:
                rescheduleResult = JobStatus.FAILED

        return rescheduleResult
    except Exception:
        gLogger.exception("JobWrapperTemplate failed to reschedule Job")
        return JobStatus.FAILED


def sendJobAccounting(job: JobWrapper, status: str, minorStatus: str):
    """safe sending job accounting (always catching exceptions)"""
    try:
        job.sendJobAccounting(status, minorStatus)
    except Exception:  # pylint: disable=broad-except
        gLogger.exception(
            f"JobWrapper failed sending job accounting for [status:minorStatus] [{status}:{minorStatus}]",
        )


def createAndEnterWorkingDirectory(jobID: str, workingDirectory: str, jobReport: JobReport) -> bool:
    """Create the working directory and change to it"""
    wdir = os.path.expandvars(workingDirectory)
    if os.path.isdir(wdir):
        os.chdir(wdir)
        return True

    try:
        os.makedirs(wdir)  # this will raise an exception if wdir already exists (which is ~OK)
        if os.path.isdir(wdir):
            os.chdir(wdir)
    except OSError as osError:
        if osError.errno == errno.EEXIST and os.path.isdir(wdir):
            gLogger.exception("JobWrapperTemplate found that the working directory already exists")
            rescheduleFailedJob(jobID, "Working Directory already exists", jobReport)
        else:
            gLogger.exception("JobWrapperTemplate could not create working directory")
            rescheduleFailedJob(jobID, "Could Not Create Working Directory", jobReport)
        return False
    return True


def getJobWrapper(jobID: int, arguments: dict, jobReport: JobReport) -> JobWrapper:
    """Create a JobWrapper instance"""
    try:
        job = JobWrapper(jobID, jobReport)
        job.initialize(arguments)
    except Exception:  # pylint: disable=broad-except
        gLogger.exception("JobWrapper failed the initialization phase")
        rescheduleResult = rescheduleFailedJob(
            jobID=jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_INITIALIZATION, jobReport=jobReport
        )
        job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_INITIALIZATION)
        return None
    return job


def transferInputSandbox(job: JobWrapper, inputSandbox: list) -> bool:
    """Transfer the input sandbox"""
    try:
        result = job.transferInputSandbox(inputSandbox)
        if not result["OK"]:
            gLogger.warn(result["Message"])
            raise JobWrapperError(result["Message"])
    except JobWrapperError:
        gLogger.exception("JobWrapper failed to download input sandbox")
        rescheduleResult = rescheduleFailedJob(
            jobID=job.jobID, minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX, jobReport=job.jobReport
        )
        job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)
        return False
    except Exception:  # pylint: disable=broad-except
        gLogger.exception("JobWrapper raised exception while downloading input sandbox")
        rescheduleResult = rescheduleFailedJob(
            jobID=job.jobID, minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX, jobReport=job.jobReport
        )
        job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)
        return False
    return True


def resolveInputData(job: JobWrapper) -> bool:
    """Resolve the input data"""
    try:
        result = job.resolveInputData()
        if not result["OK"]:
            gLogger.warn(result["Message"])
            raise JobWrapperError(result["Message"])
    except JobWrapperError:
        gLogger.exception("JobWrapper failed to resolve input data")
        rescheduleResult = rescheduleFailedJob(
            jobID=job.jobID, minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION, jobReport=job.jobReport
        )
        job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION)
        return False
    except Exception:  # pylint: disable=broad-except
        gLogger.exception("JobWrapper raised exception while resolving input data")
        rescheduleResult = rescheduleFailedJob(
            jobID=job.jobID, minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION, jobReport=job.jobReport
        )
        job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION)
        return False
    return True


def processJobOutputs(job: JobWrapper) -> bool:
    """Process the job outputs"""
    try:
        result = job.processJobOutputs()
        if not result["OK"]:
            gLogger.warn(result["Message"])
            raise JobWrapperError(result["Message"])
    except JobWrapperError:
        gLogger.exception("JobWrapper failed to process output files")
        rescheduleResult = rescheduleFailedJob(
            jobID=job.jobID, minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS, jobReport=job.jobReport
        )
        job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS)
        return False
    except Exception:  # pylint: disable=broad-except
        gLogger.exception("JobWrapper raised exception while processing output files")
        rescheduleResult = rescheduleFailedJob(
            jobID=job.jobID, minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS, jobReport=job.jobReport
        )
        job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS)
        return False
    return True


def finalize(job: JobWrapper) -> int:
    """Finalize the job"""
    try:
        # Failed jobs will return !=0 / successful jobs will return 0
        return job.finalize()
    except Exception:  # pylint: disable=broad-except
        gLogger.exception("JobWrapper raised exception during the finalization phase")
        return 2


def executePayload(job: JobWrapper) -> bool:
    """Execute the payload"""
    try:
        result = job.execute()
        if not result["OK"]:
            gLogger.error("Failed to execute job", result["Message"])
            raise JobWrapperError((result["Message"], result["Errno"]))
    except JobWrapperError as exc:
        if exc.value[1] == 0 or str(exc.value[0]) == "0":
            gLogger.verbose("JobWrapper exited with status=0 after execution")
        if exc.value[1] == DErrno.EWMSRESC:
            gLogger.warn("Asked to reschedule job")
            rescheduleResult = rescheduleFailedJob(
                jobID=job.jobID, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION, jobReport=job.jobReport
            )
            job.sendJobAccounting(status=rescheduleResult, minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
            return False
        gLogger.exception("Job failed in execution phase")
        job.jobReport.setJobParameter("Error Message", repr(exc), sendFlag=False)
        job.jobReport.setJobStatus(
            status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC, sendFlag=False
        )
        job.sendFailoverRequest()
        job.sendJobAccounting(status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC)
        return False
    except Exception as exc:  # pylint: disable=broad-except
        gLogger.exception("Job raised exception during execution phase")
        job.jobReport.setJobParameter("Error Message", repr(exc), sendFlag=False)
        job.jobReport.setJobStatus(
            status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC, sendFlag=False
        )
        job.sendFailoverRequest()
        job.sendJobAccounting(status=JobStatus.FAILED, minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC)
        return False
    return True
