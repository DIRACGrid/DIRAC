"""
This module contains constants and lists for the possible job minor statuses.
"""
__RCSID__ = "$Id$"

#
PENDING_REQUESTS = 'Pending Requests'
#
EXEC_COMPLETE = 'Execution Complete'
#
JOB_INITIALIZATION = 'Job Initialization'
#
APPLICATION = 'Application'
#
APP_NOT_FOUND = 'Application not found'
#
APP_THREAD_FAILED = 'Application thread failed'
#
APP_THREAD_NOT_COMPLETE = 'Application thread did not complete'
#
APP_SUCCESS = 'Application Finished Successfully'
#
APP_ERRORS = 'Application Finished With Errors'
#
EXCEPTION_DURING_EXEC = 'Exception During Execution'
#
GOING_RESCHEDULE = 'Going to reschedule job'
#
DOWNLOADING_INPUT_SANDBOX = 'Downloading InputSandbox'
#
DOWNLOADING_INPUT_SANDBOX_LFN = 'Downloading InputSandbox LFN(s)'
#
INPUT_DATA_RESOLUTION = 'Input Data Resolution'
#
RESOLVING_OUTPUT_SANDBOX = 'Resolving Output Sandbox'
#
UPLOADING_OUTPUT_SANDBOX = 'Uploading Output Sandbox'
#
OUTPUT_SANDBOX_UPLOADED = 'Output Sandbox Uploaded'
#
UPLOADING_OUTPUT_DATA = 'Uploading Output Data'
#
UPLOADING_JOB_OUTPUTS = 'Uploading Outputs'
#
OUTPUT_DATA_UPLOADED = 'Output Data Uploaded'
#
FAILED_DOWNLOADING_INPUT_SANDBOX = 'Failed Downloading InputSandbox'
#
FAILED_DOWNLOADING_INPUT_SANDBOX_LFN = 'Failed Downloading InputSandbox LFN(s)'
#
FAILED_SENDING_REQUESTS = 'Failed sending requests'
#
STALLED_PILOT_NOT_RUNNING = 'Job stalled: pilot not running'
#
WATCHDOG_STALLED = 'Watchdog identified this job as stalled'
#
ILLEGAL_JOB_JDL = 'Illegal Job JDL'
#
INPUT_NOT_AVAILABLE = 'Input Data Not Available'
#
INPUT_CONTAINS_SLASHES = 'Input data contains //'
#
INPUT_INCORRECT = 'Input data not correctly specified'
#
JOB_WRAPPER_INITIALIZATION = 'Job Wrapper Initialization'
#
JOB_WRAPPER_EXECUTION = 'JobWrapper execution'
#
JOB_EXCEEDED_WALL_CLOCK = 'Job has exceeded maximum wall clock time'
#
JOB_INSUFFICIENT_DISK = 'Job has insufficient disk space to continue'
#
JOB_EXCEEDED_CPU = 'Job has reached the CPU limit of the queue'
#
NO_CANDIDATE_SITE_FOUND = 'No candidate sites available'
#
RECEIVED_KILL_SIGNAL = 'Received Kill signal'
