#!/usr/bin/env python
""" This template will become the job wrapper that's actually executed.

    The JobWrapperTemplate is completed and invoked by the jobAgent and uses functionalities from JobWrapper module.
    It has to be an executable.

    The JobWrapperTemplate will reschedule the job according to certain criteria:
    - the working directory could not be created
    - the jobWrapper initialization phase failed
    - the inputSandbox download failed
    - the resolution of the inpt data failed
    - the JobWrapper ended with the status DErrno.EWMSRESC
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import json
import ast
import os
import errno
import time
import signal

sitePython = "@SITEPYTHON@"
if sitePython:
  sys.path.insert(0, "@SITEPYTHON@")

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.Utilities import DErrno

from DIRAC.WorkloadManagementSystem.JobWrapper.JobWrapper import JobWrapper, rescheduleFailedJob
from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport
from DIRAC.WorkloadManagementSystem.Client import JobStatus
from DIRAC.WorkloadManagementSystem.Client import JobMinorStatus


gJobReport = None


os.umask(0o22)


class JobWrapperError(Exception):
  """ Custom exception for handling JobWrapper "genuine" errors
  """

  def __init__(self, value):
    self.value = value
    super(JobWrapperError, self).__init__()

  def __str__(self):
    return str(self.value)


def killJobWrapper(job):
  """ Function that stops and ultimately kills the JobWrapper
  """
  # Giving the JobWrapper some time to complete possible tasks, then trying to kill the process
  time.sleep(60)
  os.kill(job.currentPID, signal.SIGTERM)
  # wait for half a minute and if worker is still alive use REAL silencer
  time.sleep(30)
  # now you're dead
  os.kill(job.currentPID, signal.SIGKILL)
  return 1


def sendJobAccounting(job, status, minorStatus):
  """ safe sending job accounting (always catching exceptions)
  """
  try:
    job.sendJobAccounting(status, minorStatus)
  except Exception as exc:  # pylint: disable=broad-except
    gLogger.exception('JobWrapper failed sending job accounting for [status:minorStatus] [%s:%s]' % (status,
                                                                                                     minorStatus),
                      lException=exc)


def execute(arguments):
  """ The only real function executed here
  """

  global gJobReport

  jobID = arguments['Job'].get('JobID', 0)
  os.environ['JOBID'] = str(jobID)
  jobID = int(jobID)

  if 'WorkingDirectory' in arguments:
    wdir = os.path.expandvars(arguments['WorkingDirectory'])
    if os.path.isdir(wdir):
      os.chdir(wdir)
    else:
      try:
        os.makedirs(wdir)  # this will raise an exception if wdir already exists (which is ~OK)
        if os.path.isdir(wdir):
          os.chdir(wdir)
      except OSError as osError:
        if osError.errno == errno.EEXIST and os.path.isdir(wdir):
          gLogger.exception('JobWrapperTemplate found that the working directory already exists')
          rescheduleResult = rescheduleFailedJob(jobID, 'Working Directory already exists')
        else:
          gLogger.exception('JobWrapperTemplate could not create working directory')
          rescheduleResult = rescheduleFailedJob(jobID, 'Could Not Create Working Directory')
        return 1

  gJobReport = JobReport(jobID, 'JobWrapper')

  try:
    job = JobWrapper(jobID, gJobReport)
    job.initialize(arguments)  # initialize doesn't return S_OK/S_ERROR
  except Exception as exc:  # pylint: disable=broad-except
    gLogger.exception('JobWrapper failed the initialization phase', lException=exc)
    rescheduleResult = rescheduleFailedJob(jobID=jobID,
                                           minorStatus=JobMinorStatus.JOB_WRAPPER_INITIALIZATION,
                                           jobReport=gJobReport)
    job.sendJobAccounting(status=rescheduleResult,
                          minorStatus=JobMinorStatus.JOB_WRAPPER_INITIALIZATION)
    return 1

  if 'InputSandbox' in arguments['Job']:
    gJobReport.commit()
    try:
      result = job.transferInputSandbox(arguments['Job']['InputSandbox'])
      if not result['OK']:
        gLogger.warn(result['Message'])
        raise JobWrapperError(result['Message'])
    except JobWrapperError:
      gLogger.exception('JobWrapper failed to download input sandbox')
      rescheduleResult = rescheduleFailedJob(jobID=jobID,
                                             minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX,
                                             jobReport=gJobReport)
      job.sendJobAccounting(status=rescheduleResult,
                            minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)
      return 1
    except Exception as exc:  # pylint: disable=broad-except
      gLogger.exception('JobWrapper raised exception while downloading input sandbox', lException=exc)
      rescheduleResult = rescheduleFailedJob(jobID=jobID,
                                             minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX,
                                             jobReport=gJobReport)
      job.sendJobAccounting(status=rescheduleResult,
                            minorStatus=JobMinorStatus.DOWNLOADING_INPUT_SANDBOX)
      return 1
  else:
    gLogger.verbose('Job has no InputSandbox requirement')

  gJobReport.commit()

  if 'InputData' in arguments['Job']:
    if arguments['Job']['InputData']:
      try:
        result = job.resolveInputData()
        if not result['OK']:
          gLogger.warn(result['Message'])
          raise JobWrapperError(result['Message'])
      except JobWrapperError:
        gLogger.exception('JobWrapper failed to resolve input data')
        rescheduleResult = rescheduleFailedJob(jobID=jobID,
                                               minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION,
                                               jobReport=gJobReport)
        job.sendJobAccounting(status=rescheduleResult,
                              minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION)
        return 1
      except Exception as exc:  # pylint: disable=broad-except
        gLogger.exception('JobWrapper raised exception while resolving input data', lException=exc)
        rescheduleResult = rescheduleFailedJob(jobID=jobID,
                                               minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION,
                                               jobReport=gJobReport)
        job.sendJobAccounting(status=rescheduleResult,
                              minorStatus=JobMinorStatus.INPUT_DATA_RESOLUTION)
        return 1
    else:
      gLogger.verbose('Job has a null InputData requirement:')
      gLogger.verbose(arguments)
  else:
    gLogger.verbose('Job has no InputData requirement')

  gJobReport.commit()

  try:
    result = job.execute()
    if not result['OK']:
      gLogger.error('Failed to execute job', result['Message'])
      raise JobWrapperError((result['Message'], result['Errno']))
  except JobWrapperError as exc:
    if exc.value[1] == 0 or str(exc.value[0]) == '0':
      gLogger.verbose('JobWrapper exited with status=0 after execution')
    if exc.value[1] == DErrno.EWMSRESC:
      gLogger.warn("Asked to reschedule job")
      rescheduleResult = rescheduleFailedJob(jobID=jobID,
                                             minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION,
                                             jobReport=gJobReport)
      job.sendJobAccounting(status=rescheduleResult,
                            minorStatus=JobMinorStatus.JOB_WRAPPER_EXECUTION)
      return 1
    gLogger.exception('Job failed in execution phase')
    gJobReport.setJobParameter('Error Message', repr(exc), sendFlag=False)
    gJobReport.setJobStatus(status=JobStatus.FAILED,
                            minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC,
                            sendFlag=False)
    job.sendFailoverRequest()
    job.sendJobAccounting(status=JobStatus.FAILED,
                          minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC)
    return 1
  except Exception as exc:  # pylint: disable=broad-except
    gLogger.exception('Job raised exception during execution phase', lException=exc)
    gJobReport.setJobParameter('Error Message', repr(exc), sendFlag=False)
    gJobReport.setJobStatus(status=JobStatus.FAILED,
                            minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC,
                            sendFlag=False)
    job.sendFailoverRequest()
    job.sendJobAccounting(status=JobStatus.FAILED,
                          minorStatus=JobMinorStatus.EXCEPTION_DURING_EXEC)
    return 1

  if 'OutputSandbox' in arguments['Job'] or 'OutputData' in arguments['Job']:
    try:
      result = job.processJobOutputs()
      if not result['OK']:
        gLogger.warn(result['Message'])
        raise JobWrapperError(result['Message'])
    except JobWrapperError as exc:
      gLogger.exception('JobWrapper failed to process output files')
      gJobReport.setJobParameter('Error Message', repr(exc), sendFlag=False)
      gJobReport.setJobStatus(status=JobStatus.FAILED,
                              minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS,
                              sendFlag=False)
      job.sendFailoverRequest()
      job.sendJobAccounting(status=JobStatus.FAILED,
                            minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS)

      return 2
    except Exception as exc:  # pylint: disable=broad-except
      gLogger.exception('JobWrapper raised exception while processing output files', lException=exc)
      gJobReport.setJobParameter('Error Message', repr(exc), sendFlag=False)
      gJobReport.setJobStatus(status=JobStatus.FAILED,
                              minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS,
                              sendFlag=False)
      job.sendFailoverRequest()
      job.sendJobAccounting(status=JobStatus.FAILED,
                            minorStatus=JobMinorStatus.UPLOADING_JOB_OUTPUTS)
      return 2
  else:
    gLogger.verbose('Job has no OutputData or OutputSandbox requirement')

  try:
    # Failed jobs will return !=0 / successful jobs will return 0
    return job.finalize()
  except Exception as exc:  # pylint: disable=broad-except
    gLogger.exception('JobWrapper raised exception during the finalization phase', lException=exc)
    return 2


##########################################################


ret = -3
try:
  jsonFileName = os.path.realpath(__file__) + '.json'
  with open(jsonFileName, 'r') as f:
    jobArgsFromJSON = json.loads(f.readlines()[0])
  jobArgs = ast.literal_eval(jobArgsFromJSON)
  if not isinstance(jobArgs, dict):
    raise TypeError("jobArgs is of type %s" % type(jobArgs))
  if 'Job' not in jobArgs:
    raise ValueError("jobArgs does not contain 'Job' key: %s" % str(jobArgs))
  ret = execute(jobArgs)
  gJobReport.commit()
except Exception as exc:  # pylint: disable=broad-except
  gLogger.exception("JobWrapperTemplate exception", lException=exc)
  try:
    gJobReport.commit()
    ret = -1
  except Exception as exc:  # pylint: disable=broad-except
    gLogger.exception("Could not commit the job report", lException=exc)
    ret = -2

sys.exit(ret)
