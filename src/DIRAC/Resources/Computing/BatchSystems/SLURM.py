""" SLURM.py is a DIRAC independent class representing SLURM batch system.
    SLURM objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import os
import re
import subprocess
import shlex

__RCSID__ = "$Id$"


class SLURM(object):

  def submitJob(self, **kwargs):
    """ Submit nJobs to the OAR batch system
    """

    resultDict = {}

    MANDATORY_PARAMETERS = ['Executable', 'OutputDir', 'ErrorDir',
                            'Queue', 'SubmitOptions']

    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    nJobs = kwargs.get('NJobs')
    if not nJobs:
      nJobs = 1

    outputDir = kwargs['OutputDir']
    errorDir = kwargs['ErrorDir']
    queue = kwargs['Queue']
    submitOptions = kwargs['SubmitOptions']
    executable = kwargs['Executable']
    numberOfProcessors = kwargs.get('NumberOfProcessors', 1)
    # numberOfNodes is treated as a string as it can contain values such as "2-4"
    # where 2 would represent the minimum number of nodes to allocate, and 4 the maximum
    numberOfNodes = kwargs.get('NumberOfNodes', '1')
    numberOfGPUs = kwargs.get('NumberOfGPUs')
    preamble = kwargs.get('Preamble')

    outFile = os.path.join(outputDir, "%jobid%")
    errFile = os.path.join(errorDir, "%jobid%")
    outFile = os.path.expandvars(outFile)
    errFile = os.path.expandvars(errFile)
    executable = os.path.expandvars(executable)

    jobIDs = []
    for _i in range(nJobs):
      jid = ''
      cmd = '%s; ' % preamble if preamble else ''
      # By default, all the environment variables of the submitter node are propagated to the workers
      # It can create conflicts during the installation of the pilots
      # --export restricts the propagation to the PATH variable to get a clean environment in the workers
      cmd += "sbatch --export=PATH "
      cmd += "-o %s/%%j.out " % outputDir
      cmd += "-e %s/%%j.err " % errorDir
      cmd += "--partition=%s " % queue
      # One pilot (task) per node, allocating a certain number of processors
      cmd += "--ntasks-per-node=1 "
      cmd += "--nodes=%s " % numberOfNodes
      cmd += "--cpus-per-task=%d " % numberOfProcessors
      if numberOfGPUs:
        cmd += "--gpus-per-task=%d " % int(numberOfGPUs)
      # Additional options
      cmd += "%s %s" % (submitOptions, executable)
      sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      output, error = sp.communicate()
      status = sp.returncode
      if status != 0 or not output:
        break

      lines = output.split('\n')
      for line in lines:
        result = re.search(r'Submitted batch job (\d*)', line)
        if result:
          jid = result.groups()[0]
          break

      if not jid:
        break

      jid = jid.strip()
      jobIDs.append(jid)

    if jobIDs:
      resultDict['Status'] = 0
      resultDict['Jobs'] = jobIDs
    else:
      resultDict['Status'] = status
      resultDict['Message'] = error
    return resultDict

  def killJob(self, **kwargs):
    """ Delete a job from SLURM batch scheduler. Input: list of jobs output: int
    """

    resultDict = {}

    MANDATORY_PARAMETERS = ['JobIDList', 'Queue']
    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    jobIDList = kwargs['JobIDList']
    if not jobIDList:
      resultDict['Status'] = -1
      resultDict['Message'] = 'Empty job list'
      return resultDict

    queue = kwargs['Queue']

    successful = []
    failed = []
    errors = ''
    for job in jobIDList:
      cmd = 'scancel --partition=%s %s' % (queue, job)
      sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      output, error = sp.communicate()
      status = sp.returncode
      if status != 0:
        failed.append(job)
        errors += error
      else:
        successful.append(job)

    resultDict['Status'] = 0
    if failed:
      resultDict['Status'] = 1
      resultDict['Message'] = errors
    resultDict['Successful'] = successful
    resultDict['Failed'] = failed
    return resultDict

  def getJobStatus(self, **kwargs):
    """ Get status of the jobs in the given list
    """

    resultDict = {}

    if 'JobIDList' not in kwargs or not kwargs['JobIDList']:
      resultDict['Status'] = -1
      resultDict['Message'] = 'Empty job list'
      return resultDict

    jobIDList = kwargs['JobIDList']

    jobIDs = ""
    for jobID in jobIDList:
      jobIDs += jobID + ","

    # displays accounting data for all jobs in the Slurm job accounting log or Slurm database
    cmd = "sacct -j %s -o JobID,STATE" % jobIDs
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode
    if status != 0:
      resultDict['Status'] = 1
      resultDict['Message'] = error
      return resultDict

    statusDict = {}
    lines = output.split('\n')
    jids = set()
    for line in lines[1:]:
      jid, status = line.split()
      jids.add(jid)
      if jid in jobIDList:
        if status in ['PENDING', 'SUSPENDED', 'CONFIGURING']:
          statusDict[jid] = 'Waiting'
        elif status in ['RUNNING', 'COMPLETING']:
          statusDict[jid] = 'Running'
        elif status in ['CANCELLED', 'PREEMPTED']:
          statusDict[jid] = 'Aborted'
        elif status in ['COMPLETED']:
          statusDict[jid] = 'Done'
        elif status in ['FAILED', 'TIMEOUT', 'NODE_FAIL']:
          statusDict[jid] = 'Failed'
        else:
          statusDict[jid] = 'Unknown'

    leftJobs = set(jobIDList) - jids
    for jid in leftJobs:
      statusDict[jid] = 'Unknown'

    # Final output
    resultDict['Status'] = 0
    resultDict['Jobs'] = statusDict
    return resultDict

  def getCEStatus(self, **kwargs):
    """  Get the overall status of the CE
    """

    resultDict = {}

    MANDATORY_PARAMETERS = ['Queue']
    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    user = kwargs.get('User')
    if not user:
      user = os.environ.get('USER')
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict

    queue = kwargs['Queue']

    cmd = "squeue --partition=%s --user=%s --format='%%j %%T' " % (queue, user)
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode
    if status != 0:
      resultDict['Status'] = 1
      resultDict['Message'] = error
      return resultDict

    waitingJobs = 0
    runningJobs = 0
    lines = output.split('\n')
    for line in lines[1:]:
      _jid, status = line.split()
      if status in ['PENDING', 'SUSPENDED', 'CONFIGURING']:
        waitingJobs += 1
      elif status in ['RUNNING', 'COMPLETING']:
        runningJobs += 1

    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
    return resultDict
