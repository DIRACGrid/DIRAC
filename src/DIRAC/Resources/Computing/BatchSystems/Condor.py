#########################################################################################
# Condor.py
# 10.11.2014
# Author: A.T.
#########################################################################################

""" Condor.py is a DIRAC independent class representing Condor batch system.
    Condor objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import re
import tempfile
import subprocess
import shlex
import os

__RCSID__ = "$Id$"


def parseCondorStatus(lines, jobID):
  """parse the condor_q or condor_history output for the job status

  :param lines: list of lines from the output of the condor commands, each line is a pair of jobID and statusID
  :type lines: python:list
  :param str jobID: jobID of condor job, e.g.: 123.53
  :returns: Status as known by DIRAC
  """
  jobID = str(jobID)
  for line in lines:
    l = line.strip().split()
    try:
      status = int(l[1])
    except (ValueError, IndexError):
      continue
    if l[0] == jobID:
      return {1: 'Waiting',
              2: 'Running',
              3: 'Aborted',
              4: 'Done',
              5: 'HELD'
              }.get(status, 'Unknown')
  return 'Unknown'


def treatCondorHistory(condorHistCall, qList):
  """concatenate clusterID and processID to get the same output as condor_q
  until we can expect condor version 8.5.3 everywhere

  :param str condorHistCall: condor_history command to run
  :param qList: list of jobID and status from condor_q output, will be modified in this function
  :type qList: python:list
  :returns: None
  """
  sp = subprocess.Popen(shlex.split(condorHistCall), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  output, _ = sp.communicate()
  status = sp.returncode

  # Join the ClusterId and the ProcId and add to existing list of statuses
  if status == 0:
    for line in output.split('\n'):
      values = line.strip().split()
      if len(values) == 3:
        qList.append("%s.%s %s" % tuple(values))


class Condor(object):

  def submitJob(self, **kwargs):
    """ Submit nJobs to the Condor batch system
    """

    resultDict = {}

    MANDATORY_PARAMETERS = ['Executable', 'OutputDir', 'SubmitOptions']

    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    nJobs = kwargs.get('NJobs')
    if not nJobs:
      nJobs = 1
    numberOfProcessors = kwargs.get('NumberOfProcessors')
    wholeNode = kwargs.get('WholeNode')
    outputDir = kwargs['OutputDir']
    executable = kwargs['Executable']
    submitOptions = kwargs['SubmitOptions']
    preamble = kwargs.get('Preamble')

    if wholeNode:
      requirements = '+RequiresWholeMachine=True\n Requirements = ( CAN_RUN_WHOLE_MACHINE ) && ( OpSys == "LINUX" )'
    else:
      requirements = 'Requirements = OpSys == "LINUX"'

    jdlFile = tempfile.NamedTemporaryFile(dir=outputDir, suffix=".jdl")
    jdlFile.write("""
    Executable = %s
    Universe = vanilla
    %s
    Initialdir = %s
    Output = $(Cluster).$(Process).out
    Error = $(Cluster).$(Process).err
    Log = test.log
    Environment = CONDOR_JOBID=$(Cluster).$(Process)
    Getenv = False

    request_cpus = %s

    Queue %s

    """ % (executable, requirements, outputDir, numberOfProcessors, nJobs)
    )

    jdlFile.flush()

    cmd = '%s; ' % preamble if preamble else ''
    cmd += 'condor_submit %s %s' % (submitOptions, jdlFile.name)
    sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    jdlFile.close()

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    submittedJobs = 0
    cluster = ''
    lines = output.split('\n')
    for line in lines:
      if 'cluster' in line:
        result = re.match(r'(\d+) job.*cluster (\d+)\.', line)
        if result:
          submittedJobs, cluster = result.groups()
          try:
            submittedJobs = int(submittedJobs)
          except BaseException:
            submittedJobs = 0

    if submittedJobs > 0 and cluster:
      resultDict['Status'] = 0
      resultDict['Jobs'] = []
      for i in range(submittedJobs):
        resultDict['Jobs'].append('.'.join([cluster, str(i)]))
    else:
      resultDict['Status'] = status
      resultDict['Message'] = error
    return resultDict

  def killJob(self, **kwargs):
    """ Kill jobs in the given list
    """

    resultDict = {}

    MANDATORY_PARAMETERS = ['JobIDList']
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

    successful = []
    failed = []
    errors = ''
    for job in jobIDList:
      sp = subprocess.Popen(shlex.split('condor_rm %s' % job), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

    MANDATORY_PARAMETERS = ['JobIDList']
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

    user = kwargs.get('User')
    if not user:
      user = os.environ.get('USER')
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict

    cmd = 'condor_q -submitter %s -af:j JobStatus' % user
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    qList = output.strip().split('\n')

    # FIXME: condor_history does only support j for autoformat from 8.5.3,
    # format adds whitespace for each field This will return a list of 1245 75 3
    # needs to cocatenate the first two with a dot
    condorHistCall = 'condor_history -af ClusterId ProcId JobStatus -submitter %s' % user
    treatCondorHistory(condorHistCall, qList)

    statusDict = {}
    if len(qList):
      for job in jobIDList:
        job = str(job)
        statusDict[job] = parseCondorStatus(qList, job)
        if statusDict[job] == 'HELD':
          statusDict[job] = 'Unknown'

    # Final output
    status = 0
    resultDict['Status'] = 0
    resultDict['Jobs'] = statusDict
    return resultDict

  def getCEStatus(self, **kwargs):
    """  Get the overall status of the CE
    """
    resultDict = {}

    user = kwargs.get('User')
    if not user:
      user = os.environ.get('USER')
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict

    waitingJobs = 0
    runningJobs = 0

    sp = subprocess.Popen(shlex.split('condor_q -submitter %s' % user), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      if "no record" in output:
        resultDict['Status'] = 0
        resultDict["Waiting"] = waitingJobs
        resultDict["Running"] = runningJobs
        return resultDict
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    if "no record" in output:
      resultDict['Status'] = 0
      resultDict["Waiting"] = waitingJobs
      resultDict["Running"] = runningJobs
      return resultDict

    if output:
      lines = output.split('\n')
      for line in lines:
        if not line.strip():
          continue
        if " I " in line:
          waitingJobs += 1
        elif " R " in line:
          runningJobs += 1

    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
    return resultDict
