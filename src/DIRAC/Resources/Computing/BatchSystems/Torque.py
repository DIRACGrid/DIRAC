#########################################################################################
# Torque.py
# 10.11.2014
# Author: A.T.
#########################################################################################

""" Torque.py is a DIRAC independent class representing Torque batch system.
    Torque objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import subprocess
import shlex
import os

__RCSID__ = "$Id$"


class Torque(object):

  def submitJob(self, **kwargs):
    """ Submit nJobs to the Torque batch system
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

    jobIDs = []
    status = -1

    preamble = kwargs.get("Preamble")
    for _i in range(int(nJobs)):
      cmd = '%s; ' % preamble if preamble else ''
      cmd += "qsub -o %(OutputDir)s " \
             "-e %(ErrorDir)s " \
             "-q %(Queue)s " \
             "-N DIRACPilot " \
             "%(SubmitOptions)s %(Executable)s 2>/dev/null" % kwargs
      sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      output, error = sp.communicate()
      status = sp.returncode
      if status == 0:
        jobIDs.append(output.split('.')[0])
      else:
        break

    if jobIDs:
      resultDict['Status'] = 0
      resultDict['Jobs'] = jobIDs
    else:
      resultDict['Status'] = status
      resultDict['Message'] = error
    return resultDict

  def getJobStatus(self, **kwargs):
    """ Get the status information for the given list of jobs
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

    jobDict = {}
    for job in jobIDList:
      if not job:
        continue
      jobNumber = job
      jobDict[jobNumber] = job

    cmd = 'qstat ' + ' '.join(jobIDList)
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    statusDict = {}
    output = output.replace('\r', '')
    lines = output.split('\n')
    for job in jobDict:
      statusDict[jobDict[job]] = 'Unknown'
      for line in lines:
        if line.find(job) != -1:
          if line.find('Unknown') != -1:
            statusDict[jobDict[job]] = 'Unknown'
          else:
            torqueStatus = line.split()[4]
            if torqueStatus in ['E']:
              statusDict[jobDict[job]] = 'Done'
            elif torqueStatus in ['R', 'C']:
              statusDict[jobDict[job]] = 'Running'
            elif torqueStatus in ['S', 'W', 'Q', 'H', 'T']:
              statusDict[jobDict[job]] = 'Waiting'

    # Final output
    status = 0
    resultDict['Status'] = 0
    resultDict['Jobs'] = statusDict
    return resultDict

  def getCEStatus(self, **kwargs):
    """ Get the overall CE status
    """

    resultDict = {}

    user = kwargs.get('User')
    if not user:
      user = os.environ.get('USER')
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict

    cmd = 'qselect -u %s -s WQ | wc -l; qselect -u %s -s R | wc -l' % (user, user)
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    waitingJobs, runningJobs = output.split()[:2]

    # Final output
    try:
      resultDict['Status'] = 0
      resultDict["Waiting"] = int(waitingJobs)
      resultDict["Running"] = int(runningJobs)
    except Exception as e:
      resultDict['Status'] = -1
      resultDict['Output'] = output
      resultDict['Message'] = 'Exception: %s' % str(e)

    return resultDict

  def killJob(self, **kwargs):
    """ Kill all jobs in the given list
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
      sp = subprocess.Popen(shlex.split('qdel %s' % job), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

  def getJobOutputFiles(self, **kwargs):
    """ Get output file names and templates for the specific CE
    """
    resultDict = {}
    MANDATORY_PARAMETERS = ['JobIDList', 'OutputDir', 'ErrorDir']
    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    outputDir = kwargs['OutputDir']
    errorDir = kwargs['ErrorDir']

    outputTemplate = '%s/DIRACPilot.o%%s' % outputDir
    errorTemplate = '%s/DIRACPilot.e%%s' % errorDir
    outputTemplate = os.path.expandvars(outputTemplate)
    errorTemplate = os.path.expandvars(errorTemplate)

    jobIDList = kwargs['JobIDList']

    jobDict = {}
    for job in jobIDList:
      jobDict[job] = {}
      jobDict[job]['Output'] = outputTemplate % job
      jobDict[job]['Error'] = errorTemplate % job

    resultDict['Status'] = 0
    resultDict['Jobs'] = jobDict
    resultDict['OutputTemplate'] = outputTemplate
    resultDict['ErrorTemplate'] = errorTemplate

    return resultDict
