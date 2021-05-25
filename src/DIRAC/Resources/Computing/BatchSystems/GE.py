############################################################################
#  GE class representing SGE batch system
#  10.11.2014
#  Author: A.T.
############################################################################

""" Torque.py is a DIRAC independent class representing Torque batch system.
    Torque objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes

    The GE relies on the SubmitOptions parameter to choose the right queue.
    This should be specified in the Queue description in the CS. e.g.

    SubmitOption = -l ct=6000
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import re
import shlex
import subprocess
import os

__RCSID__ = "$Id$"


class GE(object):

  def submitJob(self, **kwargs):
    """ Submit nJobs to the condor batch system
    """
    resultDict = {}

    MANDATORY_PARAMETERS = ['Executable', 'OutputDir', 'ErrorDir', 'SubmitOptions']
    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    nJobs = kwargs.get('NJobs', 1)
    preamble = kwargs.get('Preamble')

    outputs = []
    output = ''
    for _i in range(int(nJobs)):
      cmd = '%s; ' % preamble if preamble else ''
      cmd += "qsub -o %(OutputDir)s -e %(ErrorDir)s -N DIRACPilot %(SubmitOptions)s %(Executable)s" % kwargs
      sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      output, error = sp.communicate()
      status = sp.returncode
      if status == 0:
        outputs.append(output)
      else:
        break

    if outputs:
      resultDict['Status'] = 0
      resultDict['Jobs'] = []
      for output in outputs:
        match = re.match('Your job (\d*) ', output)
        if match:
          resultDict['Jobs'].append(match.groups()[0])
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

    jobIDList = kwargs.get('JobIDList')
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

    user = kwargs.get('User')
    if not user:
      user = os.environ.get('USER')
    if not user:
      resultDict['Status'] = -1
      resultDict['Message'] = 'No user name'
      return resultDict
    jobIDList = kwargs.get('JobIDList')
    if not jobIDList:
      resultDict['Status'] = -1
      resultDict['Message'] = 'Empty job list'
      return resultDict

    sp = subprocess.Popen(shlex.split('qstat -u %s' % user), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    jobDict = {}
    if output:
      lines = output.split('\n')
      for line in lines:
        l = line.strip()
        for job in jobIDList:
          if l.startswith(job):
            jobStatus = l.split()[4]
            if jobStatus in ['Tt', 'Tr']:
              jobDict[job] = 'Done'
            elif jobStatus in ['Rr', 'r']:
              jobDict[job] = 'Running'
            elif jobStatus in ['qw', 'h']:
              jobDict[job] = 'Waiting'

    sp = subprocess.Popen(shlex.split('qstat -u %s -s -z' % user), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status == 0:
      if output:
        lines = output.split('\n')
        for line in lines:
          l = line.strip()
          for job in jobIDList:
            if l.startswith(job):
              jobDict[job] = 'Done'

    if len(resultDict) != len(jobIDList):
      for job in jobIDList:
        if job not in jobDict:
          jobDict[job] = 'Unknown'

    # Final output
    status = 0
    resultDict['Status'] = 0
    resultDict['Jobs'] = jobDict
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

    cmd = 'qstat -u %s' % user
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    waitingJobs = 0
    runningJobs = 0
    doneJobs = 0

    if output:
      lines = output.split('\n')
      for line in lines:
        if not line.strip():
          continue
        if 'DIRACPilot %s' % user in line:
          jobStatus = line.split()[4]
          if jobStatus in ['Tt', 'Tr']:
            doneJobs += 1
          elif jobStatus in ['Rr', 'r']:
            runningJobs += 1
          elif jobStatus in ['qw', 'h']:
            waitingJobs = waitingJobs + 1

    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
    resultDict["Done"] = doneJobs
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
