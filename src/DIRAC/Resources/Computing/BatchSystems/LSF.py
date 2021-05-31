#########################################################################################
# LSF.py
# 10.11.2014
# Author: A.T.
#########################################################################################

""" LSF.py is a DIRAC independent class representing LSF batch system.
    LSF objects are used as backend batch system representation for
    LocalComputingElement and SSHComputingElement classes
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
import re
import subprocess
import shlex
import os

__RCSID__ = "$Id$"


class LSF(object):

  def submitJob(self, **kwargs):
    """ Submit nJobs to the condor batch system
    """

    resultDict = {}

    MANDATORY_PARAMETERS = ['Executable', 'OutputDir', 'ErrorDir',
                            'WorkDir', 'SubmitOptions', 'Queue']
    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    nJobs = kwargs.get('NJobs', 1)
    preamble = kwargs.get('Preamble')

    outputs = []
    outputDir = kwargs['OutputDir']
    errorDir = kwargs['ErrorDir']
    executable = kwargs['Executable']
    queue = kwargs['Queue']
    submitOptions = kwargs['SubmitOptions']
    outputDir = os.path.expandvars(outputDir)
    errorDir = os.path.expandvars(errorDir)
    executable = os.path.expandvars(executable)
    for _i in range(int(nJobs)):
      cmd = '%s; ' % preamble if preamble else ''
      cmd += "bsub -o %s -e %s -q %s -J DIRACPilot %s %s" % (outputDir,
                                                             errorDir,
                                                             queue,
                                                             submitOptions,
                                                             executable)
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
        match = re.search(r'Job <(\d*)>', output)
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
      sp = subprocess.Popen(shlex.split('bkill %s' % job), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
      resultDict['Message'] = error
    resultDict['Successful'] = successful
    resultDict['Failed'] = failed
    return resultDict

  def getCEStatus(self, **kwargs):
    """ Method to return information on running and pending jobs.
    """

    resultDict = {}

    MANDATORY_PARAMETERS = ['Queue']
    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    queue = kwargs['Queue']

    cmd = "bjobs -q %s -a" % queue
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    waitingJobs = 0
    runningJobs = 0
    lines = output.split("\n")
    for line in lines:
      if line.count("PEND") or line.count('PSUSP'):
        waitingJobs += 1
      if line.count("RUN") or line.count('USUSP'):
        runningJobs += 1

    # Final output
    resultDict['Status'] = 0
    resultDict["Waiting"] = waitingJobs
    resultDict["Running"] = runningJobs
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

    cmd = 'bjobs ' + ' '.join(jobIDList)
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode

    if status != 0:
      resultDict['Status'] = status
      resultDict['Message'] = error
      return resultDict

    output = output.replace('\r', '')
    lines = output.split('\n')
    statusDict = {}
    for job in jobIDList:
      statusDict[job] = 'Unknown'
      for line in lines:
        if line.find(job) != -1:
          if line.find('UNKWN') != -1:
            statusDict[job] = 'Unknown'
          else:
            lsfStatus = line.split()[2]
            if lsfStatus in ['DONE', 'EXIT']:
              statusDict[job] = 'Done'
            elif lsfStatus in ['RUN', 'SSUSP']:
              statusDict[job] = 'Running'
            elif lsfStatus in ['PEND', 'PSUSP']:
              statusDict[job] = 'Waiting'

    # Final output
    status = 0
    resultDict['Status'] = 0
    resultDict['Jobs'] = statusDict
    return resultDict
