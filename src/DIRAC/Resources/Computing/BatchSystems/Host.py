#########################################################################
#  Host.py
#  4.11.2014
#  Author: A.T.
#########################################################################

""" Host - class for managing jobs on a host. Host objects are invoked
    with LocalComputingElement or SSHComputingElement objects
"""

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import os
import glob
import shutil
import shlex
import signal
try:
  import subprocess32 as subprocess
except ImportError:
  import subprocess
import stat
import json
import multiprocessing
from datetime import datetime, timedelta

__RCSID__ = "$Id$"

# Clean job info and output after so many days
CLEAN_DELAY = timedelta(7)


class Host(object):

  def __init__(self):
    self.nCores = 1
    try:
      self.nCores = multiprocessing.cpu_count()
    except BaseException:
      pass

  def submitJob(self, **kwargs):

    resultDict = {}

    args = dict(kwargs)

    MANDATORY_PARAMETERS = ['Executable', 'SharedDir', 'OutputDir', 'ErrorDir', 'WorkDir',
                            'InfoDir', 'ExecutionContext', 'JobStamps']

    for argument in MANDATORY_PARAMETERS:
      if argument not in args:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    nJobs = args.get('NJobs', 1)
    stamps = args['JobStamps']
    context = args.get('ExecutionContext', 'Local')
    jobidName = context.upper() + '_JOBID'
    nCores = args.get('NCores', 1)

    # Prepare the executor command
    runFileName = os.path.join(args['SharedDir'], 'run_detached.sh')
    runFileName = os.path.expandvars(runFileName)
    if os.path.isfile(runFileName):
      os.unlink(runFileName)
    runFile = open(runFileName, 'w')
    runFile.write("""
( exec </dev/null
#  echo $2
  exec > $2
#  echo $3
  exec 2> $3
#  echo $1
  exec setsid $1
) &
kill -0 $! > /dev/null 2>&1 || exit 1
echo $!
exit 0
""")
    runFile.close()
    os.chmod(runFileName, stat.S_IXUSR | stat.S_IRUSR)
    jobs = []
    output = ''
    args['RunFile'] = runFileName
    for _i in range(int(nJobs)):
      args['Stamp'] = stamps[_i]
      envDict = os.environ
      envDict[jobidName] = stamps[_i]
      try:
        jobDir = '%(WorkDir)s/%(Stamp)s' % args
        jobDir = os.path.expandvars(jobDir)
        os.makedirs(jobDir)
        os.chdir(jobDir)
        popenObject = subprocess.Popen(
            [
                "%(RunFile)s %(Executable)s %(OutputDir)s/%(Stamp)s.out %(ErrorDir)s/%(Stamp)s.err" %
                args],
            stdout=subprocess.PIPE,
            shell=True,
            env=envDict,
            universal_newlines=True)
        pid = popenObject.communicate()[0]
      except OSError as x:
        output = str(x)
        break
      pid = int(pid)
      if pid:
        # Store the job info
        jobInfo = {'PID': pid,
                   'SubmissionTime': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                   'JOBID': stamps[_i],
                   'NCores': nCores
                   }
        jobString = json.dumps(jobInfo)
        pidFileName = "%(InfoDir)s/%(Stamp)s.info" % args
        pidFileName = os.path.expandvars(pidFileName)
        with open(pidFileName, 'w') as pd:
          pd.write(jobString)
        jobs.append(stamps[_i])
      else:
        break

    if jobs:
      resultDict['Status'] = 0
      resultDict['Jobs'] = jobs
    else:
      resultDict['Status'] = 1
      resultDict['Message'] = output

    return resultDict

  def __cleanJob(self, stamp, infoDir, workDir, outputDir=None, errorDir=None):

    jobDir = os.path.join(workDir, stamp)
    if os.path.isdir(jobDir):
      shutil.rmtree(jobDir)
    pidFile = os.path.join(infoDir, '%s.info' % stamp)
    if os.path.isfile(pidFile):
      os.unlink(pidFile)
    if outputDir:
      outFile = os.path.join(outputDir, '%s.out' % stamp)
      if os.path.isfile(outFile):
        os.unlink(outFile)
    if errorDir:
      errFile = os.path.join(errorDir, '%s.err' % stamp)
      if os.path.isfile(errFile):
        os.unlink(errFile)

  def __getJobInfo(self, infoDir, stamp):

    jobInfo = {}
    infoFileName = os.path.join(infoDir, '%s.info' % stamp)
    infoFileName = os.path.expandvars(infoFileName)
    if os.path.exists(infoFileName):
      infoFile = open(infoFileName, 'r')
      jobInfo = infoFile.read().strip()
      infoFile.close()
      jobInfo = json.loads(jobInfo)
    return jobInfo

  def getCEStatus(self, **kwargs):
    """ Get the overall CE status
    """
    resultDict = {'Running': 0, 'Waiting': 0}

    MANDATORY_PARAMETERS = ['InfoDir', 'WorkDir', 'OutputDir', 'ErrorDir', 'User']

    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    user = kwargs.get('User')
    infoDir = kwargs.get('InfoDir')
    workDir = kwargs.get('WorkDir')
    outputDir = kwargs.get('OutputDir')
    errorDir = kwargs.get('ErrorDir')

    running = 0
    usedCores = 0
    infoDir = os.path.expandvars(infoDir)
    infoFiles = glob.glob('%s/*.info' % infoDir)
    for infoFileName in infoFiles:
      infoFileName = os.path.expandvars(infoFileName)
      infoFile = open(infoFileName, 'r')
      jobInfo = infoFile.read().strip()
      infoFile.close()
      jobInfo = json.loads(jobInfo)
      pid = jobInfo['PID']
      cmd = 'ps -f -p %s --no-headers | wc -l' % pid
      sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      output, error = sp.communicate()
      status = sp.returncode
      if status == 0:
        if output.strip() == '1':
          running += 1
          usedCores += jobInfo['NCores']
        else:
          stamp = jobInfo['JOBID']
          jobLife = datetime.utcnow() - datetime.strptime(jobInfo['SubmissionTime'], "%Y-%m-%d %H:%M:%S")
          if jobLife > CLEAN_DELAY:
            self.__cleanJob(stamp, infoDir, workDir, outputDir, errorDir)
      else:
        resultDict['Status'] = status
        return resultDict

    resultDict['Status'] = 0
    resultDict['Running'] = running
    availableCores = self.nCores - usedCores
    resultDict['AvailableCores'] = availableCores
    return resultDict

  def __checkPid(self, pid, user):

    if pid == 0:
      return "Unknown"
    cmd = 'ps  -f -p %s | grep %s | wc -l' % (pid, user)
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = sp.communicate()
    status = sp.returncode
    if status == 0 and output.strip() == "1":
      return "Running"
    return "Done"

  def getJobStatus(self, **kwargs):

    resultDict = {}

    MANDATORY_PARAMETERS = ['InfoDir', 'JobIDList', 'User']

    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    user = kwargs.get('User')
    infoDir = kwargs.get('InfoDir')
    jobStamps = kwargs.get('JobIDList')
    jobDict = {}
    for stamp in jobStamps:
      pid = self.__getJobInfo(infoDir, stamp).get('PID', 0)
      jobDict[stamp] = self.__checkPid(pid, user)

    resultDict['Status'] = 0
    resultDict['Jobs'] = jobDict
    return resultDict

  def killJob(self, **kwargs):

    resultDict = {}

    MANDATORY_PARAMETERS = ['InfoDir', 'WorkDir', 'OutputDir',
                            'ErrorDir', 'JobIDList', 'User']

    for argument in MANDATORY_PARAMETERS:
      if argument not in kwargs:
        resultDict['Status'] = -1
        resultDict['Message'] = 'No %s' % argument
        return resultDict

    user = kwargs.get('User')
    infoDir = kwargs.get('InfoDir')
    workDir = kwargs.get('WorkDir')
    outputDir = kwargs.get('OutputDir')
    errorDir = kwargs.get('ErrorDir')
    jobStamps = kwargs.get('JobIDList')
    jobDict = {}
    for stamp in jobStamps:
      pid = self.__getJobInfo(infoDir, stamp).get('PID', 0)
      if self.__checkPid(pid, user) == 'Running':
        os.kill(pid, signal.SIGKILL)
        self.__cleanJob(stamp, infoDir, workDir, outputDir, errorDir)
        jobDict[stamp] = 'Killed'
      else:
        jobDict[stamp] = 'Done'

    resultDict['Status'] = 0
    resultDict['Successful'] = jobStamps
    resultDict['Failed'] = []
    resultDict['Jobs'] = jobDict
    return resultDict
