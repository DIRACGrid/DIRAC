########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/JobWrapper/test/TestWatchdog.py,v 1.1 2007/11/04 18:11:41 paterson Exp $
# File  : TestWatchdogMac.py
# Author: Stuart Paterson
########################################################################

from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogMac import WatchdogMac

import unittest,types,time,sys

#from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogLinux import WatchdogLinux
from DIRAC.WorkloadManagementSystem.JobWrapper.WatchdogMac import WatchdogMac
from DIRAC                                                 import S_OK, S_ERROR
import os,threading,time

from DIRAC.Core.Utilities.Subprocess import Subprocess

import sys
script = 'myPythonScript.py'

EXECUTION_RESULT = {}
#############################################################################

class JobWrapper:

  def __init__(self):
    print "\n==========================================================="
    print "DIRAC version TEST"
    print "Developer tag: 0"
    print "==========================================================="

  def execute(self, executable):
    executable = os.path.expandvars(executable)
    thread = None
    spObject = None
    if os.path.exists(executable):
      spObject = Subprocess( 0 )
      command = str('`which python` '+executable)
      maxPeekLines = 200 # will be CS param in Job Wrapper class
      thread = ExecutionThread(spObject,command, maxPeekLines)
      thread.start()
    else:
      print 'Path to executable not found'

    pid = os.getpid()
    jobcputime = 60
    watchdog = WatchdogMac(pid, thread, spObject, jobcputime)
    watchdog.calibrate()
    if thread.isAlive():
      print 'Thread alive and started in Job Wrapper'
      systemFlag = 'mac'
      if systemFlag == 'mac':
        watchdog.run()
    else:
      print 'Thread stopped very quickly...'

    print 'Execution Result is : '
    print EXECUTION_RESULT

  def main(self, executable):
    currentPID = os.getpid()
    print 'Job Wrapper started under PID ',currentPID
    result  = self.execute(executable)

#############################################################################

class ExecutionThread(threading.Thread):

  def __init__(self,spObject,cmd,maxPeekLines):
    threading.Thread.__init__(self)
    self.cmd = cmd
    self.spObject = spObject
    self.outputLines = []
    self.maxPeekLines = maxPeekLines
    self.outputFile = 'appstd.out'

  def run(self):
    cmd = self.cmd
    spObject = self.spObject
    pid = os.getpid()
    start = time.time()
    output = spObject.systemCall( cmd, callbackFunction = self.sendOutput, shell = True )
    EXECUTION_RESULT['Thread'] = output
    timing = time.time() - start
    EXECUTION_RESULT['PID']=pid
    EXECUTION_RESULT['Timing']=timing

  def sendOutput(self,stdid,line):
    f = open(self.outputFile,'a')
    f.write('time: '+str(time.time())+'\n')
    f.write('stdid '+str(stdid)+'\n')
    f.write('line '+str(line)+'\n')
    f.close()
    self.outputLines.append(line)

  def getOutput(self,lines=0):
    if self.outputLines:
      size = len(self.outputLines)
      #reduce max size of output peeking
      if size > self.maxPeekLines:
        cut = size - self.maxPeekLines
        self.outputLines = self.outputLines[cut:]

      #restrict to smaller number of lines for regular
      #peeking by the watchdog
      if lines:
        size = len(self.outputLines)
        cut  = size - lines
        self.outputLines = self.outputLines[cut:]

      result = S_OK()
      result['Value'] = self.outputLines
    else:
      result = S_ERROR('No Job output found')

    return result

  #############################################################################
  #############################################################################

class WatchdogMacTestCase(unittest.TestCase):
  """ Base class for the Watchdog test cases
  """
  def test_runWatchdog(self):
    j = JobWrapper()
    j.main(script)

if __name__ == '__main__':
  print 'Starting Unit Test for Watchdog'
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(WatchdogMacTestCase)
  print 'Unit test finished, Job Wrapper harness completed execution'
 # suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(GetSystemInfoTestCase))
#  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)

