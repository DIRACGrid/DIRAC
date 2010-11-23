#! /usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
import sys,os

from DIRAC.Core.Utilities.List import sortList,randomize

if len(sys.argv) < 2:
  print 'Usage: dirac-dms-clean-directory <lfn|inputFileOfLfns>'
  sys.exit()
else:
  inputFileName = sys.argv[1]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = sortList(string.splitlines(),True)
  inputFile.close()
else:
  lfns = [inputFileName]

from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
rm = ReplicaManager()
for lfn in sortList(lfns):
  lfn = lfn.strip()
  if not lfn: continue
  print "Cleaning directory %s ... " % lfn,
  sys.stdout.flush()
  result = rm.cleanLogicalDirectory(lfn)
  if result['OK']:
    print 'OK'
  else:
    print "ERROR: %s" % result['Message']  
