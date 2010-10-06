#! /usr/bin/env python
from DIRAC.Core.Base.Script                           import parseCommandLine
parseCommandLine()
from DIRAC.DataManagementSystem.Client.FTSRequest     import FTSRequest
import os,sys

if not len(sys.argv) >= 4:
  print 'Usage: dirac-dms-fts-submit <lfn|fileOfLFN> sourceSE targetSE'
  sys.exit()
else:
  inputFileName = sys.argv[1]
  sourceSE = sys.argv[2]
  targetSE = sys.argv[3]

if not os.path.exists(inputFileName):
  lfns = [inputFileName]
else:
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  inputFile.close()
  lfns = string.splitlines()

oFTSRequest = FTSRequest()
oFTSRequest.setSourceSE(sourceSE)
oFTSRequest.setTargetSE(targetSE)
for lfn in lfns:
  oFTSRequest.setLFN(lfn)
oFTSRequest.submit(monitor=True,printOutput=False)
