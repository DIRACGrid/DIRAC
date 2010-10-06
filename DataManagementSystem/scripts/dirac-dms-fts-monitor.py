#! /usr/bin/env python
from DIRAC.Core.Base.Script                           import parseCommandLine
parseCommandLine()
from DIRAC.DataManagementSystem.Client.FTSRequest     import FTSRequest
import os,sys

if not len(sys.argv) >= 6:
  print 'Usage: dirac-dms-fts-submit <lfn|fileOfLFN> sourceSE targetSE guid server'
  sys.exit()
else:
  inputFileName = sys.argv[1]
  sourceSE = sys.argv[2]
  targetSE = sys.argv[3]
  guid = sys.argv[4]
  server = sys.argv[5]

if not os.path.exists(inputFileName):
  lfns = [inputFileName]
else:
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  inputFile.close()
  lfns = string.splitlines()

oFTSRequest = FTSRequest()
for lfn in lfns:
  oFTSRequest.setLFN(lfn)
oFTSRequest.setFTSGUID(guid)
oFTSRequest.setFTSServer(server)
oFTSRequest.setSourceSE(sourceSE)
oFTSRequest.setTargetSE(targetSE)
oFTSRequest.monitor(untilTerminal=True)
