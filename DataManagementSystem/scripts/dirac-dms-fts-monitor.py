#! /usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################
__RCSID__   = "$Id:  $"

from DIRAC.Core.Base import Script  

Script.setUsageMessage("""
Monitor the status of the given FTS request

Usage:
   %s <lfn|fileOfLFN> sourceSE targetSE guid server
""" % Script.scriptName)

Script.parseCommandLine()


from DIRAC.DataManagementSystem.Client.FTSRequest     import FTSRequest
import os,sys

if not len(sys.argv) >= 6:
  Script.showHelp()
  DIRAC.exit( -1 )
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
