#!/bin/env python
"""
Create and put 'PutAndRegister' request with a single local file

  warning: make sure the file you want to put is accessible from DIRAC production hosts,
           i.e. put file on network fs (AFS or NFS), otherwise operation will fail!!!

Usage:
  dirac-dms-put-and-register-request [options] requestName LFN localFile targetSE

Arguments:
  requestName:  a request name
  LFN:          logical file name
  localFile:    local file you want to put
  targetSE:     target SE
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import os

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC
  from DIRAC import gLogger

  args = Script.getPositionalArgs()

  requestName = None
  LFN = None
  PFN = None
  targetSE = None
  if len(args) != 4:
    Script.showHelp()
  else:
    requestName = args[0]
    LFN = args[1]
    PFN = args[2]
    targetSE = args[3]

  if not os.path.isabs(LFN):
    gLogger.error("LFN should be absolute path!!!")
    DIRAC.exit(-1)

  gLogger.info("will create request '%s' with 'PutAndRegister' "
               "operation using %s pfn and %s target SE" % (requestName, PFN, targetSE))

  from DIRAC.RequestManagementSystem.Client.Request import Request
  from DIRAC.RequestManagementSystem.Client.Operation import Operation
  from DIRAC.RequestManagementSystem.Client.File import File
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.Core.Utilities.Adler import fileAdler

  if not os.path.exists(PFN):
    gLogger.error("%s does not exist" % PFN)
    DIRAC.exit(-1)
  if not os.path.isfile(PFN):
    gLogger.error("%s is not a file" % PFN)
    DIRAC.exit(-1)

  PFN = os.path.abspath(PFN)
  size = os.path.getsize(PFN)
  adler32 = fileAdler(PFN)

  request = Request()
  request.RequestName = requestName

  putAndRegister = Operation()
  putAndRegister.Type = "PutAndRegister"
  putAndRegister.TargetSE = targetSE
  opFile = File()
  opFile.LFN = LFN
  opFile.PFN = PFN
  opFile.Size = size
  opFile.Checksum = adler32
  opFile.ChecksumType = "ADLER32"
  putAndRegister.addFile(opFile)
  request.addOperation(putAndRegister)
  reqClient = ReqClient()
  putRequest = reqClient.putRequest(request)
  if not putRequest["OK"]:
    gLogger.error("unable to put request '%s': %s" % (requestName, putRequest["Message"]))
    DIRAC.exit(-1)

  gLogger.always("Request '%s' has been put to ReqDB for execution." % requestName)
  gLogger.always("You can monitor its status using command: 'dirac-rms-request %s'" % requestName)
  DIRAC.exit(0)


if __name__ == "__main__":
  main()
