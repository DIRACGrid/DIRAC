#!/usr/bin/env python
"""
Create a DIRAC RemoveReplica|RemoveFile request to be executed by the RMS

Usage:
  dirac-dms-create-removal-request [options] ... SE LFN ...

Arguments:
  SE:       StorageElement|All
  LFN:      LFN or file containing a List of LFNs
"""
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

__RCSID__ = "ea64b42 (2012-07-29 16:45:05 +0200) ricardo <Ricardo.Graciani@gmail.com>"

import os
from hashlib import md5
import time
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript
from DIRAC.Core.Utilities.List import breakListIntoChunks


@DIRACScript()
def main():
  Script.parseCommandLine(ignoreErrors=False)

  args = Script.getPositionalArgs()
  if len(args) < 2:
    Script.showHelp()

  targetSE = args.pop(0)

  lfns = []
  for inputFileName in args:
    if os.path.exists(inputFileName):
      inputFile = open(inputFileName, 'r')
      string = inputFile.read()
      inputFile.close()
      lfns.extend([lfn.strip() for lfn in string.splitlines()])
    else:
      lfns.append(inputFileName)

  from DIRAC.Resources.Storage.StorageElement import StorageElement
  import DIRAC
  # Check is provided SE is OK
  if targetSE != 'All':
    se = StorageElement(targetSE)
    if not se.valid:
      print(se.errorReason)
      print()
      Script.showHelp()

  from DIRAC.RequestManagementSystem.Client.Request import Request
  from DIRAC.RequestManagementSystem.Client.Operation import Operation
  from DIRAC.RequestManagementSystem.Client.File import File
  from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
  from DIRAC.RequestManagementSystem.private.RequestValidator import RequestValidator
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

  reqClient = ReqClient()
  fc = FileCatalog()

  requestOperation = 'RemoveReplica'
  if targetSE == 'All':
    requestOperation = 'RemoveFile'

  for lfnList in breakListIntoChunks(lfns, 100):

    oRequest = Request()
    requestName = "%s_%s" % (
        md5(repr(time.time()).encode()).hexdigest()[:16],
        md5(repr(time.time()).encode()).hexdigest()[:16],
    )
    oRequest.RequestName = requestName

    oOperation = Operation()
    oOperation.Type = requestOperation
    oOperation.TargetSE = targetSE

    res = fc.getFileMetadata(lfnList)
    if not res['OK']:
      print("Can't get file metadata: %s" % res['Message'])
      DIRAC.exit(1)
    if res['Value']['Failed']:
      print("Could not get the file metadata of the following, so skipping them:")
      for fFile in res['Value']['Failed']:
        print(fFile)

    lfnMetadata = res['Value']['Successful']

    for lfn in lfnMetadata:
      rarFile = File()
      rarFile.LFN = lfn
      rarFile.Size = lfnMetadata[lfn]['Size']
      rarFile.Checksum = lfnMetadata[lfn]['Checksum']
      rarFile.GUID = lfnMetadata[lfn]['GUID']
      rarFile.ChecksumType = 'ADLER32'
      oOperation.addFile(rarFile)

    oRequest.addOperation(oOperation)

    isValid = RequestValidator().validate(oRequest)
    if not isValid['OK']:
      print("Request is not valid: ", isValid['Message'])
      DIRAC.exit(1)

    result = reqClient.putRequest(oRequest)
    if result['OK']:
      print('Request %d Submitted' % result['Value'])
    else:
      print('Failed to submit Request: ', result['Message'])


if __name__ == "__main__":
  main()
