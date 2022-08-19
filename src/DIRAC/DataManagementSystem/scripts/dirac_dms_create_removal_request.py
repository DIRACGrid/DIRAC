#!/usr/bin/env python
"""
Create a DIRAC RemoveReplica|RemoveFile request to be executed by the RMS
"""
import os
from hashlib import md5
import time

from DIRAC.Core.Base.Script import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(" SE:   StorageElement|All")
    Script.registerArgument(["LFN:  LFN or file containing a List of LFNs"])
    Script.parseCommandLine(ignoreErrors=False)

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    args = Script.getPositionalArgs()

    targetSE = args.pop(0)

    lfns = []
    for inputFileName in args:
        if os.path.exists(inputFileName):
            with open(inputFileName) as inputFile:
                string = inputFile.read()
            lfns.extend([lfn.strip() for lfn in string.splitlines()])
        else:
            lfns.append(inputFileName)

    from DIRAC.Resources.Storage.StorageElement import StorageElement
    import DIRAC

    # Check is provided SE is OK
    if targetSE != "All":
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

    requestOperation = "RemoveReplica"
    if targetSE == "All":
        requestOperation = "RemoveFile"

    for lfnList in breakListIntoChunks(lfns, 100):

        oRequest = Request()
        requestName = "{}_{}".format(
            md5(repr(time.time()).encode()).hexdigest()[:16],
            md5(repr(time.time()).encode()).hexdigest()[:16],
        )
        oRequest.RequestName = requestName

        oOperation = Operation()
        oOperation.Type = requestOperation
        oOperation.TargetSE = targetSE

        res = fc.getFileMetadata(lfnList)
        if not res["OK"]:
            print("Can't get file metadata: %s" % res["Message"])
            DIRAC.exit(1)
        if res["Value"]["Failed"]:
            print("Could not get the file metadata of the following, so skipping them:")
            for fFile in res["Value"]["Failed"]:
                print(fFile)

        lfnMetadata = res["Value"]["Successful"]

        for lfn in lfnMetadata:
            rarFile = File()
            rarFile.LFN = lfn
            rarFile.Size = lfnMetadata[lfn]["Size"]
            rarFile.Checksum = lfnMetadata[lfn]["Checksum"]
            rarFile.GUID = lfnMetadata[lfn]["GUID"]
            rarFile.ChecksumType = "ADLER32"
            oOperation.addFile(rarFile)

        oRequest.addOperation(oOperation)

        isValid = RequestValidator().validate(oRequest)
        if not isValid["OK"]:
            print("Request is not valid: ", isValid["Message"])
            DIRAC.exit(1)

        result = reqClient.putRequest(oRequest)
        if result["OK"]:
            print("Request %d Submitted" % result["Value"])
        else:
            print("Failed to submit Request: ", result["Message"])


if __name__ == "__main__":
    main()
