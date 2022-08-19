#!/usr/bin/env python
"""
Create a DIRAC MoveReplica request to be executed by the RMS
"""
import os
import time
from hashlib import md5

from DIRAC.Core.Base.Script import Script


def getLFNList(arg):
    """get list of LFNs"""
    lfnList = []
    if os.path.exists(arg):
        lfnList = [line.split()[0] for line in open(arg).read().splitlines()]
    else:
        lfnList = [arg]
    return list(set(lfnList))


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(" sourceSE:   source SE")
    Script.registerArgument(" LFN:        LFN or file containing a List of LFNs")
    Script.registerArgument(["targetSE:   target SEs"])
    Script.parseCommandLine()

    import DIRAC
    from DIRAC import gLogger

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    args = Script.getPositionalArgs()

    sourceSE = args[0]
    lfnList = getLFNList(args[1])
    targetSEs = list({se for targetSE in args[2:] for se in targetSE.split(",")})

    gLogger.info(
        "Will create request with 'MoveReplica' "
        "operation using %s lfns and %s target SEs" % (len(lfnList), len(targetSEs))
    )

    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
    from DIRAC.RequestManagementSystem.Client.Request import Request
    from DIRAC.RequestManagementSystem.Client.Operation import Operation
    from DIRAC.RequestManagementSystem.Client.File import File
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    from DIRAC.Core.Utilities.List import breakListIntoChunks

    lfnChunks = breakListIntoChunks(lfnList, 100)
    multiRequests = len(lfnChunks) > 1

    error = 0
    count = 0
    reqClient = ReqClient()
    fc = FileCatalog()
    for lfnChunk in lfnChunks:
        metaDatas = fc.getFileMetadata(lfnChunk)
        if not metaDatas["OK"]:
            gLogger.error("unable to read metadata for lfns: %s" % metaDatas["Message"])
            error = -1
            continue
        metaDatas = metaDatas["Value"]
        for failedLFN, reason in metaDatas["Failed"].items():
            gLogger.error(f"skipping {failedLFN}: {reason}")
        lfnChunk = set(metaDatas["Successful"])

        if not lfnChunk:
            gLogger.error("LFN list is empty!!!")
            error = -1
            continue

        if len(lfnChunk) > Operation.MAX_FILES:
            gLogger.error("too many LFNs, max number of files per operation is %s" % Operation.MAX_FILES)
            error = -1
            continue

        count += 1

        request = Request()
        request.RequestName = "{}_{}".format(
            md5(repr(time.time()).encode()).hexdigest()[:16],
            md5(repr(time.time()).encode()).hexdigest()[:16],
        )

        moveReplica = Operation()
        moveReplica.Type = "MoveReplica"
        moveReplica.SourceSE = sourceSE
        moveReplica.TargetSE = ",".join(targetSEs)

        for lfn in lfnChunk:
            metaDict = metaDatas["Successful"][lfn]
            opFile = File()
            opFile.LFN = lfn
            opFile.Size = metaDict["Size"]

            if "Checksum" in metaDict:
                # # should check checksum type, now assuming Adler32 (metaDict["ChecksumType"] = 'AD'
                opFile.Checksum = metaDict["Checksum"]
                opFile.ChecksumType = "ADLER32"
            moveReplica.addFile(opFile)

        request.addOperation(moveReplica)

        result = reqClient.putRequest(request)
        if not result["OK"]:
            gLogger.error("Failed to submit Request: %s" % (result["Message"]))
            error = -1
            continue

        if not multiRequests:
            gLogger.always("Request %d submitted successfully" % result["Value"])

    if multiRequests:
        gLogger.always("%d requests have been submitted" % (count))
    DIRAC.exit(error)


if __name__ == "__main__":
    main()
