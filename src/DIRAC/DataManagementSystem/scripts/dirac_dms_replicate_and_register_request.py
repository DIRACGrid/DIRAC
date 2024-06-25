#!/bin/env python
"""
Create and put 'ReplicateAndRegister' request.
"""
import os
from DIRAC.Core.Base.Script import Script
from DIRAC import gLogger
import DIRAC


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
    catalog = None
    Script.registerSwitch("C:", "Catalog=", "Catalog to use")
    Script.registerSwitch("N:", "ChunkSize=", "Number of files per request")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(" requestName:  a request name")
    Script.registerArgument(" LFNs:         single LFN or file with LFNs")
    Script.registerArgument(["targetSE:     target SE"])
    Script.parseCommandLine()

    chunksize = 100
    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "C" or switch[0].lower() == "catalog":
            catalog = switch[1]
        if switch[0] == "N" or switch[0].lower() == "chunksize":
            chunksize = int(switch[1])
    args = Script.getPositionalArgs()

    requestName = None
    targetSEs = None
    if len(args) < 3:
        Script.showHelp(exitCode=1)

    requestName = args[0]
    lfnList = getLFNList(args[1])
    targetSEs = list({se for targetSE in args[2:] for se in targetSE.split(",")})

    gLogger.info(
        "Will create request '%s' with 'ReplicateAndRegister' "
        "operation using %s lfns and %s target SEs" % (requestName, len(lfnList), len(targetSEs))
    )

    from DIRAC.RequestManagementSystem.Client.Request import Request
    from DIRAC.RequestManagementSystem.Client.Operation import Operation
    from DIRAC.RequestManagementSystem.Client.File import File
    from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    from DIRAC.Core.Utilities.List import breakListIntoChunks

    lfnChunks = breakListIntoChunks(lfnList, chunksize)
    multiRequests = len(lfnChunks) > 1

    error = 0
    count = 0
    reqClient = ReqClient()
    fc = FileCatalog()
    requestIDs = []
    for lfnChunk in lfnChunks:
        metaDatas = fc.getFileMetadata(lfnChunk)
        if not metaDatas["OK"]:
            gLogger.error(f"unable to read metadata for lfns: {metaDatas['Message']}")
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
            gLogger.error(f"too many LFNs, max number of files per operation is {Operation.MAX_FILES}")
            error = -1
            continue

        count += 1
        request = Request()
        request.RequestName = requestName if not multiRequests else "%s_%d" % (requestName, count)

        replicateAndRegister = Operation()
        replicateAndRegister.Type = "ReplicateAndRegister"
        replicateAndRegister.TargetSE = ",".join(targetSEs)
        if catalog is not None:
            replicateAndRegister.Catalog = catalog

        for lfn in lfnChunk:
            metaDict = metaDatas["Successful"][lfn]
            opFile = File()
            opFile.LFN = lfn
            opFile.Size = metaDict["Size"]

            if "Checksum" in metaDict:
                # # should check checksum type, now assuming Adler32 (metaDict["ChecksumType"] = 'AD'
                opFile.Checksum = metaDict["Checksum"]
                opFile.ChecksumType = "ADLER32"
            replicateAndRegister.addFile(opFile)

        request.addOperation(replicateAndRegister)

        putRequest = reqClient.putRequest(request)
        if not putRequest["OK"]:
            gLogger.error(f"unable to put request '{request.RequestName}': {putRequest['Message']}")
            error = -1
            continue
        requestIDs.append(str(putRequest["Value"]))
        if not multiRequests:
            gLogger.always(f"Request '{request.RequestName}' has been put to ReqDB for execution.")

    if multiRequests:
        gLogger.always("%d requests have been put to ReqDB for execution, with name %s_<num>" % (count, requestName))
    if requestIDs:
        gLogger.always(f"RequestID(s): {' '.join(requestIDs)}")
    gLogger.always("You can monitor requests' status using command: 'dirac-rms-request <requestName/ID>'")
    DIRAC.exit(error)


if __name__ == "__main__":
    main()
