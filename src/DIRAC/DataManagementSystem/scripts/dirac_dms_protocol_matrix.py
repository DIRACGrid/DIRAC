#!/usr/bin/env python
"""
Generate a matrix of:

 * protocols used for interactive TPC
 * protocols used for FTS transfers
 * Intermediate hop for multihop transfer

The output is a CSV file containing a matrix source/destination.

By default, all the SEs are taken into account, but the matrix is factorized by using baseSEs.
If you want the detail per se, use --Full

Suppose you have the following in your CS::

  StorageElementBases{
    IN2P3-Disk
  }
  StorageElements{
    IN2P3-DST{
      BaseSE = IN2P3-Disk
    }
    IN2P3-User{
      BaseSE = IN2P3-Disk
    }
    AnotherDisk{
    }
  }

You can have the following combinations::

  DIRAC-PROD>dirac-dms-protocol-matrix
  Using sources: IN2P3-Disk, AnotherDisk
  Using target: IN2P3-Disk, AnotherDisk

  DIRAC-PROD>dirac-dms-protocol-matrix --FromSE=IN2P3-User
  Using sources: IN2P3-User
  Using target: IN2P3-Disk, AnotherDisk

  DIRAC-PROD>dirac-dms-protocol-matrix --FromSE=IN2P3-User --Bidirection
  Using sources: IN2P3-User
  Using target: IN2P3-User
"""
import csv
from collections import defaultdict

from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("", "FromSE=", "SE1[,SE2,...]")
    Script.registerSwitch("", "TargetSE=", "SE1[,SE2,...]")
    Script.registerSwitch("", "OutputFile=", "CSV output file (default /tmp/protocol-matrix.csv)")
    Script.registerSwitch("", "Bidirection", "If FromSE or TargetSE are specified, make a square matrix ")
    Script.registerSwitch("", "FTS", "Display the protocols sent to FTS")
    Script.registerSwitch("", "TPC", "Display the protocols tried for interactive TPC")
    Script.registerSwitch("", "Multihop", "Display the intermediate hop")
    Script.registerSwitch("", "Full", "Do not factorize with base SE")
    Script.registerSwitch("", "ExcludeSE=", "SEs to not take into account for the matrix")

    Script.parseCommandLine()
    from DIRAC import gConfig, gLogger, S_ERROR
    from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
    from DIRAC.DataManagementSystem.private.FTS3Utilities import getFTS3Plugin
    from DIRAC.Resources.Storage.StorageElement import StorageElement
    from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup

    fromSE = []
    targetSE = []
    excludeSE = []
    outputFile = "/tmp/protocol-matrix.csv"
    bidirection = False
    ftsTab = False
    tpcTab = False
    multihopTab = False
    fullOutput = False
    for switch in Script.getUnprocessedSwitches():
        if switch[0] == "FromSE":
            fromSE = switch[1].split(",")
        elif switch[0] == "TargetSE":
            targetSE = switch[1].split(",")
        elif switch[0] == "ExcludeSE":
            excludeSE = switch[1].split(",")
        elif switch[0] == "OutputFile":
            outputFile = switch[1]
        elif switch[0] == "Bidirection":
            bidirection = True
        elif switch[0] == "FTS":
            ftsTab = True
        elif switch[0] == "TPC":
            tpcTab = True
        elif switch[0] == "Multihop":
            multihopTab = True
        elif switch[0] == "Full":
            fullOutput = True

    if not any([ftsTab, tpcTab, multihopTab]):
        ftsTab = tpcTab = multihopTab = True

    fts3Plugin = getFTS3Plugin()
    thirdPartyProtocols = DMSHelpers().getThirdPartyProtocols()

    # List all the BaseSE
    seBases = gConfig.getSections("/Resources/StorageElementBases")["Value"]
    # construct a dict { baseSE : <inherited storages>}
    seForSeBases = {}

    allSEs = gConfig.getSections("/Resources/StorageElements/")["Value"]

    # Remove the SEs that we want to exclude
    allSEs = set(allSEs) - set(excludeSE)

    # We go through all the SEs and fill in the seForSEBases dict.
    # Basically, at the end of the loop, the dict will contain
    # for each baseSE an entry corresponding to one real storage (the first one)
    # and itself for each real non inherited SE
    for se in allSEs:
        baseSE = gConfig.getOption(f"/Resources/StorageElements/{se}/BaseSE").get("Value")
        if baseSE and not fullOutput:
            if baseSE not in seForSeBases:
                seForSeBases[baseSE] = se
        else:
            # If no baseSE, we put self
            seForSeBases[se] = se

    # Now let's take into account what source and destination we want.

    # If the user did not specify source or dest, generate everything
    if not fromSE and not targetSE:
        fromSE = list(seForSeBases)
        targetSE = list(seForSeBases)
    else:  # he specified at least source of dest
        # if bidirection, source and target should be the same
        if bidirection:
            if not fromSE and targetSE:  # we gave target, but no source
                fromSE = targetSE
            elif fromSE and not targetSE:  # we gave source but no target
                targetSE = fromSE
            elif fromSE and targetSE:  # we gave both
                fromSE = targetSE = list(set(fromSE + targetSE))

        else:  # no bidirection
            # he specified a targetSE
            if not fromSE:
                fromSE = list(seForSeBases)
            elif not targetSE:
                targetSE = list(seForSeBases)

    fromSE = sorted(fromSE)
    targetSE = sorted(targetSE)

    gLogger.notice(f"Using sources: {','.join(fromSE)}")
    gLogger.notice(f"Using target: {','.join(targetSE)}")

    # Now we construct the SE object for each SE that we want to appear
    ses = {}
    for se in set(fromSE + targetSE):
        ses[se] = StorageElement(seForSeBases.get(se, se))

    ret = getVOfromProxyGroup()
    if not ret["OK"] or not ret.get("Value", ""):
        gLogger.error("Aborting, Bad Proxy:", ret.get("Message", "Proxy does not belong to a VO!"))
        exit(1)
    vo = ret["Value"]
    gLogger.notice("Using the Virtual Organization:", vo)
    # dummy LFN, still has to follow lfn convention
    lfn = f"/{vo}/toto.xml"

    # Create a matrix of protocol src/dest

    tpMatrix = defaultdict(dict)
    ftsMatrix = defaultdict(dict)
    multihopMatrix = defaultdict(dict)

    # For each source and destination, generate the url pair, and the compatible third party protocols
    for src, dst in ((x, y) for x in fromSE for y in targetSE):
        if ftsTab:
            try:
                fts3TpcProto = fts3Plugin.selectTPCProtocols(sourceSEName=ses[src].name, destSEName=ses[dst].name)
                res = ses[dst].generateTransferURLsBetweenSEs(lfn, ses[src], fts3TpcProto)
            except ValueError as e:
                res = S_ERROR(str(e))
            if not res["OK"]:
                surls = "Error"
                gLogger.notice("Could not generate transfer URLS", f"src:{src}, dst:{dst}, error:{res['Message']}")
            else:
                # We only keep the protocol part of the url
                surls = "/".join(res["Value"]["Protocols"])
            ftsMatrix[src][dst] = f"{surls}"
            gLogger.verbose(f"{src} -> {dst}: {surls}")

        # Add also the third party protocols
        if tpcTab:
            proto = ",".join(ses[dst].negociateProtocolWithOtherSE(ses[src], thirdPartyProtocols)["Value"])

            tpMatrix[src][dst] = f"{proto}"

            gLogger.verbose(f"{src} -> {dst}: {proto}")

        if multihopTab:
            hop = fts3Plugin.findMultiHopSEToCoverUpForWLCGFailure(ses[src].name, ses[dst].name)
            multihopMatrix[src][dst] = hop

    # Write the matrix in the file
    with open(outputFile, "w") as csvfile:
        csvWriter = csv.writer(csvfile, delimiter=";", quoting=csv.QUOTE_MINIMAL)

        if tpcTab:
            csvWriter.writerow(["Direct TPC"])

            csvWriter.writerow(["src/dst"] + targetSE)

            for src in fromSE:
                srcRow = [src]
                for dst in targetSE:
                    srcRow.append(tpMatrix[src].get(dst, "NA"))
                csvWriter.writerow(srcRow)

            # make an empty line
            # csvWriter.writerow([""] * (len(targetSE) + 1))
            csvWriter.writerow([])
            csvWriter.writerow([])
            csvWriter.writerow([])

        if ftsTab:
            csvWriter.writerow(["FTS3 transfers"])

            csvWriter.writerow(["src/dst"] + targetSE)

            for src in fromSE:
                srcRow = [src]
                for dst in targetSE:
                    srcRow.append(ftsMatrix[src].get(dst, "NA"))
                csvWriter.writerow(srcRow)

            csvWriter.writerow([])
            csvWriter.writerow([])
            csvWriter.writerow([])

        if multihopTab:
            csvWriter.writerow(["Multihop"])

            csvWriter.writerow(["src/dst"] + targetSE)

            for src in fromSE:
                srcRow = [src]
                for dst in targetSE:
                    srcRow.append(multihopMatrix[src].get(dst, "NA"))
                csvWriter.writerow(srcRow)

            csvWriter.writerow([])
            csvWriter.writerow([])
            csvWriter.writerow([])
    gLogger.notice("Wrote Matrix to", outputFile)


if __name__ == "__main__":
    main()
