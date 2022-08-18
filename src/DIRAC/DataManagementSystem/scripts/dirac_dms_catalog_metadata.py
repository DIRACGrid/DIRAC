#!/usr/bin/env python
"""
Get metadata for the given file specified by its Logical File Name or for a list of files
contained in the specifed file

Example:
  $ dirac-dms-catalog-metadata /formation/user/v/vhamar/Example.txt
  FileName                                     Size        GUID                                     Status   Checksum
  /formation/user/v/vhamar/Example.txt         34          EDE6DDA4-3344-3F39-A993-8349BA41EB23     1        eed20d47
"""
from DIRAC import exit as DIRACExit
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(("LocalFile: Path to local file containing LFNs", "LFN:       Logical File Names"))
    Script.registerArgument(["Catalog:   file catalog plug-ins"], mandatory=False)
    Script.parseCommandLine()

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

    import os

    # parseCommandLine show help when mandatory arguments are not specified or incorrect argument
    inputFileName, catalogs = Script.getPositionalArgs(group=True)

    if os.path.exists(inputFileName):
        inputFile = open(inputFileName)
        string = inputFile.read()
        lfns = string.splitlines()
        inputFile.close()
    else:
        lfns = [inputFileName]

    res = FileCatalog(catalogs=catalogs).getFileMetadata(lfns)
    if not res["OK"]:
        print("ERROR:", res["Message"])
        DIRACExit(-1)

    print("FileName".ljust(100), "Size".ljust(10), "GUID".ljust(40), "Status".ljust(8), "Checksum".ljust(10))
    for lfn in sorted(res["Value"]["Successful"].keys()):
        metadata = res["Value"]["Successful"][lfn]
        checksum = ""
        if "Checksum" in metadata:
            checksum = str(metadata["Checksum"])
        size = ""
        if "Size" in metadata:
            size = str(metadata["Size"])
        guid = ""
        if "GUID" in metadata:
            guid = str(metadata["GUID"])
        status = ""
        if "Status" in metadata:
            status = str(metadata["Status"])
        print(f"{lfn.ljust(100)} {size.ljust(10)} {guid.ljust(40)} {status.ljust(8)} {checksum.ljust(10)}")

    for lfn in sorted(res["Value"]["Failed"].keys()):
        message = res["Value"]["Failed"][lfn]
        print(lfn, message)


if __name__ == "__main__":
    main()
