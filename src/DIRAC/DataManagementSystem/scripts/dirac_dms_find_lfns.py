#!/usr/bin/env python
"""
Find files in the FileCatalog using file metadata

Examples::

  $ dirac-dms-find-lfns Path=/lhcb/user "Size>1000" "CreationDate<2015-05-15"
"""
from DIRAC.Core.Base.Script import Script


@Script()
def main():
    Script.registerSwitch("", "Path=", "    Directory path to search for")
    Script.registerSwitch("", "SE=", "    (comma-separated list of) SEs/SE-groups to be searched")
    # Registering arguments will automatically add their description to the help menu
    Script.registerArgument(
        ["metaspec: metadata index specification (of the form: " '"meta=value" or "meta<value", "meta!=value", etc.)'],
        mandatory=False,
    )
    Script.parseCommandLine(ignoreErrors=True)
    args = Script.getPositionalArgs()

    import DIRAC
    from DIRAC import gLogger
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    from DIRAC.DataManagementSystem.Client.MetaQuery import MetaQuery, FILE_STANDARD_METAKEYS
    from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

    path = "/"
    seList = None
    for opt, val in Script.getUnprocessedSwitches():
        if opt == "Path":
            path = val
        elif opt == "SE":
            seList = resolveSEGroup(val.split(","))

    if seList:
        args.append(f"SE={','.join(seList)}")
    fc = FileCatalog()
    result = fc.getMetadataFields()
    if not result["OK"]:
        gLogger.error("Can not access File Catalog:", result["Message"])
        DIRAC.exit(-1)
    typeDict = result["Value"]["FileMetaFields"]
    typeDict.update(result["Value"]["DirectoryMetaFields"])
    # Special meta tags
    typeDict.update(FILE_STANDARD_METAKEYS)

    if len(args) < 1:
        print(f"Error: No argument provided\n{Script.scriptName}:")
        gLogger.notice(f"MetaDataDictionary: \n{str(typeDict)}")
        Script.showHelp(exitCode=1)

    mq = MetaQuery(typeDict=typeDict)
    result = mq.setMetaQuery(args)
    if not result["OK"]:
        gLogger.error("Illegal metaQuery:", result["Message"])
        DIRAC.exit(-1)
    metaDict = result["Value"]
    path = metaDict.pop("Path", path)
    # check if path exists and is a directory
    result = fc.isDirectory(path)
    if not result["OK"]:
        gLogger.error("Can not access File Catalog:", result["Message"])
        DIRAC.exit(-1)
    if path not in result["Value"]["Successful"]:
        gLogger.error("Failed to query path status in file catalogue.", result["Message"])
        DIRAC.exit(-1)
    if not result["Value"]["Successful"][path]:
        gLogger.error(f"{path} does not exist or is not a directory.")
        DIRAC.exit(-1)
    result = fc.findFilesByMetadata(metaDict, path)
    if not result["OK"]:
        gLogger.error("Can not access File Catalog:", result["Message"])
        DIRAC.exit(-1)
    lfnList = sorted(result["Value"])

    gLogger.notice("\n".join(lfn for lfn in lfnList))


if __name__ == "__main__":
    main()
