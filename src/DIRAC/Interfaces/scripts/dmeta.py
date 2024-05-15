#!/usr/bin/env python
"""
Manipulate metadata in the FileCatalog.
Usage:
      list metadata indices: dmeta -I
      add metadata index: dmeta -i f|d meta=(int|float|string|date)
      delete metadata index: dmeta -i -r [metadata]
      or
      manipulate metadata for lfn: dmeta add|rm|ls [lfn] meta[=value]

Examples:
      $ dmeta add ./some_lfn_file some_meta="some_value"',
      $ dmeta -i f testindex=int
      $ dmeta ls ./some_lfn_file",
      $ dmeta rm ./some_lfn_file some_meta",
"""
import DIRAC
from DIRAC import S_OK, gLogger

from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument


class DMetaCommand:
    def run(self, lfn, metas):
        raise NotImplementedError


class DMetaAdd(DMetaCommand):
    def __init__(self, fcClient):
        self.fcClient = fcClient

    def run(self, lfn, metas):
        metadict = {}
        for meta in metas:
            name, value = meta.split("=")
            metadict[name] = value
        result = self.fcClient.setMetadataBulk({lfn: metadict})
        if not result["OK"]:
            gLogger.error(result["Message"])
        if result["Value"]["Failed"]:
            for ff in result["Value"]["Failed"]:
                print("Error:", ff, result["Value"]["Failed"][ff])


class DMetaRm(DMetaCommand):
    def __init__(self, fcClient):
        self.fcClient = fcClient

    def run(self, lfn, metas):
        result = self.fcClient.removeMetadata({lfn: metas})
        if not result["OK"]:
            gLogger.error(result["Message"])
        if result["Value"]["Failed"]:
            for ff in result["Value"]["Failed"]:
                print("Error:", ff, result["Value"]["Failed"][ff])


class DMetaList(DMetaCommand):
    def __init__(self, catalog):
        self.catalog = catalog

    def run(self, lfn, metas):
        retVal = self.catalog.getMeta(lfn)

        if not retVal["OK"]:
            gLogger.error(retVal["Message"])
            DIRAC.exit(-1)
        metadict = retVal["Value"]

        if not metas:
            for k, v in metadict.items():
                gLogger.notice(f"{k} = {v}")
        else:
            for meta in metas:
                if meta in metadict.keys():
                    gLogger.notice(f"{meta} = {metadict[meta]}")


from DIRAC.Core.Base.Script import Script


@Script()
def main():
    class Params:
        def __init__(self):
            self.index = False
            self.listIndex = False

        def setIndex(self, arg):
            gLogger.notice(f"Setting index: {arg}")
            self.index = arg
            return S_OK()

        def getIndex(self):
            return self.index

        def setListIndex(self, arg):
            self.listIndex = True

        def getListIndex(self):
            return self.listIndex

    params = Params()

    Script.registerSwitch("i:", "index=", "set or remove metadata indices", params.setIndex)
    Script.registerSwitch("I", "list-index", "list defined metadata indices", params.setListIndex)

    configCache = ConfigCache()
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    args = Script.getPositionalArgs()

    session = DSession()
    catalog = DCatalog()

    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import (
        FileCatalogClientCLI,
    )

    fc = FileCatalog()
    fccli = FileCatalogClientCLI(fc)

    if params.getIndex():
        if params.getIndex() == "r":
            for meta in args:
                result = fc.deleteMetadataField(meta)
        else:
            fdType = "-" + params.getIndex()
            for arg in args:
                rtype = None
                meta, mtype = arg.split("=")
                if mtype.lower()[:3] == "int":
                    rtype = "INT"
                elif mtype.lower()[:7] == "varchar":
                    rtype = mtype
                elif mtype.lower() == "string":
                    rtype = "VARCHAR(128)"
                elif mtype.lower() == "float":
                    rtype = "FLOAT"
                elif mtype.lower() == "date":
                    rtype = "DATETIME"
                elif mtype.lower() == "metaset":
                    rtype = "MetaSet"
                else:
                    gLogger.error(f"Error: illegal metadata type {mtype}")
                    DIRAC.exit(-1)
                res = fc.addMetadataField(meta, rtype, fdType)
                if not res["OK"]:
                    gLogger.error(res["Message"])
                    DIRAC.exit(-1)
        DIRAC.exit(0)

    if params.getListIndex():
        fccli.do_meta("show")
        DIRAC.exit(0)

    meta_commands = {
        "add": DMetaAdd(catalog.catalog),
        "rm": DMetaRm(catalog.catalog),
        "ls": DMetaList(catalog),
    }

    if len(args) < 2:
        gLogger.error(f"Error: Not enough arguments provided\n{Script.scriptName}:")
        Script.showHelp(exitCode=-1)

    command = args[0]

    if command not in meta_commands.keys():
        gLogger.error(f'Unknown dmeta command "{command}"')
        gLogger.notice(f"{Script.scriptName}:")
        Script.showHelp(exitCode=-1)

    command = meta_commands[command]

    lfn = pathFromArgument(session, args[1])

    metas = args[2:]

    command.run(lfn, metas)


if __name__ == "__main__":
    main()
