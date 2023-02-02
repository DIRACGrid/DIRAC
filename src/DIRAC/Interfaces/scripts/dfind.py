#!/usr/bin/env python
"""
find files in the FileCatalog

Examples:
    $ dfind . "some_integer_metadata>1"
"""
from DIRAC.Core.Base.Script import Script
from DIRAC.Interfaces.Utilities.DCommands import DSession
from DIRAC.Interfaces.Utilities.DCommands import DCatalog
from DIRAC.Interfaces.Utilities.DCommands import pathFromArgument
from DIRAC.Interfaces.Utilities.DConfigCache import ConfigCache


@Script()
def main():
    configCache = ConfigCache()
    Script.registerArgument(" lfn:         directory entry in the FileCatalog")
    Script.registerArgument(
        ['metaspec:    metadata index specifcation (of the form: "meta=value" or "meta<value", "meta!=value", etc.)'],
        mandatory=False,
    )
    Script.parseCommandLine(ignoreErrors=True)
    configCache.cacheConfig()

    lfn, metaspecs = Script.getPositionalArgs(group=True)

    session = DSession()
    catalog = DCatalog()

    lfn = pathFromArgument(session, lfn)

    from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import (
        FileCatalogClientCLI,
    )

    fccli = FileCatalogClientCLI(catalog.catalog)

    fccli.do_find("-q " + lfn + " " + " ".join(metaspecs))


if __name__ == "__main__":
    main()
