#!/usr/bin/env python
"""
Launch the File Catalog shell

Example:
  $ dirac-dms-filecatalog-cli
  Starting DIRAC FileCatalog client
  File Catalog Client $Revision: 1.17 $Date:
  FC:/>help

  Documented commands (type help <topic>):
  ========================================
  add    chmod  find   guid  ls     pwd       replicate  rmreplica   user
  cd     chown  get    id    meta   register  rm         size
  chgrp  exit   group  lcd   mkdir  replicas  rmdir      unregister

  Undocumented commands:
  ======================
  help

  FC:/>
"""

from DIRAC.Core.Base.Script import Script


@Script()
def main():
    fcType = None
    catalog = None
    Script.registerSwitch("f:", "file-catalog=", f"   Catalog client type to use (default {fcType})")
    Script.parseCommandLine(ignoreErrors=False)

    from DIRAC import exit as dexit

    for switch in Script.getUnprocessedSwitches():
        if switch[0].lower() == "f" or switch[0].lower() == "file-catalog":
            fcType = switch[1]

    if not fcType:
        # A particular catalog is not specified, try to instantiate the catalog container
        from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

        catalog = FileCatalog()
        if not catalog.valid:
            print("Failed to create the FileCatalog container. Try to use a specific catalog with -f option")
            dexit(-1)
        result = catalog.getMasterCatalogNames()
        if not result["OK"]:
            print("Failed to get the Master catalog name for the FileCatalog container")
            dexit(-1)
        masterCatalog = result["Value"][0]
        readCatalogs = [c[0] for c in catalog.getReadCatalogs()]
        writeCatalogs = [c[0] for c in catalog.getWriteCatalogs()]
        allCatalogs = list(set([masterCatalog] + readCatalogs + writeCatalogs))

        if len(allCatalogs) == 1:
            # If we have a single catalog in the container, let's use this catalog directly
            fcType = allCatalogs[0]
            catalog = None
        else:
            print("Starting FileCatalog container client:")
            print(f"   {masterCatalog} - Master")
            for cat in allCatalogs:
                if cat != masterCatalog:
                    cTypes = ["Write"] if cat in writeCatalogs else []
                    cTypes.extend(["Read"] if cat in readCatalogs else [])
                    print(f"   {cat} - {'-'.join(cTypes)}")
            print("")

    if fcType:
        from DIRAC.Resources.Catalog.FileCatalogFactory import FileCatalogFactory

        result = FileCatalogFactory().createCatalog(fcType)
        if not result["OK"]:
            print(result["Message"])
            dexit(-1)
        catalog = result["Value"]
        print(f"Starting {fcType} client")

    from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI

    cli = FileCatalogClientCLI(catalog)
    cli.cmdloop()


if __name__ == "__main__":
    main()
