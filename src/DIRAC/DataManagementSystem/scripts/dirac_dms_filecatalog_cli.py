#!/usr/bin/env python
"""
Launch the File Catalog shell

Usage:
   dirac-dms-filecatalog-cli [option]

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
from __future__ import print_function
from __future__ import absolute_import
from __future__ import division

import sys

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.DIRACScript import DIRACScript


@DIRACScript()
def main():
  fcType = 'FileCatalog'
  Script.registerSwitch("f:", "file-catalog=", "   Catalog client type to use (default %s)" % fcType)
  Script.parseCommandLine(ignoreErrors=False)

  from DIRAC import gConfig, exit as dexit
  from DIRAC.Resources.Catalog.FileCatalogFactory import FileCatalogFactory

  __RCSID__ = "$Id$"

  fcType = gConfig.getValue("/LocalSite/FileCatalog", "")

  res = gConfig.getSections("/Resources/FileCatalogs", listOrdered=True)
  if not res['OK']:
    dexit(1)
  fcList = res['Value']
  if not fcType:
    if res['OK']:
      fcType = res['Value'][0]

  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() == "f" or switch[0].lower() == "file-catalog":
      fcType = switch[1]

  if not fcType:
    print("No file catalog given and defaults could not be obtained")
    sys.exit(1)

  from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI

  result = FileCatalogFactory().createCatalog(fcType)
  if not result['OK']:
    print(result['Message'])
    if fcList:
      print("Possible choices are:")
      for fc in fcList:
        print(' ' * 5, fc)
    sys.exit(1)
  print("Starting %s client" % fcType)
  catalog = result['Value']
  cli = FileCatalogClientCLI(catalog)
  cli.cmdloop()


if __name__ == "__main__":
  main()
