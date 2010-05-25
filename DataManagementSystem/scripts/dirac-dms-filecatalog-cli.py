#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import sys,os
import DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Core.Base import Script

fcType = ''

Script.registerSwitch( "f:", "file-catalog=","   Catalog client type to use")
Script.parseCommandLine( ignoreErrors = False )
res = gConfig.getSections("/Resources/FileCatalogs/")
if res['OK']:
  fcType = res['Value'][0]

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "f" or switch[0].lower() == "file-catalog":
    fcType = switch[1]

from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI

if fcType == "LcgFileCatalog":
  from DIRAC.Resources.Catalog.LcgFileCatalogClient import LcgFileCatalogClient
  cli = FileCatalogClientCLI(LcgFileCatalogClient())
  try:
    host = os.environ['LFC_HOST']
  except Exception,x:
    print "LFC_HOST environment variable not defined"
    sys.exit(1)
  print "Starting LFC FileCatalog client"
  cli.cmdloop()
elif fcType == "LcgFileCatalogProxy":
  from DIRAC.Resources.Catalog.LcgFileCatalogProxyClient import LcgFileCatalogProxyClient
  cli = FileCatalogClientCLI(LcgFileCatalogProxyClient())
  print "Starting LFC Proxy FileCatalog client"
  cli.cmdloop() 
elif fcType == "LcgFileCatalogCombined":
  from DIRAC.Resources.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
  cli = FileCatalogClientCLI(LcgFileCatalogCombinedClient())
  print "Starting LFC FileCatalog Combined client"
  cli.cmdloop()  
elif fcType == "FileCatalog":
  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  cli = FileCatalogClientCLI(FileCatalogClient())
  print "Starting DIRAC FileCatalog client"
  cli.cmdloop()  
else:
  print "Unknown catalog type", fcType
