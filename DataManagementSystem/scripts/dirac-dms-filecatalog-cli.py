#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
import sys, os
import DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Core.Base import Script

fcType = ''

Script.registerSwitch( "f:", "file-catalog=", "   Catalog client type to use" )
Script.parseCommandLine( ignoreErrors = False )
res = gConfig.getSections( "/Resources/FileCatalogs/" )
if res['OK']:
  ##Take catalog -1 as default as it seems that the list returned is reversed compared to CS web interface
  fcType = res['Value'][-1]

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "f" or switch[0].lower() == "file-catalog":
    fcType = switch[1]

from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI

if fcType == "LcgFileCatalog" or fcType == "LFC" :
  from DIRAC.Resources.Catalog.LcgFileCatalogClient import LcgFileCatalogClient
  cli = FileCatalogClientCLI( LcgFileCatalogClient() )
  try:
    host = os.environ['LFC_HOST']
  except Exception, x:
    print "LFC_HOST environment variable not defined"
    sys.exit( 1 )
  print "Starting LFC FileCatalog client"
  cli.cmdloop()
elif fcType == "LcgFileCatalogProxy" or fcType == "LFCproxy":
  from DIRAC.Resources.Catalog.LcgFileCatalogProxyClient import LcgFileCatalogProxyClient
  cli = FileCatalogClientCLI( LcgFileCatalogProxyClient() )
  print "Starting LFC Proxy FileCatalog client"
  cli.cmdloop()
elif fcType == "LcgFileCatalogCombined" or fcType == "LFCCombined":
  from DIRAC.Resources.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
  cli = FileCatalogClientCLI( LcgFileCatalogCombinedClient() )
  print "Starting LFC FileCatalog Combined client"
  cli.cmdloop()
elif fcType == "FileCatalog" or fcType == "DiracFC" :
  from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
  cli = FileCatalogClientCLI( FileCatalogClient() )
  print "Starting DIRAC FileCatalog client"
  cli.cmdloop()
else:
  print "Unknown catalog type", fcType
