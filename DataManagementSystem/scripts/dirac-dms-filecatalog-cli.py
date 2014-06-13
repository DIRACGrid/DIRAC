#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Launch the File Catalog shell

Usage:
   %s [option]
""" % Script.scriptName )

catalogs = []
Script.registerSwitch( "f:", "file-catalog=", "   Catalog name to use, default is the standard VO choice" )

Script.parseCommandLine( ignoreErrors = False )
  
import sys, os
import DIRAC
from DIRAC import gLogger, gConfig
from DIRAC.Resources.Catalog.FileCatalogFactory import FileCatalogFactory
from DIRAC.Core.Security.ProxyInfo import getVOfromProxyGroup
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import Resources

if __name__ == "__main__":

  result = getVOfromProxyGroup()
  if not result['OK']:
    gLogger.notice( 'Error:', result['Message'] )
    DIRAC.exit( 1 )
  vo = result['Value']  
  resources = Resources( vo = vo )
  
  result = gConfig.getSections("/LocalSite/Catalogs")
  if result['OK']:
    catalogs = result['Value']
  
  userCatalogs = []
  for switch in Script.getUnprocessedSwitches():
    if switch[0].lower() == "f" or switch[0].lower() == "file-catalog":
      userCatalogs.append( switch[1] )
  if userCatalogs:
    catalogs = userCatalogs   
  
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.DataManagementSystem.Client.FileCatalogClientCLI import FileCatalogClientCLI
  if catalogs:
    catalog = FileCatalog( catalogs = catalogs, vo = vo ) 
  else:
    catalog = FileCatalog( vo = vo )   
    
  writeCatalogs = []  
  for catalogName, oCatalog, master in catalog.getWriteCatalogs():
    writeCatalogs.append( catalogName )
  readCatalogs = []  
  for catalogName, oCatalog, master in catalog.getReadCatalogs():
    readCatalogs.append( catalogName )  
    
  if not writeCatalogs and not readCatalogs:
    print "No File Catalog client is available, exiting ... "
    DIRAC.exit( -1 )   
    
  print "Starting File Catalog Console with:" 
  if writeCatalogs:
    print "   %s write enabled catalogs" % ','.join( writeCatalogs )  
  if readCatalogs:
    print "   %s read enabled catalogs" % ','.join( readCatalogs )      
  
  cli = FileCatalogClientCLI( catalog )
  cli.cmdloop()
