#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

from DIRAC.Core.Base import Script 

Script.setUsageMessage("""
Check the integrity of the state of the storages and information in the File Catalogs
for a given file or a collection of files.

Usage:
   %s <lfn | fileContainingLfns> <SE> <status>
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC import gLogger
from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
import sys, os

if len( sys.argv ) < 2:
  Script.showHelp()
  DIRAC.exit( -1 )
else:
  inputFileName = sys.argv[1]

if os.path.exists( inputFileName ):
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

integrityClient = DataIntegrityClient()
res = integrityClient.catalogFileToBK( lfns )
if not res['OK']:
  gLogger.error( res['Message'] )
  sys.exit()
replicas = res['Value']['CatalogReplicas']
metadata = res['Value']['CatalogMetadata']
res = integrityClient.checkPhysicalFiles( replicas, metadata )
if not res['OK']:
  gLogger.error( res['Message'] )
  sys.exit()
