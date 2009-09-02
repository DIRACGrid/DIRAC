#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-check-file-integrity.py,v 1.6 2009/09/02 20:40:40 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-check-file-integrity.py,v 1.6 2009/09/02 20:40:40 acsmith Exp $"
__VERSION__ = "$Revision: 1.6 $"

from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
import sys,os

if len(sys.argv) < 2:
  print 'Usage: dirac-dms-check-file-integrity <lfn | fileContainingLfns>'
  sys.exit()
else:
  inputFileName = sys.argv[1]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

integrityClient = DataIntegrityClient()
res = integrityClient.catalogFileToBK(lfns)
if not res['OK']:
  gLogger.error(res['Message'])
  sys.exit()
replicas = res['Value']['CatalogReplicas']
metadata = res['Value']['CatalogMetadata']
res = integrityClient.checkPhysicalFiles(replicas,metadata)
if not res['OK']:
  gLogger.error(res['Message'])
  sys.exit()
