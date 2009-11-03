#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-check-file-integrity.py,v 1.7 2009/11/03 10:32:12 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-check-file-integrity.py,v 1.7 2009/11/03 10:32:12 acsmith Exp $"
__VERSION__ = "$Revision: 1.7 $"

from DIRAC import gLogger
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
