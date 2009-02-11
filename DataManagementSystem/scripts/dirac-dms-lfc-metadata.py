#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/scripts/dirac-dms-lfc-metadata.py,v 1.1 2009/02/11 12:08:23 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-dms-lfc-metadata.py,v 1.1 2009/02/11 12:08:23 acsmith Exp $"
__VERSION__ = "$ $"

from DIRAC.Core.Utilities.List import sortList
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
client = FileCatalog('LcgFileCatalogCombined')
import os,sys

if not len(sys.argv) == 2:
  print 'Usage: ./dirac-dms-lfc-metadata.py <lfn | fileContainingLfns>'
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

res = client.getFileMetadata(lfns)
if not res['OK']:
  print res['Message']
  sys.exit()

print '%s %s %s %s %s' % ('FileName'.ljust(100),'Size'.ljust(10),'GUID'.ljust(40),'Status'.ljust(8),'Checksum'.ljust(10))
for lfn in sortList(res['Value']['Successful'].keys()):
  metadata = res['Value']['Successful'][lfn]
  checksum = metadata['CheckSumValue']
  size = str(metadata['Size'])
  guid = metadata['GUID']
  status = metadata['Status']
  print '%s %s %s %s %s' % (lfn.ljust(100),size.ljust(10),guid.ljust(40),status.ljust(8),checksum.ljust(10))

for lfn in sortList(res['Value']['Failed'].keys()):
  message = res['Value']['Failed'][lfn]
  print lfn,message
