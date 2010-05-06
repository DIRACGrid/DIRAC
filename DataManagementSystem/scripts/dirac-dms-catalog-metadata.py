#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"

from DIRAC.Core.Utilities.List                          import sortList
from DIRAC.DataManagementSystem.Client.ReplicaManager   import ReplicaManager
import os,sys

if not len(sys.argv) >= 2:
  print 'Usage: ./dirac-dms-catalog-metadata.py <lfn | fileContainingLfns> [Catalog]'
  sys.exit()
else:
  inputFileName = sys.argv[1]
  catalogs = []
  if len(sys.argv) == 3:
    catalogs = [sys.argv[2]]  

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

rm = ReplicaManager()
res = rm.getCatalogFileMetadata(lfns,catalogs=catalogs)
if not res['OK']:
  print res['Message']
  sys.exit()

print '%s %s %s %s %s' % ('FileName'.ljust(100),'Size'.ljust(10),'GUID'.ljust(40),'Status'.ljust(8),'Checksum'.ljust(10))
for lfn in sortList(res['Value']['Successful'].keys()):
  metadata = res['Value']['Successful'][lfn]
  checksum = ''
  if metadata.has_key('CheckSumValue'):
    checksum = str(metadata['CheckSumValue'])
  size = ''
  if metadata.has_key('Size'):
    size = str(metadata['Size'])
  guid = ''
  if metadata.has_key('GUID'):
    guid = str(metadata['GUID'])
  status = ''
  if metadata.has_key('Status'):
    status = str(metadata['Status'])
  print '%s %s %s %s %s' % (lfn.ljust(100),size.ljust(10),guid.ljust(40),status.ljust(8),checksum.ljust(10))

for lfn in sortList(res['Value']['Failed'].keys()):
  message = res['Value']['Failed'][lfn]
  print lfn,message
