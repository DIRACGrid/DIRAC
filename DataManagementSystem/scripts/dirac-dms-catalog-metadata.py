#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"

from DIRAC           import exit as DIRACExit
from DIRAC.Core.Base import Script 

Script.setUsageMessage("""
Get metadata for the given file specified by its Logical File Name or for a list of files
contained in the specifed file

Usage:
   %s <lfn | fileContainingLfns> [Catalog]
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC.Core.Utilities.List                          import sortList
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

import os, sys

if not len(sys.argv) >= 2:
  Script.showHelp()
  DIRACExit( -1 )
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

res = FileCatalog( catalogs = catalogs ).getFileMetadata( lfns )
if not res['OK']:
  print "ERROR:",res['Message']
  DIRACExit( -1 )

print '%s %s %s %s %s' % ('FileName'.ljust(100),'Size'.ljust(10),'GUID'.ljust(40),'Status'.ljust(8),'Checksum'.ljust(10))
for lfn in sortList(res['Value']['Successful'].keys()):
  metadata = res['Value']['Successful'][lfn]
  checksum = ''
  if metadata.has_key('Checksum'):
    checksum = str(metadata['Checksum'])
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
