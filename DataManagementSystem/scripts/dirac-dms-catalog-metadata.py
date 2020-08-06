#!/usr/bin/env python

from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
__RCSID__ = "$Id$"

from DIRAC import exit as DIRACExit
from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Get metadata for the given file specified by its Logical File Name or for a list of files
contained in the specifed file

Usage:
   %s <lfn | fileContainingLfns> [Catalog]
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

import os
args = Script.getPositionalArgs()

if not len(args) >= 1:
  Script.showHelp(1)
else:
  inputFileName = args[0]
  catalogs = []
  if len(args) == 2:
    catalogs = [args[1]]


if os.path.exists(inputFileName):
  inputFile = open(inputFileName, 'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

res = FileCatalog(catalogs=catalogs).getFileMetadata(lfns)
if not res['OK']:
  print("ERROR:", res['Message'])
  DIRACExit(-1)

print(
    '%s %s %s %s %s' %
    ('FileName'.ljust(100),
     'Size'.ljust(10),
     'GUID'.ljust(40),
     'Status'.ljust(8),
     'Checksum'.ljust(10)))
for lfn in sorted(res['Value']['Successful'].keys()):
  metadata = res['Value']['Successful'][lfn]
  checksum = ''
  if 'Checksum' in metadata:
    checksum = str(metadata['Checksum'])
  size = ''
  if 'Size' in metadata:
    size = str(metadata['Size'])
  guid = ''
  if 'GUID' in metadata:
    guid = str(metadata['GUID'])
  status = ''
  if 'Status' in metadata:
    status = str(metadata['Status'])
  print('%s %s %s %s %s' % (lfn.ljust(100), size.ljust(10), guid.ljust(40), status.ljust(8), checksum.ljust(10)))

for lfn in sorted(res['Value']['Failed'].keys()):
  message = res['Value']['Failed'][lfn]
  print(lfn, message)
