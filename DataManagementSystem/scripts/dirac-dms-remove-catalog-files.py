#!/usr/bin/env python
########################################################################
# $Header:  $
########################################################################
from __future__ import print_function
__RCSID__ = "$Id:  $"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import exit as dexit
from DIRAC import gLogger
Script.setUsageMessage("""
Remove the given file or a list of files from the File Catalog

Usage:
   %s <LFN | fileContainingLFNs>
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
res = getProxyInfo()
if not res['OK']:
  gLogger.fatal("Can't get proxy info", res['Message'])
  dexit(1)
properties = res['Value'].get('groupProperties', [])
if 'FileCatalogManagement' not in properties:
  gLogger.error("You need to use a proxy from a group with FileCatalogManagement")
  dexit(5)

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
fc = FileCatalog()
import os

args = Script.getPositionalArgs()

if len(args) < 1:
  Script.showHelp()
  DIRAC.exit(-1)
else:
  inputFileName = args[0]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName, 'r')
  string = inputFile.read()
  lfns = [lfn.strip() for lfn in string.splitlines()]
  inputFile.close()
else:
  lfns = [inputFileName]

res = fc.removeFile(lfns)
if not res['OK']:
  print("Error:", res['Message'])
  dexit(1)
for lfn in sorted(res['Value']['Failed'].keys()):
  message = res['Value']['Failed'][lfn]
  print('Error: failed to remove %s: %s' % (lfn, message))
print('Successfully removed %d catalog files.' % (len(res['Value']['Successful'])))
