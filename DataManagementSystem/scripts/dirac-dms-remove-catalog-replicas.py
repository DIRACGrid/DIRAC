#!/usr/bin/env python
########################################################################
# $Header: $
########################################################################
__RCSID__ = "$Id$"

from DIRAC           import exit as DIRACExit
from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Remove the given file replica or a list of file replicas from the File Catalog
This script should be used with great care as it may leave dark data in the storage!
Use dirac-dms-remove-replicas instead

Usage:
   %s <LFN | fileContainingLFNs> <SE>
""" % Script.scriptName )

Script.parseCommandLine()

from DIRAC.DataManagementSystem.Client.DataManager import DataManager
dm = DataManager()
import os, sys

if len( sys.argv ) < 3:
  Script.showHelp()
  DIRACExit( -1 )
else:
  inputFileName = sys.argv[1]
  storageElementName = sys.argv[2]

if os.path.exists( inputFileName ):
  inputFile = open( inputFileName, 'r' )
  string = inputFile.read()
  lfns = [ lfn.strip() for lfn in string.splitlines() ]
  inputFile.close()
else:
  lfns = [inputFileName]

res = dm.removeReplicaFromCatalog( storageElementName, lfns )
if not res['OK']:
  print res['Message']
  sys.exit()
for lfn in sorted( res['Value']['Failed'] ):
  message = res['Value']['Failed'][lfn]
  print 'Failed to remove %s replica of %s: %s' % ( storageElementName, lfn, message )
print 'Successfully remove %d catalog replicas at %s' % ( len( res['Value']['Successful'] ), storageElementName )
